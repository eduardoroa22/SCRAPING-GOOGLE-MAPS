from typing import Iterable, List, Optional, Sequence, Set, Tuple
import math
import time
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import argparse
import csv
import socket, ssl
from datetime import date
from dataclasses import dataclass
import re
from scraping import find_emails_on_site

try:
    from dotenv import load_dotenv  # pip install python-dotenv (optional)
    load_dotenv()
except Exception:
    pass

@dataclass
class RunResult:
    state_code: str
    state_name: str
    sheet_tab: str
    added_count: int
    api_requests: int

@dataclass
class BBox:
    lat_min: float
    lat_max: float
    lng_min: float
    lng_max: float
    state_code: str
    state_name: str

SHEET_TAB_EXPLICIT = os.getenv("SHEET_STATE")
SHEET_TAB_TEMPLATE = os.getenv("SHEET_TAB_TEMPLATE")

CENTERS_CSV_DEFAULT = os.getenv("CENTERS_CSV")

ENRICH_EMAILS = os.getenv("ENRICH_EMAILS", "0") == "1"

# Core credentials / IDs
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLEAPI_KEY")
GOOGLE_SHEETS_ID = os.getenv("GOOGLESHEETS")  # Sheet ID (not an API key)
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLESERVICE")  # path to JSON

# Operational knobs (with sensible defaults)
TARGET_STATE_DEFAULT = os.getenv("TARGET_STATE", "CA")
STATE_BBOX_FILE_DEFAULT = os.getenv("STATE_BBOX_FILE")  # optional CSV path
GRID_SPACING_KM_DEFAULT = float(os.getenv("GRID_SPACING_KM", "30"))
RADIUS_M_DEFAULT = int(os.getenv("RADIUS_M", "25000"))
CSV_OUTPUT_DEFAULT = os.getenv("CSV_OUTPUT")  # optional
PACE_S_DEFAULT = float(os.getenv("PACE_SECONDS", "1.0"))
FLUSH_EVERY_DEFAULT = int(os.getenv("FLUSH_EVERY", "10"))            # guardar cada 10
CHUNK_APPEND_ROWS_DEFAULT = int(os.getenv("CHUNK_APPEND_ROWS", "50"))  # tamaño de cada lote a Sheets
SHEETS_APPEND_PACE_S_DEFAULT = float(os.getenv("SHEETS_PACE_SECONDS", "0.2"))  # pausa entre lotes

# Optimizaciones
KEYWORD_STRATEGY_DEFAULT = os.getenv("KEYWORD_STRATEGY", "all")  # all, combined, first
MAX_KEYWORDS_PER_CENTER_DEFAULT = int(os.getenv("MAX_KEYWORDS_PER_CENTER", "0")) or None
STOP_AFTER_NEW_DEFAULT = int(os.getenv("STOP_AFTER_NEW", "0")) or None
SKIP_OVERLAP_CENTERS_DEFAULT = os.getenv("SKIP_OVERLAP_CENTERS", "0") == "1"
OVERLAP_FACTOR_DEFAULT = float(os.getenv("OVERLAP_FACTOR", "0.6"))

# Keywords: comma-separated in env -> list
_ENV_KW = os.getenv("KEYWORDS")
if _ENV_KW:
    ENV_KEYWORDS = [s.strip() for s in _ENV_KW.split(",") if s.strip()]
else:
    ENV_KEYWORDS = None

# ---------------------- Places API endpoints ----------------------

# ---------------------- Defaults / Config -------------------------
DEFAULT_KEYWORDS = [
    "recording studio",
    "music studio",
    "mixing studio",
    "mastering studio",
    "rehearsal studio",
    "music production"
]

# California bounding box (approx) — used if you don't provide a CSV with bboxes
# South->North: 32.5 -> 42.0 ; West->East: -124.5 -> -114.1
DEFAULT_STATE_BBOX = {
    "CA": {
        "state_name": "California",
        "lat_min": 32.5,
        "lat_max": 42.0,
        "lng_min": -124.5,
        "lng_max": -114.1,
    }
}

# Google Maps URL fallback pattern when 'url' isn't present in details
MAPS_PLACE_URL = "https://www.google.com/maps/place/?q=place_id:{place_id}"

# Sheets header (DON'T change order unless you also update read_existing_place_ids col)
HEADERS = [
    "Business Name",
    "Address",
    "City",
    "Zip",
    "Website",
    "Phone",
    "Email",
    "maps_url",
    "lat",
    "lng",
    "place_id",
    "keyword",
    "center_lat",
    "center_lng",
]
def _col_letter_from_index_one_based(idx: int) -> str:
    letters = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters

PLACE_ID_COL_INDEX = HEADERS.index("place_id") + 1
PLACE_ID_COL_LETTER = _col_letter_from_index_one_based(PLACE_ID_COL_INDEX)

NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
# Place Details fields (v2 JSON API)
DETAIL_FIELDS = "name,formatted_address,website,url,geometry,formatted_phone_number,international_phone_number,address_components"
INCLUDE_HINTS = ["record", "mix", "master", "audio", "music", "studio", "recording"]
EXCLUDE_IF_NAME_CONTAINS = [
    "tattoo", "yoga", "pilates", "dance", "photo", "photography",
    "fitness", "crossfit", "martial", "hair", "salon", "nail",
]

def parse_address_components(address_components: List[dict]) -> tuple:
    """Extract city and zip code from address components."""
    city = ""
    zip_code = ""
    
    for component in address_components:
        types = component.get("types", [])
        if "locality" in types:
            city = component.get("long_name", "")
        elif "postal_code" in types:
            zip_code = component.get("long_name", "")
        elif not city and "administrative_area_level_2" in types:
            # Fallback para condados si no hay city
            city = component.get("long_name", "")
    
    return city, zip_code

def km_to_deg_lat(km: float) -> float:
    # ~111 km per degree of latitude
    return km / 111.0


def extract_email_from_website(website: str) -> str:
    """Intenta extraer email del website (básico)."""
    # Esta es una implementación básica. Para algo más robusto necesitarías hacer scraping del website
    # Por ahora retornamos vacío ya que el Places API no proporciona emails directamente
    return ""

def km_to_deg_lng(km: float, lat_deg: float) -> float:
    # longitude degrees vary with latitude
    return km / (111.320 * math.cos(math.radians(lat_deg)))


def generate_grid(lat_min: float, lat_max: float, lng_min: float, lng_max: float, spacing_km: float) -> Iterable[Tuple[float, float]]:
    lat = lat_min
    while lat <= lat_max + 1e-9:
        lng_step = km_to_deg_lng(spacing_km, lat)
        lng = lng_min
        while lng <= lng_max + 1e-9:
            yield round(lat, 6), round(lng, 6)
            lng += lng_step
        lat += km_to_deg_lat(spacing_km)

# ---------------------- Helpers: Places API -----------------------

def backoff_sleep(attempt: int, base: float = 1.7) -> None:
    # attempt starts at 1
    wait = (base ** (attempt - 1)) + (0.25 * attempt)
    time.sleep(wait)


def nearby_search(api_key: str, lat: float, lng: float, keyword: str, radius_m: int, pagetoken: Optional[str] = None, language: str = "en", region: str = "us") -> dict:
    params = {
        "key": api_key,
        "language": language,
        "region": region,
    }
    if pagetoken:
        params["pagetoken"] = pagetoken
    else:
        params.update({
            "location": f"{lat},{lng}",
            "radius": radius_m,
            "keyword": keyword,
        })
    for attempt in range(1, 7):  # up to 6 backoff attempts
        r = requests.get(NEARBY_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        status = data.get("status", "")
        if status in ("OK", "ZERO_RESULTS"):
            return data
        if status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED", "UNKNOWN_ERROR"):
            backoff_sleep(attempt)
            continue
        # Other statuses (INVALID_REQUEST, etc.) — return as-is
        return data
    return {"status": "FAILED", "results": []}


def fetch_details(api_key: str, place_id: str) -> dict:
    params = {"place_id": place_id, "fields": DETAIL_FIELDS, "key": api_key}
    for attempt in range(1, 7):
        r = requests.get(DETAILS_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        status = data.get("status", "")
        if status in ("OK", "ZERO_RESULTS", "NOT_FOUND"):
            return data
        if status in ("OVER_QUERY_LIMIT", "RESOURCE_EXHAUSTED", "UNKNOWN_ERROR"):
            backoff_sleep(attempt)
            continue
        return data
    return {"status": "FAILED", "result": {}}

# ---------------------- Helpers: filters --------------------------

def likely_music_studio(name_lower: str) -> bool:
    if not name_lower:
        return False
    # Keep if it contains 'studio' + music hint OR contains 'recording'
    if "studio" in name_lower and any(h in name_lower for h in INCLUDE_HINTS):
        return True
    if "recording" in name_lower:
        return True
    # Also keep if explicitly music/audio terms present without 'studio'
    if any(h in name_lower for h in ["music", "audio", "mix", "master"]):
        return True
    return False


def should_exclude(name_lower: str) -> bool:
    return any(bad in name_lower for bad in EXCLUDE_IF_NAME_CONTAINS)

# ---------------------- Optimización keywords -----------------------

def tokenize_keywords(keywords: Sequence[str]) -> Set[str]:
    """Extrae tokens únicos de un conjunto de keywords."""
    tokens = set()
    for kw in keywords:
        for token in re.findall(r'\b\w+\b', kw.lower()):
            if len(token) > 2:  # Ignorar tokens muy cortos
                tokens.add(token)
    return tokens

def combine_keywords(keywords: Sequence[str], max_tokens: int = 8) -> str:
    """Combina múltiples keywords en una sola consulta."""
    tokens = tokenize_keywords(keywords)
    # Priorizar tokens clave de música
    priority_tokens = [t for t in tokens if t in ["recording", "studio", "music", "audio", "mix", "master"]]
    other_tokens = [t for t in tokens if t not in priority_tokens]
    
    # Usar primero tokens prioritarios, luego otros hasta el límite
    selected = priority_tokens + other_tokens
    if len(selected) > max_tokens:
        selected = selected[:max_tokens]
    
    return " ".join(selected)

def calculate_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calcula distancia aproximada en metros entre dos puntos."""
    # Fórmula haversine simplificada para distancias cortas
    dx = 111320 * math.cos(math.radians((lat1 + lat2) / 2)) * (lng2 - lng1)
    dy = 111320 * (lat2 - lat1)
    return math.sqrt(dx*dx + dy*dy)

# ---------------------- Google Sheets helpers ---------------------

def build_sheets_service(service_account_json: str):
    creds = Credentials.from_service_account_file(
        service_account_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def ensure_tab_and_headers(svc, spreadsheet_id: str, tab_title: str) -> None:
    """Create tab if missing and ensure header row is present."""
    try:
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = meta.get("sheets", [])
        titles = {s["properties"]["title"] for s in sheets}
        requests_body = {"requests": []}
        if tab_title not in titles:
            requests_body["requests"].append({
                "addSheet": {
                    "properties": {
                        "title": tab_title,
                        "gridProperties": {"rowCount": 5000, "columnCount": 20}
                    }
                }
            })
        if requests_body["requests"]:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body=requests_body
            ).execute()
        # Write headers if A1 is empty
        rng = f"{tab_title}!A1:{_col_letter_from_index_one_based(len(HEADERS))}1"
        res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
        values = res.get("values")
        if not values:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=rng,
                valueInputOption="RAW",
                body={"values": [HEADERS]},
            ).execute()
    except HttpError as e:
        print(f"[Sheets] ERROR ensuring tab/headers: {e}")
        raise


def read_existing_place_ids(svc, spreadsheet_id: str, tab_title: str) -> set:
    rng = f"{tab_title}!{PLACE_ID_COL_LETTER}2:{PLACE_ID_COL_LETTER}"
    try:
        res = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
        values = res.get("values", [])
        existing = {row[0] for row in values if row and row[0]}
        return existing
    except HttpError as e:
        print(f"[Sheets] ERROR reading existing place_ids: {e}")
        return set()


def append_rows_to_sheet(svc, spreadsheet_id: str, tab_title: str, rows: List[List]):
    if not rows:
        return
    rng = f"{tab_title}!A2"
    try:
        svc.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
    except HttpError as e:
        print(f"[Sheets] ERROR appending rows: {e}")
        raise


def append_rows_with_retry(
    svc,
    spreadsheet_id: str,
    tab_title: str,
    rows: List[List],
    max_attempts: int = 10,
    chunk_size: int = CHUNK_APPEND_ROWS_DEFAULT,
    base_backoff: float = 5.0,
    sheets_pace_s: float = SHEETS_APPEND_PACE_S_DEFAULT,
    ):
    """Append con reintentos + chunking para robustecer contra fallos de red."""
    if not rows:
        return

    i = 0
    while i < len(rows):
        sub = rows[i : i + max(1, chunk_size)]
        attempt = 1
        while True:
            try:
                append_rows_to_sheet(svc, spreadsheet_id, tab_title, sub)
                break
            except Exception as e:
                # Detectar errores pasajeros (red/SSL/5xx)
                transient = isinstance(e, (ConnectionError, ConnectionResetError, ConnectionAbortedError, socket.timeout, ssl.SSLError))
                if isinstance(e, HttpError):
                    status = getattr(e, "status_code", None) or getattr(getattr(e, "resp", None), "status", None)
                    try:
                        status = int(status) if status is not None else None
                    except Exception:
                        status = None
                    if status is not None and 500 <= status < 600:
                        transient = True

                if not transient or attempt >= max_attempts:
                    print(f"[Sheets] Append failed permanently after {attempt} attempts; raising. Error: {e}")
                    raise

                wait = (base_backoff ** (attempt - 1)) + (0.2 * attempt)
                print(f"[Sheets] Transient error appending rows (attempt {attempt}). Retrying in {wait:.1f}s…")
                time.sleep(wait)
                if attempt >= 2 and chunk_size > 1:
                    chunk_size = max(1, chunk_size // 2)  # reducir el tamaño del lote si insiste el error
                attempt += 1

        i += len(sub)
        if sheets_pace_s > 0:
            time.sleep(sheets_pace_s)

# ---------------------- BBox loading -------------------------------

def load_state_bbox(state_code: str, state_bbox_file: Optional[str]) -> BBox:
    state_code = state_code.upper()
    # 1) Try CSV file if provided
    if state_bbox_file:
        with open(state_bbox_file, "r", newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if row.get("state_code", "").upper() == state_code:
                    return BBox(
                        lat_min=float(row["lat_min"]),
                        lat_max=float(row["lat_max"]),
                        lng_min=float(row["lng_min"]),
                        lng_max=float(row["lng_max"]),
                        state_code=state_code,
                        state_name=row.get("state_name") or state_code,
                    )
    # 2) Built-in fallback for CA
    if state_code in DEFAULT_STATE_BBOX:
        d = DEFAULT_STATE_BBOX[state_code]
        return BBox(
            lat_min=d["lat_min"], lat_max=d["lat_max"],
            lng_min=d["lng_min"], lng_max=d["lng_max"],
            state_code=state_code, state_name=d["state_name"],
        )
    raise ValueError(
        f"Bounding box for state '{state_code}' not found. Provide --state-bbox-file with the state's lat/lng bounds."
    )

# ---------------------- Core collection logic ----------------------

def collect_for_state(
    api_key: str,
    sheet_id: str,
    service_account_json: str,
    bbox: BBox,
    keywords: Sequence[str] = DEFAULT_KEYWORDS,
    grid_spacing_km: float = 30.0,
    radius_m: int = 25000,
    csv_output: Optional[str] = None,
    pace_s: float = 1.0,
    tab_title: Optional[str] = None,
    tab_title_template: Optional[str] = None,
    centers_override: Optional[Sequence[Tuple[float, float]]] = None,
    # Nuevos parámetros de optimización
    keyword_strategy: str = "all",
    max_keywords_per_center: Optional[int] = None,
    stop_after_new: Optional[int] = None,
    skip_overlap_centers: bool = False,
    overlap_factor: float = 0.6,
) -> None:
    """Sweep a state bbox and write new rows to its tab in the Sheet."""
    # Build Sheets client and ensure tab+headers
    svc = build_sheets_service(service_account_json)
    tab_title_resolved = resolve_tab_title(bbox, tab_title, tab_title_template)
    ensure_tab_and_headers(svc, sheet_id, tab_title_resolved)

    # Load existing place_ids to make re-runs idempotent
    existing_place_ids = read_existing_place_ids(svc, sheet_id, tab_title_resolved)
    seen: set = set(existing_place_ids)

    # Optional local CSV backup
    csv_writer = None
    csv_file = None
    if csv_output:
        csv_file = open(csv_output, "w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(HEADERS)

    total_requests = 0
    buffered_rows: List[List] = []
    flush_every = FLUSH_EVERY_DEFAULT
    
    # Seguimiento para la optimización de solapamiento
    processed_centers = []  # [(lat, lng, nuevos_encontrados), ...]

    try:
        # elegir la fuente de centros
        if centers_override and len(centers_override) > 0:
            centers_iter = centers_override
            print(f"[Centers] Usando CSV con {len(centers_override)} puntos")
        else:
            centers_iter = generate_grid(bbox.lat_min, bbox.lat_max, bbox.lng_min, bbox.lng_max, grid_spacing_km)
            print("[Centers] Usando grid generado")

        # Preparar keywords según la estrategia elegida
        effective_keywords = list(keywords)  # copia para no modificar original
        
        if keyword_strategy == "first" and effective_keywords:
            effective_keywords = [effective_keywords[0]]
            print(f"[Optimize] Estrategia 'first': usando sólo la primera keyword: '{effective_keywords[0]}'")
        elif keyword_strategy == "combined" and effective_keywords:
            combined = combine_keywords(effective_keywords)
            effective_keywords = [combined]
            print(f"[Optimize] Estrategia 'combined': combinando {len(keywords)} keywords en una sola: '{combined}'")
        
        # Limitar keywords por centro si se especificó
        if max_keywords_per_center is not None and max_keywords_per_center > 0:
            if max_keywords_per_center < len(effective_keywords):
                effective_keywords = effective_keywords[:max_keywords_per_center]
                print(f"[Optimize] Limitando a {max_keywords_per_center} keywords por centro")

        for center_lat, center_lng in centers_iter:
            # Verificar si debemos saltar este centro por solapamiento
            if skip_overlap_centers and processed_centers:
                skip = False
                overlap_dist = overlap_factor * radius_m
                
                for pc_lat, pc_lng, found_count in processed_centers:
                    if found_count >= (stop_after_new or 1):  # Sólo consideramos centros que produjeron resultados
                        dist = calculate_distance(center_lat, center_lng, pc_lat, pc_lng)
                        if dist < overlap_dist:
                            skip = True
                            print(f"[Optimize] Saltando centro ({center_lat}, {center_lng}) por solapamiento con centro previo a {dist:.0f}m")
                            break
                
                if skip:
                    continue
            
            # Contador de nuevos lugares encontrados en este centro
            new_places_this_center = 0
            
            # Para cada keyword, buscar lugares cercanos
            for keyword_idx, keyword in enumerate(effective_keywords):
                # Si ya alcanzamos el límite de nuevos lugares para este centro, pasar al siguiente
                if stop_after_new is not None and new_places_this_center >= stop_after_new:
                    print(f"[Optimize] Alcanzado límite de {stop_after_new} nuevos lugares en este centro. Pasando al siguiente.")
                    break
                
                print(f"Searching '{keyword}' around ({center_lat}, {center_lng})")
                data = nearby_search(api_key, center_lat, center_lng, keyword, radius_m)
                total_requests += 1
                time.sleep(pace_s)

                page_count = 1
                while True:
                    results = data.get("results", [])
                    # Si es página 2+ y no encontramos nada nuevo en la primera página, cortar
                    if page_count > 1 and new_places_this_center == 0:
                        print(f"[Optimize] La primera página no produjo resultados nuevos. Omitiendo páginas adicionales.")
                        break
                    
                    # Procesar resultados
                    for res in results:
                        pid = res.get("place_id")
                        if not pid or pid in seen:
                            continue
                            
                        name = (res.get("name") or "").strip()
                        name_lower = name.lower()
                        if should_exclude(name_lower) and not any(h in name_lower for h in ["record", "mix", "master", "audio", "music", "recording"]):
                            continue
                        if not likely_music_studio(name_lower):
                            continue

                        det = fetch_details(api_key, pid)
                        time.sleep(pace_s)
                        det_res = det.get("result", {}) if isinstance(det, dict) else {}

                        address = det_res.get("formatted_address") or res.get("vicinity", "")
                        website = det_res.get("website", "")
                        maps_url = det_res.get("url") or MAPS_PLACE_URL.format(place_id=pid)
                        loc = (det_res.get("geometry", {}) or res.get("geometry", {})).get("location", {})
                        lat = loc.get("lat", "")
                        lng = loc.get("lng", "")

                        business_name = name
                        phone = det_res.get("formatted_phone_number") or det_res.get("international_phone_number", "")

                        address_components = det_res.get("address_components", [])
                        city, zip_code = parse_address_components(address_components)

                        emails = []
                        if ENRICH_EMAILS and website:
                            emails = find_emails_on_site(website)
                        email = ", ".join(emails) if emails else ""

                        row = [
                            business_name,
                            address,
                            city,
                            zip_code,
                            website,
                            phone,
                            email,
                            maps_url,
                            lat,
                            lng,
                            pid,
                            keyword,
                            center_lat,  # ← ahora sí toma el centro del CSV
                            center_lng,  # ← ahora sí toma el centro del CSV
                        ]
                        buffered_rows.append(row)
                        seen.add(pid)
                        new_places_this_center += 1
                        
                        if csv_writer:
                            csv_writer.writerow(row)

                        if len(buffered_rows) >= flush_every:
                            print(f"Flushing {len(buffered_rows)} rows to Google Sheets...")
                            append_rows_with_retry(svc, sheet_id, tab_title_resolved, buffered_rows)
                            buffered_rows.clear()
                        
                        # Verificar si alcanzamos el límite de nuevos lugares por centro
                        if stop_after_new is not None and new_places_this_center >= stop_after_new:
                            print(f"[Optimize] Alcanzado límite de {stop_after_new} nuevos lugares en este centro durante procesamiento.")
                            break
                    
                    # Si alcanzamos el límite o no hay token para siguiente página, salir
                    if (stop_after_new is not None and new_places_this_center >= stop_after_new) or not data.get("next_page_token"):
                        break
                    
                    # Procesar siguiente página
                    time.sleep(2.0)  # Necesario para que el token sea válido
                    data = nearby_search(api_key, center_lat, center_lng, keyword, radius_m, pagetoken=data.get("next_page_token"))
                    page_count += 1
                    print(f" → Página {page_count}: {len(data.get('results', []))} resultados")
                    total_requests += 1
                    time.sleep(pace_s)
            
            # Registrar este centro como procesado para optimización de solapamiento
            processed_centers.append((center_lat, center_lng, new_places_this_center))
            print(f"[Centro {center_lat},{center_lng}] Nuevos lugares encontrados: {new_places_this_center}")
    finally:
        if buffered_rows:
            print(f"Final flush of {len(buffered_rows)} rows to Google Sheets...")
            append_rows_with_retry(svc, sheet_id, tab_title_resolved, buffered_rows)
        if csv_file:
            csv_file.close()

    added = len(seen) - len(existing_place_ids)
    print(f"Done. State: {bbox.state_name} | Unique places added this run: {added} | API requests: {total_requests}")

    return RunResult(
        state_code=bbox.state_code,
        state_name=bbox.state_name,
        sheet_tab=tab_title_resolved,
        added_count=added,
        api_requests=total_requests,
    )

# ---------------------- Main / CLI ---------------------------------
def load_centers_csv(path: str) -> List[Tuple[float,float]]:
    centers: List[Tuple[float,float]] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        sample = f.read(1024)
        f.seek(0)
        has_header = ('lat' in sample.lower() and ('lng' in sample.lower() or 'lon' in sample.lower()))
        if has_header:
            rdr = csv.DictReader(f)
            for row in rdr:
                lat = row.get("lat") or row.get("latitude")
                lng = row.get("lng") or row.get("lon") or row.get("longitude")
                if lat is None or lng is None:
                    continue
                try:
                    centers.append((round(float(lat), 6), round(float(lng), 6)))
                except:
                    pass
        else:
            rdr = csv.reader(f)
            for row in rdr:
                if not row or len(row) < 2:
                    continue
                try:
                    centers.append((round(float(row[0]), 6), round(float(row[1]), 6)))
                except:
                    continue
    return centers

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect music/recording studios into Google Sheets by sweeping a state with a Places grid.")
    # Read defaults from env (set above). Users can still override with CLI flags if they want.
    p.add_argument("--api-key", default=GOOGLE_PLACES_API_KEY, help="Google Places API key")
    p.add_argument("--sheet-id", default=GOOGLE_SHEETS_ID, help="Target Google Sheet ID")
    p.add_argument("--service-account", default=GOOGLE_SERVICE_ACCOUNT_JSON, help="Path to Service Account JSON for Google Sheets API")
    p.add_argument("--state", default=TARGET_STATE_DEFAULT, help="State code (e.g., CA)")
    p.add_argument("--state-bbox-file", default=STATE_BBOX_FILE_DEFAULT, help="CSV with columns: state_code,state_name,lat_min,lat_max,lng_min,lng_max")
    p.add_argument("--grid-spacing-km", type=float, default=GRID_SPACING_KM_DEFAULT, help="Grid spacing in kilometers")
    p.add_argument("--radius-m", type=int, default=RADIUS_M_DEFAULT, help="Places Nearby radius (max 50000)")
    p.add_argument("--keywords", nargs="*", default=ENV_KEYWORDS or DEFAULT_KEYWORDS, help="Keywords to search (space-separated); or set KEYWORDS env as comma-separated")
    p.add_argument("--csv-output", default=CSV_OUTPUT_DEFAULT, help="Optional local CSV backup path")
    p.add_argument("--pace-s", type=float, default=PACE_S_DEFAULT, help="Delay seconds between API calls (be courteous)")

    p.add_argument("--sheet-tab", default=SHEET_TAB_EXPLICIT, help="Exact tab title override")
    p.add_argument("--sheet-tab-template", default=SHEET_TAB_TEMPLATE, help="Tab template using {state_code} {state_name} {yyyymmdd}")

    p.add_argument("--centers-csv", default=CENTERS_CSV_DEFAULT, help="CSV con columnas lat,lng para usar como centros (override del grid)")
    
    # Nuevos parámetros de optimización
    p.add_argument("--keyword-strategy", choices=["all", "combined", "first"], default=KEYWORD_STRATEGY_DEFAULT,
                  help="Estrategia de keywords: 'all'=una búsqueda por keyword, 'combined'=tokens combinados, 'first'=sólo primera keyword")
    p.add_argument("--max-keywords-per-center", type=int, default=MAX_KEYWORDS_PER_CENTER_DEFAULT,
                  help="Limitar cuántas keywords usar por centro (0=todas)")
    p.add_argument("--stop-after-new", type=int, default=STOP_AFTER_NEW_DEFAULT,
                  help="Dejar de buscar en un centro después de encontrar N nuevos lugares (0=sin límite)")
    p.add_argument("--skip-overlap-centers", action="store_true", default=SKIP_OVERLAP_CENTERS_DEFAULT,
                  help="Saltar centros que estén muy cerca de otros ya procesados con éxito")
    p.add_argument("--overlap-factor", type=float, default=OVERLAP_FACTOR_DEFAULT,
                  help="Factor para determinar cuándo dos centros se solapan (0.6 = 60% del radio)")

    return p.parse_args(argv)

def resolve_tab_title(bbox: BBox, explicit_tab: Optional[str], template: Optional[str]) -> str:
    if explicit_tab:
        return explicit_tab
    if template:
        today = date.today()
        return template.format(
            state_code=bbox.state_code,
            state_name=bbox.state_name,
            yyyymmdd=today.strftime("%Y%m%d"),
        )
    return bbox.state_name