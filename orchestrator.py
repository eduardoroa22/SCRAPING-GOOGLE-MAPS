# orchestrator.py
from __future__ import annotations
import os
import sys
import glob
import traceback
from typing import Optional, Sequence, Tuple, List
from dotenv import load_dotenv
from functions import ApiHardStop, notify_failure_halt
from functions import (
    load_centers_csv,
    load_state_bbox,
    BBox,
    collect_for_state,
    resolve_tab_title,
    GOOGLE_PLACES_API_KEY as ENV_API_KEY,
    GOOGLE_SHEETS_ID as ENV_SHEET_ID,
    GOOGLE_SERVICE_ACCOUNT_JSON as ENV_SERVICE_JSON,
    STATE_BBOX_FILE_DEFAULT,
    GRID_SPACING_KM_DEFAULT,
    RADIUS_M_DEFAULT,
    PACE_S_DEFAULT,
    SHEET_TAB_EXPLICIT,
    SHEET_TAB_TEMPLATE,
    KEYWORD_STRATEGY_DEFAULT,
    MAX_KEYWORDS_PER_CENTER_DEFAULT,
    STOP_AFTER_NEW_DEFAULT,
    SKIP_OVERLAP_CENTERS_DEFAULT,
    OVERLAP_FACTOR_DEFAULT,
)

from notifier_ses import notify_success, notify_failure, notify_summary

load_dotenv()

def _bbox_from_centers(state_code: str, centers: List[Tuple[float, float]]) -> BBox:
    lat_min = min(c[0] for c in centers)
    lat_max = max(c[0] for c in centers)
    lng_min = min(c[1] for c in centers)
    lng_max = max(c[1] for c in centers)
    return BBox(lat_min=lat_min, lat_max=lat_max, lng_min=lng_min, lng_max=lng_max,
                state_code=state_code, state_name=state_code)

def main(argv: Optional[Sequence[str]] = None) -> None:
    api_key = os.getenv("GOOGLEAPI_KEY", ENV_API_KEY)
    sheet_id = os.getenv("GOOGLESHEETS", ENV_SHEET_ID)
    service_json = os.getenv("GOOGLESERVICE", ENV_SERVICE_JSON)
    states_dir = os.getenv("STATES_DIR", "states")
    state_bbox_file = os.getenv("STATE_BBOX_FILE", STATE_BBOX_FILE_DEFAULT)

    if not api_key or not sheet_id or not service_json:
        print("[Config] Faltan GOOGLEAPI_KEY / GOOGLESHEETS / GOOGLESERVICE")
        sys.exit(1)

    # Opciones operativas (heredadas de tu .env)
    grid_spacing_km = float(os.getenv("GRID_SPACING_KM", GRID_SPACING_KM_DEFAULT))
    radius_m = int(os.getenv("RADIUS_M", RADIUS_M_DEFAULT))
    pace_s = float(os.getenv("PACE_SECONDS", PACE_S_DEFAULT))
    tab_title = os.getenv("SHEET_STATE", SHEET_TAB_EXPLICIT)
    tab_template = os.getenv("SHEET_TAB_TEMPLATE", SHEET_TAB_TEMPLATE)

    keyword_strategy = os.getenv("KEYWORD_STRATEGY", KEYWORD_STRATEGY_DEFAULT)
    max_kw_per_center = int(os.getenv("MAX_KEYWORDS_PER_CENTER", str(MAX_KEYWORDS_PER_CENTER_DEFAULT or 0)) or "0") or None
    stop_after_new = int(os.getenv("STOP_AFTER_NEW", str(STOP_AFTER_NEW_DEFAULT or 0)) or "0") or None
    skip_overlap_centers = os.getenv("SKIP_OVERLAP_CENTERS", "0") == "1" or SKIP_OVERLAP_CENTERS_DEFAULT
    overlap_factor = float(os.getenv("OVERLAP_FACTOR", OVERLAP_FACTOR_DEFAULT))

    # Descubrir CSVs de estados (centros por estado)
    paths = sorted(glob.glob(os.path.join(states_dir, "*.csv")))
    if not paths:
        print(f"[Runner] No hay CSVs en {states_dir}")
        sys.exit(1)

    print(f"[Runner] Encontrados {len(paths)} CSVs en {states_dir}")

    completed: list[tuple[str, str, int, int]] = []  # (code, name, added, reqs)

    for csv_path in paths:
        filename = os.path.basename(csv_path)
        state_code = os.path.splitext(filename)[0].upper()  # p.ej. CA.csv -> CA
        print(f"[Runner] CSV: {csv_path}")

        try:
            centers = load_centers_csv(csv_path)
            if not centers:
                raise RuntimeError(f"CSV sin centros válidos: {csv_path}")

            # BBox: preferir archivo maestro; si no, inferir del CSV
            if state_bbox_file:
                try:
                    bbox = load_state_bbox(state_code, state_bbox_file)
                except Exception:
                    bbox = _bbox_from_centers(state_code, centers)
            else:
                bbox = _bbox_from_centers(state_code, centers)

            # Ejecutar recolección (forzamos centers_override = CSV de ese estado)
            result = collect_for_state(
                api_key=api_key,
                sheet_id=sheet_id,
                service_account_json=service_json,
                bbox=bbox,
                grid_spacing_km=grid_spacing_km,   # sin efecto si hay centers_override
                radius_m=radius_m,
                csv_output=None,                   # si quieres, pon un CSV por estado
                pace_s=pace_s,
                tab_title=tab_title,
                tab_title_template=tab_template,
                centers_override=centers,
                keyword_strategy=keyword_strategy,
                max_keywords_per_center=max_kw_per_center,
                stop_after_new=stop_after_new,
                skip_overlap_centers=skip_overlap_centers,
                overlap_factor=overlap_factor,
            )

            # result es RunResult (agregado en functions.py)
            notify_success(
                state_code=bbox.state_code,
                state_name=bbox.state_name,
                added=result.added_count,
                api_requests=result.api_requests,
                sheet_tab=result.sheet_tab,
                sheet_id=sheet_id,
            )
            completed.append((bbox.state_code, bbox.state_name, result.added_count, result.api_requests))

        except ApiHardStop as e:
            # Email con contexto preciso y corte del flujo
            notify_failure_halt(
                state_code=e.state_code,
                state_name=e.state_name,
                sheet_tab=e.tab_title,
                csv_path=csv_path,
                center_lat=e.center_lat,
                center_lng=e.center_lng,
                keyword=e.keyword,
                status=e.status,
                error_message=e.error_message,
            )
            print("[Runner] HALT por REQUEST_DENIED (Maps). Corrige la clave/billing y relanza.")
            sys.exit(2)

        except Exception as e:
            # Otros errores
            notify_failure(state_code=state_code, state_name=state_code, err=e)
            print("[Runner] ERROR: se detiene la ejecución del lote.")
            print("".join(traceback.format_exception(type(e), e, e.__traceback__)))
            sys.exit(2)

    # Si llegamos aquí, todos los estados finalizaron bien
    if completed:
        notify_summary(completed)

if __name__ == "__main__":
    main()
