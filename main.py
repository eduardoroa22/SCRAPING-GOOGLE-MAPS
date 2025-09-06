from __future__ import annotations
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from functions import *

def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    # Validate required settings when running with just the VS Code Play button
    missing = []
    if not args.api_key:
        missing.append("GOOGLE_PLACES_API_KEY or --api-key")
    if not args.sheet_id:
        missing.append("GOOGLE_SHEETS_ID or --sheet-id")
    if not args.service_account:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON or --service-account")
    if missing:
        print("\n[Config] Faltan variables requeridas:\n - " + "\n - ".join(missing))
        print("\nConfigúralas en tu entorno, en un .env, o pásalas por CLI. Ejemplos de .env al final de este archivo en las instrucciones.")
        sys.exit(1)

    # Log de parámetros clave (sin exponer secretos)
    def _mask(s: Optional[str]) -> str:
        if not s:
            return ""
        return (s[:6] + "…") if len(s) > 8 else "***"
    print(f"[Config] State={args.state} | Grid={args.grid_spacing_km}km | Radius={args.radius_m}m | Pace={args.pace_s}s | Keywords={args.keywords}")
    print(f"[Config] SheetID={_mask(args.sheet_id)} | ServiceJSON={args.service_account}")
    
    # Mostrar optimizaciones
    print(f"[Optimize] Strategy={args.keyword_strategy} | MaxKW={args.max_keywords_per_center or 'all'} | StopAfter={args.stop_after_new or 'unlimited'}")
    if args.skip_overlap_centers:
        print(f"[Optimize] SkipOverlap=True | OverlapFactor={args.overlap_factor}")
    
    bbox = load_state_bbox(args.state, args.state_bbox_file)

    centers = None
    if args.centers_csv:
        centers = load_centers_csv(args.centers_csv)
        print(f"[Config] Usando {len(centers)} centros desde CSV (override del grid)")
        
    collect_for_state(
        api_key=args.api_key,
        sheet_id=args.sheet_id,
        service_account_json=args.service_account,
        bbox=bbox,
        keywords=args.keywords,
        grid_spacing_km=args.grid_spacing_km,
        radius_m=args.radius_m,
        csv_output=args.csv_output,
        pace_s=args.pace_s,
        tab_title=args.sheet_tab,                   
        tab_title_template=args.sheet_tab_template, 
        centers_override=centers,
        # Parámetros de optimización
        keyword_strategy=args.keyword_strategy,
        max_keywords_per_center=args.max_keywords_per_center,
        stop_after_new=args.stop_after_new,
        skip_overlap_centers=args.skip_overlap_centers,
        overlap_factor=args.overlap_factor,
    )


if __name__ == "__main__":
    main()