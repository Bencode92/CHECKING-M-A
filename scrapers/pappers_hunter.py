#!/usr/bin/env python3
"""
Pappers Hunter — Extraction ciblée via l'API Pappers.
Canal 1 : Succession (dirigeant 55+, entreprise ancienne, saine)
Canal 2 : Distressed (procédure collective en cours)
Génère pappers_results.json pour le dashboard.
"""

import requests
import json
import os
import sys
import time
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
PAPPERS_API = "https://api.pappers.fr/v2"


def search_pappers(api_token, params, max_results=500):
    """Appel paginé à l'API Pappers /recherche."""
    all_results = []
    page = 1
    par_page = min(params.get("par_page", 20), 100)
    
    while len(all_results) < max_results:
        query_params = {
            "api_token": api_token,
            "page": page,
            "par_page": par_page,
            **{k: v for k, v in params.items() if v is not None and k != "par_page"},
        }
        
        print(f"  📡 Pappers API page {page}...")
        try:
            resp = requests.get(f"{PAPPERS_API}/recherche", params=query_params, timeout=30)
            if resp.status_code == 401:
                print("❌ Token API invalide.")
                sys.exit(1)
            elif resp.status_code == 429:
                print("⚠ Rate limit. Pause 5s...")
                time.sleep(5)
                continue
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  ⚠ Erreur API: {e}")
            break
        
        results = data.get("resultats", data.get("results", []))
        total = data.get("total", 0)
        if not results:
            break
        all_results.extend(results)
        print(f"  → {len(results)} résultats (total API: {total})")
        if len(all_results) >= total or len(results) < par_page:
            break
        page += 1
        time.sleep(0.5)
    
    return all_results[:max_results]


def parse_company(raw):
    """Transformer un résultat Pappers brut en format propre."""
    siege = raw.get("siege", {}) or {}
    dirigeants = raw.get("dirigeants", []) or []
    
    dirigeant_principal = {}
    if dirigeants:
        dirigeants_sorted = sorted(dirigeants, key=lambda d: d.get("age", 0) or 0, reverse=True)
        d = dirigeants_sorted[0]
        dirigeant_principal = {
            "nom": f"{d.get('prenom', '')} {d.get('nom', '')}".strip(),
            "age": d.get("age"),
            "qualite": d.get("qualite", ""),
            "date_naissance": d.get("date_de_naissance", ""),
        }
    
    return {
        "siren": raw.get("siren", ""),
        "siret": raw.get("siret", siege.get("siret", "")),
        "nom": raw.get("nom_entreprise", raw.get("denomination", "")),
        "nom_commercial": raw.get("nom_commercial", ""),
        "code_naf": raw.get("code_naf", ""),
        "libelle_naf": raw.get("libelle_code_naf", ""),
        "date_creation": raw.get("date_creation", ""),
        "forme_juridique": raw.get("forme_juridique", ""),
        "adresse": siege.get("adresse_ligne_1", ""),
        "code_postal": siege.get("code_postal", ""),
        "ville": siege.get("ville", ""),
        "departement": siege.get("departement", ""),
        "chiffre_affaires": raw.get("chiffre_affaires", raw.get("derniers_comptes", {}).get("chiffre_affaires") if raw.get("derniers_comptes") else None),
        "resultat": raw.get("resultat", raw.get("derniers_comptes", {}).get("resultat") if raw.get("derniers_comptes") else None),
        "effectif": raw.get("effectif", raw.get("tranche_effectif", "")),
        "dirigeant": dirigeant_principal,
        "nb_dirigeants": len(dirigeants),
        "procedure_collective": raw.get("procedure_collective_en_cours", False),
        "procedures": raw.get("procedures_collectives", []),
        "url_pappers": f"https://www.pappers.fr/entreprise/{raw.get('siren', '')}",
    }


def run_succession_hunt(api_token, codes_naf, **kwargs):
    """Canal 1: Chasse aux entreprises en succession."""
    print("\n" + "=" * 60)
    print("🔵 CANAL 1 — CHASSE SUCCESSION")
    print("   Dirigeant 55+, entreprise ancienne, pas de procédure")
    print("=" * 60)
    
    params = {
        "code_naf": ",".join(codes_naf) if codes_naf else None,
        "age_dirigeant_min": kwargs.get("age_min", 55),
        "age_dirigeant_max": kwargs.get("age_max", 80),
        "entreprise_cessee": "false",
        "date_creation_max": kwargs.get("date_creation_max", "01-01-2000"),
        "chiffre_affaires_min": kwargs.get("ca_min"),
        "chiffre_affaires_max": kwargs.get("ca_max"),
        "departement": ",".join(kwargs["departements"]) if kwargs.get("departements") else None,
        "region": ",".join(kwargs["regions"]) if kwargs.get("regions") else None,
        "par_page": 100,
    }
    params = {k: v for k, v in params.items() if v is not None}
    
    results = search_pappers(api_token, params, max_results=kwargs.get("max_results", 500))
    companies = [parse_company(r) for r in results]
    companies = [c for c in companies if not c.get("procedure_collective")]
    
    print(f"\n📊 {len(companies)} entreprises saines trouvées")
    return companies


def run_distressed_hunt(api_token, codes_naf, **kwargs):
    """Canal 2: Chasse aux entreprises en difficulté."""
    print("\n" + "=" * 60)
    print("🔴 CANAL 2 — CHASSE DISTRESSED")
    print("   Procédure collective en cours")
    print("=" * 60)
    
    params = {
        "code_naf": ",".join(codes_naf) if codes_naf else None,
        "entreprise_cessee": "false",
        "chiffre_affaires_min": kwargs.get("ca_min"),
        "chiffre_affaires_max": kwargs.get("ca_max"),
        "departement": ",".join(kwargs["departements"]) if kwargs.get("departements") else None,
        "region": ",".join(kwargs["regions"]) if kwargs.get("regions") else None,
        "par_page": 100,
    }
    params = {k: v for k, v in params.items() if v is not None}
    
    results = search_pappers(api_token, params, max_results=kwargs.get("max_results", 1000))
    companies = [parse_company(r) for r in results]
    companies = [c for c in companies if c.get("procedure_collective")]
    
    print(f"\n📊 {len(companies)} entreprises en procédure collective trouvées")
    return companies


def save_results(succession, distressed):
    """Sauvegarder les résultats en JSON."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    output = {
        "source": "Pappers API",
        "extracted_at": datetime.now().isoformat(),
        "succession": {"count": len(succession), "companies": succession},
        "distressed": {"count": len(distressed), "companies": distressed},
    }
    
    filepath = os.path.join(DATA_DIR, "pappers_results.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Sauvegardé: {filepath}")
    print(f"   Succession: {len(succession)} | Distressed: {len(distressed)}")
    return filepath


if __name__ == "__main__":
    import argparse
    import yaml
    
    parser = argparse.ArgumentParser(description="Pappers Hunter")
    parser.add_argument("--token", help="API token Pappers (ou via config.yaml)")
    parser.add_argument("--config", default="config.yaml", help="Fichier de configuration")
    parser.add_argument("--canal", choices=["succession", "distressed", "both"], default="both")
    parser.add_argument("--naf", nargs="*", help="Codes NAF (override config)")
    parser.add_argument("--dept", nargs="*", help="Départements")
    parser.add_argument("--age-min", type=int, help="Âge min dirigeant")
    parser.add_argument("--ca-min", type=int, help="CA minimum")
    parser.add_argument("--ca-max", type=int, help="CA maximum")
    parser.add_argument("--max", type=int, default=500, help="Max résultats par canal")
    args = parser.parse_args()
    
    config = {}
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.config)
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
    
    pappers_config = config.get("pappers", {})
    api_token = args.token or pappers_config.get("api_token", "")
    if not api_token or api_token == "VOTRE_TOKEN_ICI":
        api_token = os.environ.get("PAPPERS_API_TOKEN", "")
    if not api_token:
        print("❌ Token Pappers requis. Utilisez --token ou PAPPERS_API_TOKEN ou config.yaml")
        sys.exit(1)
    
    codes_naf = args.naf or pappers_config.get("codes_naf", [])
    succ_config = pappers_config.get("succession", {})
    kwargs = {
        "departements": args.dept or pappers_config.get("departements", []),
        "regions": pappers_config.get("regions", []),
        "age_min": args.age_min or succ_config.get("age_dirigeant_min", 55),
        "age_max": succ_config.get("age_dirigeant_max", 80),
        "ca_min": args.ca_min or succ_config.get("chiffre_affaires_min"),
        "ca_max": args.ca_max or succ_config.get("chiffre_affaires_max"),
        "date_creation_max": succ_config.get("date_creation_max", "01-01-2000"),
        "max_results": args.max,
    }
    
    succession, distressed = [], []
    if args.canal in ("succession", "both"):
        succession = run_succession_hunt(api_token, codes_naf, **kwargs)
    if args.canal in ("distressed", "both"):
        distressed = run_distressed_hunt(api_token, codes_naf, **kwargs)
    save_results(succession, distressed)
