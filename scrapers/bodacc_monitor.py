#!/usr/bin/env python3
"""
BODACC Monitor — Surveille les procédures collectives des agences d'intérim.
Focus : NAF 78.xx (travail temporaire, placement, mise à disposition de RH).
1. Filtre plein-texte BODACC sur les mots-clés cible (activité déclarée + raison sociale)
2. Enrichissement SIRENE (NAF, effectif, date de création) via recherche-entreprises.api.gouv.fr
Génère bodacc_alerts.json pour le dashboard.
"""

import requests
import json
import os
import re
import time
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
BODACC_API = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records"
SIRENE_API = "https://recherche-entreprises.api.gouv.fr/search"

DEFAULT_KEYWORDS = ["travail temporaire", "intérim", "interim", "agence d'emploi",
                    "mise à disposition de personnel", "portage salarial"]
DEFAULT_NAF_PREFIXES = ["78.20", "78.10", "78.30"]


def load_config(name="config.yaml"):
    """config.yaml est à la racine du repo ; on tolère aussi scrapers/."""
    here = os.path.dirname(os.path.abspath(__file__))
    for path in (os.path.join(here, name), os.path.join(here, "..", name)):
        if os.path.exists(path):
            try:
                import yaml
                with open(path) as f:
                    return yaml.safe_load(f) or {}
            except ImportError:
                break
    return {}


def fetch_bodacc_collectif(jours_lookback=30, departements=None, size=100, keywords=None):
    """Récupère les annonces BODACC de type 'collectif' (procédures collectives).
    keywords : filtre plein-texte côté API sur l'activité déclarée et la raison sociale
    (le BODACC ne porte pas de code NAF)."""
    date_from = (datetime.now() - timedelta(days=jours_lookback)).strftime("%Y-%m-%d")
    
    where_clauses = [
        f"familleavis_lib = 'Procédures collectives'",
        f"dateparution >= '{date_from}'",
    ]
    
    if keywords:
        kw_filter = " OR ".join(
            f"listepersonnes LIKE '%{k}%' OR commercant LIKE '%{k}%'"
            for k in keywords if "'" not in k
        )
        where_clauses.append(f"({kw_filter})")
    
    if departements:
        dept_filter = " OR ".join([f"departement_code_etablissement = '{d}'" for d in departements])
        where_clauses.append(f"({dept_filter})")
    
    where = " AND ".join(where_clauses)
    
    all_records = []
    offset = 0
    
    while True:
        params = {
            "where": where,
            "order_by": "dateparution DESC",
            "limit": min(size, 100),
            "offset": offset,
        }
        
        print(f"  📡 BODACC API offset={offset}...")
        try:
            resp = requests.get(BODACC_API, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  ⚠ Erreur API BODACC: {e}")
            break
        
        results = data.get("results", [])
        if not results:
            break
        
        all_records.extend(results)
        offset += len(results)
        
        if offset >= size or offset >= data.get("total_count", 0):
            break
    
    return all_records


def _json_field(record, key):
    raw = record.get(key)
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return {}


def _first_personne(record):
    p = _json_field(record, "listepersonnes").get("personne", {})
    return p[0] if isinstance(p, list) and p else (p if isinstance(p, dict) else {})


def extract_siren(registre):
    for r in (registre if isinstance(registre, list) else [registre or ""]):
        digits = re.sub(r"\D", "", str(r))
        if len(digits) == 9:
            return digits
    return ""


def parse_record(record):
    """Transformer un record BODACC brut (API v2.1) en format propre."""
    jugement = _json_field(record, "jugement")
    personne = _first_personne(record)
    return {
        "id": record.get("id", ""),
        "date_parution": record.get("dateparution", ""),
        "numero_annonce": record.get("numeroannonce", ""),
        "tribunal": record.get("tribunal", ""),
        "type_annonce": record.get("typeavis_lib", ""),
        "famille": record.get("familleavis_lib", ""),
        "nature": jugement.get("nature", ""),
        "nom_entreprise": record.get("commercant", ""),
        "registre": record.get("registre", ""),
        "siren": extract_siren(record.get("registre", "")),
        "activite": personne.get("activite", "") or "",
        "ville": record.get("ville", ""),
        "departement": record.get("numerodepartement", ""),
        "code_postal": record.get("cp", ""),
        "contenu": " — ".join(x for x in (jugement.get("famille"), jugement.get("nature"),
                                          jugement.get("complementJugement")) if x),
        "url_bodacc": record.get("url_complete", ""),
        # Remplis par enrich_sirene()
        "code_naf": "", "tranche_effectif": "", "date_creation": "",
        "nb_etablissements": "", "nom_sirene": "", "naf_cible": False,
    }


def enrich_sirene(records, naf_prefixes, pause=0.15):
    """Ajoute NAF / effectif / création via l'API SIRENE publique (gratuite, ~7 req/s)."""
    cache = {}
    for r in records:
        siren = r.get("siren")
        if not siren:
            continue
        if siren not in cache:
            try:
                resp = requests.get(SIRENE_API, params={"q": siren, "per_page": 1}, timeout=15)
                hit = (resp.json().get("results") or [{}])[0] if resp.ok else {}
            except Exception as e:
                print(f"  ⚠ SIRENE {siren}: {e}")
                hit = {}
            cache[siren] = hit
            time.sleep(pause)
        hit = cache[siren]
        r["code_naf"] = hit.get("activite_principale", "") or ""
        r["tranche_effectif"] = hit.get("tranche_effectif_salarie", "") or ""
        r["date_creation"] = hit.get("date_creation", "") or ""
        r["nb_etablissements"] = hit.get("nombre_etablissements", "") or ""
        r["nom_sirene"] = hit.get("nom_complet", "") or ""
        r["naf_cible"] = any(r["code_naf"].startswith(p) for p in naf_prefixes)
    return records


def classify_procedure(record):
    """Classifier le type de procédure à partir du contenu."""
    contenu = (record.get("contenu", "") + " " + record.get("nature", "")).lower()
    
    if "redressement judiciaire" in contenu:
        if "ouverture" in contenu or "jugement d'ouverture" in contenu:
            return "🟡 Ouverture RJ"
        elif "plan de cession" in contenu:
            return "🔴 Plan de cession (RJ)"
        elif "plan de continuation" in contenu or "plan de redressement" in contenu:
            return "🟢 Plan de continuation"
        return "🟡 RJ"
    elif "liquidation judiciaire" in contenu:
        if "ouverture" in contenu:
            return "🔴 Ouverture LJ"
        elif "cession" in contenu:
            return "🔴 Plan de cession (LJ)"
        elif "clôture" in contenu:
            return "⚪ Clôture LJ"
        return "🔴 LJ"
    elif "sauvegarde" in contenu:
        return "🟢 Sauvegarde"
    elif "plan de cession" in contenu:
        return "🔴 Plan de cession"
    
    return "⚪ Autre"


def run_monitor(jours_lookback=30, departements=None, max_results=500,
                keywords=None, naf_prefixes=None, enrich=True):
    """Exécuter le monitoring BODACC (cible intérim)."""
    keywords = keywords if keywords is not None else DEFAULT_KEYWORDS
    naf_prefixes = naf_prefixes or DEFAULT_NAF_PREFIXES
    print("=" * 60)
    print("🟡 BODACC MONITOR — Procédures collectives · INTÉRIM")
    print(f"   Période: {jours_lookback} derniers jours")
    print(f"   Mots-clés: {', '.join(keywords) if keywords else '(aucun)'}")
    if departements:
        print(f"   Départements: {', '.join(departements)}")
    print("=" * 60)
    
    records = fetch_bodacc_collectif(
        jours_lookback=jours_lookback,
        departements=departements,
        size=max_results,
        keywords=keywords,
    )
    
    print(f"\n📊 {len(records)} annonces collectif trouvées")
    
    parsed = []
    for r in records:
        p = parse_record(r)
        p["type_procedure"] = classify_procedure(p)
        parsed.append(p)
    
    if enrich and parsed:
        print(f"\n🔎 Enrichissement SIRENE ({len(parsed)} annonces)...")
        enrich_sirene(parsed, naf_prefixes)
        n_naf = sum(1 for p in parsed if p.get("naf_cible"))
        print(f"   {n_naf} avec NAF cible ({', '.join(naf_prefixes)}), "
              f"{len(parsed) - n_naf} retenues par mot-clé seulement")
    
    priority_order = {
        "🔴 Plan de cession (RJ)": 0,
        "🔴 Plan de cession (LJ)": 1,
        "🔴 Plan de cession": 2,
        "🔴 Ouverture LJ": 3,
        "🟡 Ouverture RJ": 4,
        "🟡 RJ": 5,
        "🟢 Sauvegarde": 6,
        "🟢 Plan de continuation": 7,
        "⚪ Clôture LJ": 8,
        "⚪ Autre": 9,
    }
    parsed.sort(key=lambda x: priority_order.get(x["type_procedure"], 99))
    
    from collections import Counter
    type_counts = Counter(p["type_procedure"] for p in parsed)
    print("\n📈 Répartition:")
    for t, c in type_counts.most_common():
        print(f"   {t}: {c}")
    
    return parsed


def save_results(records):
    """Sauvegarder en JSON."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    output = {
        "source": "BODACC API",
        "scraped_at": datetime.now().isoformat(),
        "count": len(records),
        "records": records,
    }
    
    filepath = os.path.join(DATA_DIR, "bodacc_alerts.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Sauvegardé: {filepath} ({len(records)} annonces)")
    return filepath


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BODACC Collectif Monitor")
    parser.add_argument("--days", type=int, default=30, help="Nombre de jours en arrière")
    parser.add_argument("--dept", nargs="*", help="Départements (ex: 75 92 69)")
    parser.add_argument("--max", type=int, default=500, help="Nombre max de résultats")
    parser.add_argument("--keywords", nargs="*", help="Mots-clés plein-texte (override config.cible.mots_cles)")
    parser.add_argument("--no-enrich", action="store_true", help="Sans enrichissement SIRENE")
    args = parser.parse_args()

    config = load_config()
    cible = config.get("cible", {})
    bodacc_cfg = config.get("bodacc", {})

    records = run_monitor(
        jours_lookback=args.days,
        departements=args.dept,
        max_results=args.max,
        keywords=args.keywords if args.keywords is not None else cible.get("mots_cles", DEFAULT_KEYWORDS),
        naf_prefixes=cible.get("naf_prefixes", DEFAULT_NAF_PREFIXES),
        enrich=not args.no_enrich and bodacc_cfg.get("enrichir_sirene", True),
    )
    save_results(records)
