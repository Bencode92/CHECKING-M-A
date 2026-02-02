#!/usr/bin/env python3
"""
BODACC Monitor — Surveille les ouvertures de procédures collectives.
Focus : jugements d'ouverture RJ/LJ/Sauvegarde (= entreprises potentiellement à reprendre).
Génère bodacc_alerts.json pour le dashboard.
"""

import requests
import json
import os
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
BODACC_API = "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/annonces-commerciales/records"


def fetch_bodacc_collectif(jours_lookback=30, departements=None, size=100):
    """Récupère les annonces BODACC de type 'collectif' (procédures collectives)."""
    date_from = (datetime.now() - timedelta(days=jours_lookback)).strftime("%Y-%m-%d")
    
    where_clauses = [
        f"familleavis_lib = 'Procédures collectives'",
        f"dateparution >= '{date_from}'",
    ]
    
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


def parse_record(record):
    """Transformer un record BODACC brut en format propre."""
    return {
        "id": record.get("id_annonce", ""),
        "date_parution": record.get("dateparution", ""),
        "numero_annonce": record.get("numeroannonce", ""),
        "tribunal": record.get("tribunal", ""),
        "type_annonce": record.get("typeavis_lib", ""),
        "famille": record.get("familleavis_lib", ""),
        "nature": record.get("nature", ""),
        "nom_entreprise": record.get("commercant", record.get("personne", "")),
        "registre": record.get("registre", ""),
        "ville": record.get("ville", ""),
        "departement": record.get("departement_code_etablissement", ""),
        "code_postal": record.get("cp", ""),
        "contenu": record.get("contenu_annonce", record.get("jugement", "")),
        "url_bodacc": f"https://www.bodacc.fr/annonce/detail/{record.get('id_annonce', '')}" if record.get("id_annonce") else "",
    }


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


def run_monitor(jours_lookback=30, departements=None, max_results=500):
    """Exécuter le monitoring BODACC."""
    print("=" * 60)
    print("🟡 BODACC MONITOR — Procédures collectives")
    print(f"   Période: {jours_lookback} derniers jours")
    if departements:
        print(f"   Départements: {', '.join(departements)}")
    print("=" * 60)
    
    records = fetch_bodacc_collectif(
        jours_lookback=jours_lookback,
        departements=departements,
        size=max_results,
    )
    
    print(f"\n📊 {len(records)} annonces collectif trouvées")
    
    parsed = []
    for r in records:
        p = parse_record(r)
        p["type_procedure"] = classify_procedure(p)
        parsed.append(p)
    
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
    args = parser.parse_args()

    records = run_monitor(
        jours_lookback=args.days,
        departements=args.dept,
        max_results=args.max,
    )
    save_results(records)
