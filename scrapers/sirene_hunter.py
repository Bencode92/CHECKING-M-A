#!/usr/bin/env python3
"""
SIRENE Hunter — Chasse succession sur les agences d'intérim d'Île-de-France.
Source : recherche-entreprises.api.gouv.fr (gratuit, sans token, ~7 req/s).
Donne : dirigeants (année de naissance), CA / résultat (derniers comptes déposés),
effectif, date de création, siège.

Sortie : data/pappers_results.json (même format que pappers_hunter → onglet Pappers
du dashboard) + data/cibles_interim_idf.csv trié pour lecture / tri Excel.

  python sirene_hunter.py                     # config.yaml : NAF 78.xx, 75+92 d'abord
  python sirene_hunter.py --dept 75 92        # Paris + Hauts-de-Seine seulement
  python sirene_hunter.py --age-min 60        # dirigeant 60+
  python sirene_hunter.py --all               # sans filtre d'âge (liste complète)
"""

import csv
import json
import os
import time
from datetime import datetime

import requests

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
API = "https://recherche-entreprises.api.gouv.fr/search"
PER_PAGE = 25
PAUSE = 0.15  # ~7 req/s max côté API

DEFAULT_NAF = ["78.20Z", "78.10Z", "78.30Z"]
DEFAULT_DEPTS = ["75", "92", "93", "94", "78", "91", "95", "77"]  # ordre = priorité

NAF_LIBELLES = {
    "78.20Z": "Activités des agences de travail temporaire",
    "78.10Z": "Activités des agences de placement de main-d'œuvre",
    "78.30Z": "Autre mise à disposition de ressources humaines",
}
EFFECTIF = {
    "NN": "n.r.", "00": "0", "01": "1-2", "02": "3-5", "03": "6-9", "11": "10-19",
    "12": "20-49", "21": "50-99", "22": "100-199", "31": "200-249", "32": "250-499",
    "41": "500-999", "42": "1000-1999", "51": "2000-4999", "52": "5000-9999", "53": "10000+",
}
# Qualités qui ne sont pas des dirigeants opérationnels
QUALITES_EXCLUES = ("commissaire", "liquidateur", "administrateur judiciaire", "mandataire")


def load_config(name="config.yaml"):
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


def fetch_all(naf_codes, departements, max_pages=400):
    """Pagine l'API pour un ou plusieurs départements (matching sur établissements,
    on filtre ensuite sur le siège)."""
    results = []
    for dept in departements:
        page, total = 1, None
        while page <= max_pages:
            params = {
                "activite_principale": ",".join(naf_codes),
                "departement": dept,
                "etat_administratif": "A",
                "per_page": PER_PAGE,
                "page": page,
            }
            try:
                resp = requests.get(API, params=params, timeout=30)
                if resp.status_code == 429:
                    time.sleep(2)
                    continue
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  ⚠ dept {dept} page {page}: {e}")
                break
            if total is None:
                total = data.get("total_results", 0)
                print(f"  📡 dept {dept}: {total} sociétés actives ({data.get('total_pages', '?')} pages)")
            batch = data.get("results", [])
            if not batch:
                break
            results.extend(batch)
            if page >= data.get("total_pages", 0):
                break
            page += 1
            time.sleep(PAUSE)
    # dédoublonnage (une société peut sortir sur plusieurs départements)
    seen, uniq = set(), []
    for r in results:
        if r.get("siren") not in seen:
            seen.add(r.get("siren"))
            uniq.append(r)
    return uniq


def dirigeant_principal(raw, year):
    """Dirigeant personne physique le plus âgé (hors CAC / mandataires)."""
    best = None
    for d in raw.get("dirigeants") or []:
        if d.get("type_dirigeant") != "personne physique":
            continue
        q = (d.get("qualite") or "").lower()
        if any(x in q for x in QUALITES_EXCLUES):
            continue
        an = d.get("annee_de_naissance")
        age = year - int(an) if an and str(an).isdigit() else None
        cand = {
            "nom": f"{(d.get('prenoms') or '').split(',')[0].strip().title()} {(d.get('nom') or '').upper()}".strip(),
            "age": age,
            "qualite": d.get("qualite") or "",
            "date_naissance": d.get("date_de_naissance") or "",
        }
        if best is None or (age or 0) > (best["age"] or 0):
            best = cand
    return best or {}


def derniers_comptes(raw):
    fin = raw.get("finances") or {}
    if not fin:
        return None, None, None
    an = max(fin.keys())
    return an, fin[an].get("ca"), fin[an].get("resultat_net")


def score_succession(c, age_min):
    s = 0
    age = c["dirigeant"].get("age") or 0
    if age >= 65: s += 35
    elif age >= 60: s += 25
    elif age >= age_min: s += 15
    ca = c.get("chiffre_affaires") or 0
    if ca >= 5_000_000: s += 25
    elif ca >= 2_000_000: s += 20
    elif ca >= 1_000_000: s += 10
    if c.get("date_creation"):
        anc = datetime.now().year - int(c["date_creation"][:4])
        if anc >= 20: s += 20
        elif anc >= 10: s += 15
        elif anc >= 5: s += 8
    if c.get("effectif_code") not in (None, "", "NN", "00"): s += 5
    if not c.get("procedure_collective"): s += 15
    return s


def parse_company(raw, year):
    siege = raw.get("siege") or {}
    an, ca, res = derniers_comptes(raw)
    naf = siege.get("activite_principale") or raw.get("activite_principale") or ""
    eff = raw.get("tranche_effectif_salarie") or "NN"
    slug = "".join(ch if ch.isalnum() else "-" for ch in (raw.get("nom_complet") or "").lower()).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return {
        "siren": raw.get("siren", ""),
        "siret": siege.get("siret", ""),
        "nom": raw.get("nom_raison_sociale") or raw.get("nom_complet") or "",
        "nom_commercial": siege.get("nom_commercial") or "",
        "enseignes": ", ".join(siege.get("liste_enseignes") or []),
        "code_naf": naf,
        "libelle_naf": NAF_LIBELLES.get(naf, ""),
        "date_creation": raw.get("date_creation") or "",
        "forme_juridique": raw.get("nature_juridique") or "",
        "categorie": raw.get("categorie_entreprise") or "",
        "adresse": siege.get("adresse") or "",
        "code_postal": siege.get("code_postal") or "",
        "ville": siege.get("libelle_commune") or "",
        "departement": siege.get("departement") or "",
        "chiffre_affaires": ca,
        "resultat": res,
        "annee_comptes": an,
        "effectif_code": eff,
        "effectif": EFFECTIF.get(eff, eff),
        "nb_etablissements": raw.get("nombre_etablissements") or 0,
        "dirigeant": dirigeant_principal(raw, year),
        "nb_dirigeants": sum(1 for d in raw.get("dirigeants") or [] if d.get("type_dirigeant") == "personne physique"),
        "procedure_collective": False,  # non fourni par SIRENE → voir onglet BODACC
        "url_pappers": f"https://www.pappers.fr/entreprise/{slug}-{raw.get('siren', '')}",
        "url_annuaire": f"https://annuaire-entreprises.data.gouv.fr/entreprise/{raw.get('siren', '')}",
    }


def run(naf_codes, departements, age_min=55, age_max=85, ca_min=None, exclude_categories=("GE", "ETI"), keep_all=False):
    year = datetime.now().year
    print("=" * 60)
    print("🔵 SIRENE HUNTER — Agences d'intérim, chasse succession")
    print(f"   NAF: {', '.join(naf_codes)}  |  Départements: {', '.join(departements)}")
    print(f"   Filtre: dirigeant ≥ {age_min} ans" + (f", CA ≥ {ca_min:,} €" if ca_min else "") +
          ("  [--all : pas de filtre d'âge]" if keep_all else ""))
    print("=" * 60)

    raw = fetch_all(naf_codes, departements)
    print(f"\n📊 {len(raw)} sociétés uniques récupérées")

    companies = []
    for r in raw:
        c = parse_company(r, year)
        if c["departement"] not in departements:      # siège hors cible
            continue
        if c["code_naf"] not in naf_codes:            # NAF du siège hors cible (matching API = tout établissement)
            continue
        if c["categorie"] in exclude_categories:      # Adecco, Manpower…
            continue
        if not c["dirigeant"].get("age"):             # pas de dirigeant PP daté
            continue
        if not keep_all and c["dirigeant"]["age"] < age_min:
            continue
        if c["dirigeant"]["age"] > age_max:           # registre non mis à jour (dirigeant 90+)
            continue
        if ca_min and (c["chiffre_affaires"] or 0) < ca_min:
            continue
        c["score"] = score_succession(c, age_min)
        companies.append(c)

    order = {d: i for i, d in enumerate(departements)}
    companies.sort(key=lambda c: (order.get(c["departement"], 99), -c["score"], -(c["chiffre_affaires"] or 0)))

    from collections import Counter
    print(f"✅ {len(companies)} cibles retenues")
    for d, n in sorted(Counter(c["departement"] for c in companies).items(), key=lambda x: order.get(x[0], 99)):
        print(f"   {d}: {n}")
    return companies


def save(companies, departements):
    os.makedirs(DATA_DIR, exist_ok=True)
    out = {
        "source": "SIRENE (recherche-entreprises.api.gouv.fr)",
        "extracted_at": datetime.now().isoformat(),
        "departements": departements,
        "succession": {"count": len(companies), "companies": companies},
        "distressed": {"count": 0, "companies": []},
    }
    jpath = os.path.join(DATA_DIR, "pappers_results.json")
    with open(jpath, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    cpath = os.path.join(DATA_DIR, "cibles_interim_idf.csv")
    cols = ["score", "departement", "ville", "nom", "enseignes", "code_naf", "dirigeant", "age", "qualite",
            "chiffre_affaires", "resultat", "annee_comptes", "effectif", "date_creation", "nb_etablissements",
            "siren", "adresse", "url_pappers", "url_annuaire"]
    with open(cpath, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(cols)
        for c in companies:
            d = c["dirigeant"]
            w.writerow([c["score"], c["departement"], c["ville"], c["nom"], c["enseignes"], c["code_naf"],
                        d.get("nom", ""), d.get("age", ""), d.get("qualite", ""),
                        c["chiffre_affaires"] or "", c["resultat"] or "", c["annee_comptes"] or "",
                        c["effectif"], c["date_creation"], c["nb_etablissements"],
                        c["siren"], c["adresse"], c["url_pappers"], c["url_annuaire"]])
    print(f"\n💾 {jpath}\n💾 {cpath}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="SIRENE Hunter — intérim IDF, succession")
    p.add_argument("--dept", nargs="*", help="Départements (défaut config / IDF, ordre = priorité)")
    p.add_argument("--naf", nargs="*", help="Codes NAF (défaut config.cible)")
    p.add_argument("--age-min", type=int, help="Âge mini du dirigeant (défaut config / 55)")
    p.add_argument("--ca-min", type=int, help="CA mini en € (optionnel)")
    p.add_argument("--all", action="store_true", help="Sans filtre d'âge")
    p.add_argument("--keep-grands", action="store_true", help="Garder aussi ETI / GE")
    a = p.parse_args()

    cfg = load_config()
    cible, succ = cfg.get("cible", {}), cfg.get("pappers", {}).get("succession", {})
    depts = a.dept or cible.get("departements") or DEFAULT_DEPTS
    naf = a.naf or cfg.get("pappers", {}).get("codes_naf") or DEFAULT_NAF
    age_min = a.age_min or succ.get("age_dirigeant_min", 55)
    age_max = succ.get("age_dirigeant_max", 85)

    companies = run(naf, depts, age_min=age_min, age_max=age_max, ca_min=a.ca_min,
                    exclude_categories=() if a.keep_grands else ("GE", "ETI"), keep_all=a.all)
    save(companies, depts)
