#!/usr/bin/env python3
"""
Diversification Hunter — axes voisins du luxe proposés par l'expert (brief « Diversification par rachat », 11/2026).
Même source et même logique que sirene_hunter.py (SIRENE gratuit, holdings remontées), filtres de l'entonnoir expert :
dirigeant ≥ 62 ans, société créée avant 2005, résultat positif quand il est connu. Île-de-France, Paris d'abord.

Sortie : data/diversification.json (lu par cibles.html, filtre « Diversification »).
Les sociétés déjà dans pappers_results.json / ajouts_manuels.json ne sont pas reprises (déjà triées),
sauf celles de A_ROUVRIR, rouvertes avec une note sans toucher à leur statut.

  python3 scrapers/diversification_hunter.py
"""

import json
import os
import re
import time
from datetime import datetime

import requests

from sirene_hunter import API, DATA_DIR, PAUSE, PER_PAGE, dirigeant_via_holding, parse_company

DEPTS = ["75", "92", "93", "94", "95", "77", "78", "91"]  # ordre = priorité (Paris d'abord)
AGE_MIN = 62
CREATION_MAX = "2004-12-31"
# bruit des requêtes par mot-clé : centres commerciaux, promotion immobilière, filières agricoles, écoles…
EXCLURE = re.compile(r"\bGIE\b|\bG\.?I\.?E\b|CENTRE COMMERCIAL|CTRE|COMMER[CÇ]ANTS|IMMOBILI|CENTURY|FIDUCIAIRE|CHARCUTERIE|"
                     r"BIERE|AUTO MOTO|ECOLE|INSTITUT SUP|EDITIONS", re.I)
NATURE_GIE = "6220"  # nature juridique INSEE : groupement d'intérêt économique

# axe → requêtes SIRENE (params) + NAF admis au siège + mots-clés exigés dans le nom (None = pas de filtre)
AXES = {
    "Formation vente / clienteling / langues": {
        "naf": ["85.59A"],
        "queries": [{"activite_principale": "85.59A", "resultat_net_min": 1,
                     "tranche_effectif_salarie": "02,03,11,12,21,22"}],
        "kw": r"vente|vendeur|commerc|retail|luxe|client|accueil|langue|language|linguist|anglais|beaut|parfum|cosm|esth|mode|fashion|h[oô]tel|protocole|prestige|excellence|savoir",
    },
    "Animation / force de vente / merchandising": {
        "naf": ["73.11Z", "73.12Z", "82.99Z", "82.30Z"],
        "queries": [{"activite_principale": "73.11Z,73.12Z,82.99Z,82.30Z", "q": k} for k in
                    ["animation", "promotion", "merchandising", "demonstration", "force de vente", "terrain",
                     "field", "hotesse", "trade marketing", "marketing operationnel", "street"]],
        "kw": None,
    },
    "Interprètes / multilingue": {
        "naf": ["74.30Z"],
        "queries": [{"activite_principale": "74.30Z", "resultat_net_min": 1}],
        "kw": None,
    },
    "Intérim IDF hors Paris / 92": {
        "naf": ["78.20Z"],
        "queries": [{"activite_principale": "78.20Z", "departements": ["93", "94", "95", "77", "78", "91"]}],
        "kw": None,
    },
    "Intérim / placement spécialisé (santé, beauté, aéroport, gastronomie, création)": {
        "naf": ["78.10Z", "78.20Z", "78.30Z"],
        "queries": [{"activite_principale": "78.10Z,78.20Z,78.30Z", "q": k} for k in
                    ["sante", "medical", "pharma", "optique", "audition", "dentaire", "esthetique", "beaute",
                     "spa", "coiffure", "aeroport", "airport", "travel", "cuisine", "traiteur", "sommelier",
                     "restauration", "hotel", "creation", "design", "artist", "style"]],
        "kw": None,
    },
    "Mannequins / showroom": {
        "naf": ["78.10Z", "74.90B", "90.01Z", "73.11Z"],
        "queries": [{"activite_principale": "78.10Z,74.90B,90.01Z,73.11Z", "q": k} for k in
                    ["model", "models", "mannequin", "mannequins", "casting", "showroom", "talents"]],
        "kw": None,
    },
}

# déjà dans la base et écartées, à rouvrir à la lumière de l'avis expert (statut inchangé)
A_ROUVRIR = {
    "304141625": "Avis expert 16/11 : intérim pur (même garantie financière, paie, convention) — l'intégration la plus simple de la liste, meilleur résultat (271 k€). Clientèle architecture / bureaux d'études = vraie diversification.",
    "431348432": "Avis expert 16/11 : « retail santé » = son pari contrariant (pénurie de candidats, demande décorrélée du luxe). Intérim pharmacie, 5,5 M€ de CA, dirigeant 76 ans.",
}

# vérifications web (16/11/2026) : appartenance à un groupe, activité réelle
NOTES = {
    "443863923": "Groupe IFMG / Marvesting (avec Impact Sales & Marketing et Atmosphères), ~55 M€ cumulés, dirigeant 73 ans. Force de vente supplétive, animation, merchandising (Levallois). Trop gros seul, mais transmission de groupe à surveiller.",
    "452223753": "Groupe IFMG / Marvesting (voir Input Sales Forces) — 39 M€, 500-999 salariés : hors taille.",
    "443553904": "Groupe IFMG / Marvesting (voir Input Sales Forces).",
    "314900481": "Rachetée par le groupe Xynergy fin 2022 (marque Comme J'aime) : pas une succession.",
    "602006306": "Groupe Cegedim : hors cible.",
    "692043250": "Service Innovation Group (réseau international, 27 établissements) : hors cible.",
    "414911255": "CPM : marque de field marketing d'un groupe international (Omnicom) — appartenance à vérifier.",
    "429756828": "Intérim généraliste Val-de-Marne (BTP, technique, tertiaire), ~20 ans, 4 M€ : pas de niche luxe.",
    "412083206": "Traduction et interprétation (simultanée, consécutive, liaison), Paris 8e. Clientèle luxe non vérifiée. Métier exposé à la traduction automatique.",
    "328679592": "Filiale française du groupe familial allemand KERN (langues) : la succession se joue au niveau du groupe.",
}


def fetch(params, depts):
    out, page = [], 1
    while page <= 400:
        p = {k: v for k, v in params.items() if k != "departements"}
        p.update({"departement": ",".join(depts), "etat_administratif": "A", "per_page": PER_PAGE, "page": page})
        try:
            r = requests.get(API, params=p, timeout=30)
            if r.status_code == 429:
                time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  ⚠ {params} page {page}: {e}")
            break
        out += data.get("results", [])
        if page >= data.get("total_pages", 0):
            break
        page += 1
        time.sleep(PAUSE)
    return out


def releve_familiale(raw, year):
    """Co-dirigeant personne physique de moins de 50 ans portant le nom du dirigeant le plus âgé."""
    pp = [d for d in raw.get("dirigeants") or [] if d.get("type_dirigeant") == "personne physique"
          and str(d.get("annee_de_naissance") or "").isdigit()]
    if len(pp) < 2:
        return False
    doyen = max(pp, key=lambda d: year - int(d["annee_de_naissance"]))
    nom = (doyen.get("nom") or "").split("(")[0].strip().upper()
    return any(d is not doyen and year - int(d["annee_de_naissance"]) < 50
               and (d.get("nom") or "").split("(")[0].strip().upper() == nom for d in pp)


def run():
    year = datetime.now().year
    known = {c["siren"] for c in json.load(open(os.path.join(DATA_DIR, "pappers_results.json")))["succession"]["companies"]}
    known |= {c["siren"] for c in json.load(open(os.path.join(DATA_DIR, "ajouts_manuels.json")))["companies"]}
    found = {}
    for axe, cfg in AXES.items():
        kw = re.compile(cfg["kw"], re.I) if cfg["kw"] else None
        raws = []
        for q in cfg["queries"]:
            raws += fetch(q, q.get("departements") or DEPTS)
        n0 = len(found)
        for raw in raws:
            siren = raw.get("siren")
            if not siren or siren in known or siren in found:
                continue
            c = parse_company(raw, year)
            if c["departement"] not in DEPTS or c["code_naf"] not in cfg["naf"] or c["categorie"] == "GE":
                continue
            if not c["date_creation"] or c["date_creation"] > CREATION_MAX:
                continue
            if c["resultat"] is not None and c["resultat"] <= 0:
                continue
            if EXCLURE.search(c["nom"]) or str(raw.get("nature_juridique")) == NATURE_GIE:
                continue
            if kw and not kw.search(" ".join([c["nom"], c["nom_commercial"], c["enseignes"]])):
                continue
            if not c["dirigeant"].get("age"):
                c["dirigeant"] = dirigeant_via_holding(raw, year) or c["dirigeant"]
            age = c["dirigeant"].get("age")
            if not c["dirigeant"] or (age is not None and not AGE_MIN <= age <= 85):
                continue
            signaux = []
            if age is None:
                signaux.append("âge du dirigeant inconnu (holding)")
            if releve_familiale(raw, year):
                signaux.append("relève familiale probable (co-dirigeant < 50 ans, même nom)")
            if c["annee_comptes"] is None:
                signaux.append("aucun compte publié")
            elif int(c["annee_comptes"]) < year - 3:
                signaux.append(f"derniers comptes {c['annee_comptes']} : sommeil possible")
            c["diversif"] = {"axe": axe, "signaux": signaux, "note": NOTES.get(siren, "")}
            found[siren] = c
        print(f"  {axe}: {len(raws)} lus → {len(found) - n0} retenus")

    order = {d: i for i, d in enumerate(DEPTS)}
    companies = sorted(found.values(), key=lambda c: (order.get(c["departement"], 99), -(c["dirigeant"].get("age") or 0)))
    rouvrir = [{"siren": s, "diversif": {"axe": "À rouvrir (avis expert)", "signaux": [], "note": n}} for s, n in A_ROUVRIR.items()]
    out = {
        "_lisezmoi": "Axes de diversification (avis expert 16/11/2026) — généré par scrapers/diversification_hunter.py. "
                     "Filtres : IDF, dirigeant ≥ 62 ans, créée avant 2005, résultat > 0 si connu, holdings remontées.",
        "extracted_at": datetime.now().isoformat(),
        "companies": companies + rouvrir,
    }
    path = os.path.join(DATA_DIR, "diversification.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"✅ {len(companies)} sociétés + {len(rouvrir)} à rouvrir → {path}")


if __name__ == "__main__":
    run()
