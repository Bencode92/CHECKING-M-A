# 🔍 CHECKING M&A — Acquisition Pipeline · **Intérim**

Dashboard et scrapers automatisés pour identifier des **agences d'intérim / travail temporaire** françaises à reprendre.

> Cible unique : **NAF 78.20Z** (ETT), **78.10Z** (placement / recrutement), **78.30Z** (mise à disposition de RH).
> Tout se règle dans `config.yaml` → bloc `cible` (préfixes NAF + mots-clés).

## Suivi des cibles & planning (`cibles.html`)

Tableau CRM des agences (liste SIRENE) : ★, statut (à qualifier → intéressant → à contacter → contacté → RDV / écarté),
secteurs, notes, contact, date de relance, journal ; vue **Planning** par semaine + kanban ; export CSV.
Le suivi est stocké dans le navigateur (`localStorage`) — bouton « Sauvegarder suivi » pour l'exporter en JSON,
« Restaurer » pour le recharger (ou déposer le fichier en `data/annotations.json`).

Ouvrir : double-clic sur **`Ouvrir Cibles.command`** (sert le dossier en local) — un `file://` ne peut pas lire `data/`.

### Qualification IA (site web, secteurs, intérêt)
```bash
npm install
export ANTHROPIC_API_KEY=sk-ant-...
node scrapers/enrich_ai.mjs --limit 20        # test
node scrapers/enrich_ai.mjs                   # tout (≈ 3 recherches web / société, cache dans data/enrichment.json)
```
Claude (`claude-opus-5` + recherche web) trouve le site officiel, les secteurs servis, la spécialité, les signaux
(groupe / indépendant / franchise…) et note l'intérêt 1-5 pour un repreneur. Ordre de grandeur : ~5 ¢ par société.

## Architecture

```
├── cibles.html                 # Suivi des cibles : tableau, fiches, statuts, planning, kanban
├── dashboard.html              # Sources : Pappers / Actify / BODACC
├── Ouvrir Cibles.command       # Double-clic → serveur local + ouverture de cibles.html
├── config.yaml                 # Cible (NAF + mots-clés), filtres Pappers, paramètres
├── scrapers/
│   ├── sirene_hunter.py        # API SIRENE gratuite — chasse succession IDF (dirigeants, CA, effectif)
│   ├── enrich_ai.mjs           # Qualification IA (Claude + web search) → data/enrichment.json
│   ├── pappers_hunter.py       # API Pappers — succession & distressed (filtre NAF natif, token)
│   ├── actify_scraper.py       # Scraper Actify — reprises à la barre (filtre mots-clés)
│   └── bodacc_monitor.py       # API BODACC — procédures collectives (mots-clés + enrichissement SIRENE)
├── data/                       # JSON générés par les scrapers
│   ├── pappers_results.json
│   ├── actify_listings.json
│   └── bodacc_alerts.json
└── .github/workflows/
    └── daily_scan.yml          # Cron Lun-Ven 08h15 Paris (BODACC + Actify ; Pappers si secret)
```

## 3 Sources de données

| Source | Type | Comment on cible l'intérim | Accès |
|--------|------|----------------------------|-------|
| **Pappers** | API | Filtre `code_naf` natif (78.20Z / 78.10Z / 78.30Z) + âge dirigeant, CA, procédure | Token API (gratuit 100 crédits) |
| **BODACC** | API | Le BODACC n'a pas de NAF → filtre plein-texte (« travail temporaire », « intérim »…) sur l'activité déclarée et la raison sociale, puis **enrichissement SIRENE** (NAF, effectif, création) via `recherche-entreprises.api.gouv.fr` | Public & gratuit |
| **Actify** | Scraping | Filtre mots-clés sur titre / activité / description (volume faible : les ETT passent rarement par Actify) | Public |

Sur 90 jours (sept. 2026) : ~70 annonces BODACC dont ~40 confirmées NAF 78.xx.

## Quick Start

### 1. Installation
```bash
pip install -r requirements.txt   # Python ≥ 3.10
```

### 2. Token Pappers
Créer un compte API sur [pappers.fr/api](https://www.pappers.fr/api) et ajouter le token dans `config.yaml` ou en variable d'environnement `PAPPERS_API_TOKEN`.

### 3. Lancer les scrapers
```bash
cd scrapers
# SIRENE (gratuit, sans token) — agences d'intérim IDF, dirigeant 55+, siège 75/92 en priorité
python sirene_hunter.py                 # → data/pappers_results.json + data/cibles_interim_idf.csv
python sirene_hunter.py --dept 75 92    # Paris + Hauts-de-Seine seulement
python sirene_hunter.py --all           # sans filtre d'âge
# BODACC — procédures collectives intérim, 90 derniers jours, enrichies SIRENE
python bodacc_monitor.py --days 90
# Actify — offres de reprise filtrées sur les mots-clés cible
python actify_scraper.py --max-pages 10
# Pappers — chasse succession + distressed sur NAF 78.xx
python pappers_hunter.py --token VOTRE_TOKEN --canal both
```

### 4. Dashboard
Ouvrir `dashboard.html` dans un navigateur. Les données se chargent automatiquement depuis `data/` ou manuellement via upload JSON. Les lignes BODACC affichent le NAF SIRENE (✓ cible) et la tranche d'effectif.

### 5. Automatisation GitHub Actions
Ajouter le secret `PAPPERS_API_TOKEN` dans Settings > Secrets. Le workflow tourne Lun-Ven à 08h15 (Paris) et peut être lancé à la main.

## Canaux d'acquisition

- **Canal 1 — Succession** : Dirigeant 55+, agence créée avant 2010, CA 1M–30M (le CA d'une ETT = heures facturées, marge brute ≈ 15-25 %), pas de procédure collective
- **Canal 2 — Distressed** : Agence en RJ/LJ, potentiellement à reprendre via plan de cession (attention aux garanties financières et à la caution obligatoire des ETT)

## Codes NAF ciblés (intérim)

| NAF | Libellé |
|-----|---------|
| 78.20Z | Activités des agences de travail temporaire |
| 78.10Z | Activités des agences de placement de main-d'œuvre |
| 78.30Z | Autre mise à disposition de ressources humaines |

L'ancienne cible luxe / artisanat a été retirée le 2026-09-17 (voir historique git).
