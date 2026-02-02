# 🔍 CHECKING M&A — Acquisition Pipeline

Dashboard et scrapers automatisés pour identifier des PME françaises à reprendre.

## Architecture

```
├── dashboard.html              # Dashboard 3 onglets (Pappers / Actify / BODACC)
├── config.yaml                 # Codes NAF, filtres, paramètres
├── scrapers/
│   ├── pappers_hunter.py       # API Pappers — succession & distressed
│   ├── actify_scraper.py       # Scraper Actify — reprises à la barre
│   └── bodacc_monitor.py       # API BODACC — procédures collectives
├── data/                       # JSON générés par les scrapers
│   ├── pappers_results.json
│   ├── actify_listings.json
│   └── bodacc_alerts.json
└── .github/workflows/
    └── daily_scan.yml          # Cron Lun-Ven 08h15 Paris
```

## 3 Sources de données

| Source | Type | Données | Accès |
|--------|------|---------|-------|
| **Pappers** | API | Entreprises par NAF, âge dirigeant, CA, procédure | Token API (gratuit 100 crédits) |
| **Actify** | Scraping | Offres de reprise judiciaire (CNAJMJ) | Public |
| **BODACC** | API | Ouvertures RJ/LJ, plans de cession | Public & gratuit |

## Quick Start

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Token Pappers
Créer un compte API sur [pappers.fr/api](https://www.pappers.fr/api) et ajouter le token dans `config.yaml` ou en variable d'environnement `PAPPERS_API_TOKEN`.

### 3. Lancer les scrapers
```bash
# BODACC — procédures collectives 30 derniers jours
python scrapers/bodacc_monitor.py --days 30

# Actify — offres de reprise
python scrapers/actify_scraper.py --max-pages 10

# Pappers — chasse succession + distressed
python scrapers/pappers_hunter.py --token VOTRE_TOKEN --canal both
```

### 4. Dashboard
Ouvrir `dashboard.html` dans un navigateur. Les données se chargent automatiquement depuis `data/` ou manuellement via upload JSON.

### 5. Automatisation GitHub Actions
Ajouter le secret `PAPPERS_API_TOKEN` dans Settings > Secrets. Le workflow tourne Lun-Ven à 08h15 (Paris).

## Canaux d'acquisition

- **Canal 1 — Succession** : Dirigeant 55+, entreprise créée avant 2000, CA 300K-5M, pas de procédure collective
- **Canal 2 — Distressed** : Entreprise en RJ/LJ, potentiellement à reprendre via plan de cession

## Codes NAF ciblés (luxe / artisanat)

Biscuiterie, chocolaterie, cristallerie, coutellerie, robinetterie, joaillerie, maroquinerie, savonnerie, ébénisterie, céramique, verrerie d'art, etc.

Voir `config.yaml` pour la liste complète.
