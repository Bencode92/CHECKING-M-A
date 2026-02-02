# 🔍 CHECKING M&A — Acquisition Pipeline

Pipeline automatisé de détection d'entreprises françaises à reprendre (luxe / artisanat / patrimoine).

## 3 Sources de données

| Source | Type | Méthode |
|--------|------|---------|
| **Pappers** | Entreprises saines (succession) + distressed | API REST |
| **Actify** | Offres de reprise à la barre (RJ/LJ) | Scraping |
| **BODACC** | Ouvertures de procédures collectives | API publique |

## Structure

```
├── dashboard.html              # Dashboard 3 onglets (ouvrir dans le navigateur)
├── config.yaml                 # Configuration (codes NAF, filtres, seuils)
├── scrapers/
│   ├── actify_scraper.py       # Scraper Actify (entreprises en liquidation)
│   ├── bodacc_monitor.py       # Moniteur BODACC (procédures collectives)
│   └── pappers_hunter.py       # Extracteur Pappers API
├── data/                       # Données JSON générées par les scrapers
└── .github/workflows/
    └── daily_scan.yml          # Automation quotidienne (Lun-Ven 8h15)
```

## Quick Start

### 1. Dashboard (zéro setup)

Ouvrir `dashboard.html` dans votre navigateur :
- **Onglet Pappers** : entrer votre token API → Rechercher
- **Onglet BODACC** : cliquer "Fetch API Live" (appel direct, pas de code)
- **Onglet Actify** : charger le fichier `actify_listings.json` généré par le scraper

### 2. Scrapers Python

```bash
pip install -r requirements.txt

# BODACC — procédures collectives des 30 derniers jours
python scrapers/bodacc_monitor.py --days 30

# Actify — scraper les offres de reprise
python scrapers/actify_scraper.py --max-pages 10

# Pappers — extraction ciblée (nécessite token)
python scrapers/pappers_hunter.py --token VOTRE_TOKEN --canal both
```

### 3. GitHub Actions (automatisation)

1. Ajouter le secret `PAPPERS_API_TOKEN` dans Settings > Secrets
2. Le workflow tourne automatiquement Lun-Ven à 8h15 (UTC+1)
3. Les résultats sont committés dans `data/`

## Codes NAF cibles

Luxe / artisanat / patrimoine : biscuiterie, chocolaterie, cristallerie, verrerie d'art, céramique, coutellerie, robinetterie, joaillerie, maroquinerie, savonnerie, ébénisterie, vinification, distillerie...

Voir `config.yaml` pour la liste complète.
