#!/bin/bash
# Déploie cibles.html + dashboard + données sur Cloudflare Pages (projet privé « checking-ma »,
# protégé par le mot de passe du secret SITE_PASSWORD via functions/_middleware.js).
#
# Première fois :
#   wrangler pages project create checking-ma --production-branch main
#   wrangler pages secret put SITE_PASSWORD --project-name checking-ma
# Ensuite, à chaque mise à jour de la liste / de l'enrichissement IA :
#   ./deploy.sh
set -e
cd "$(dirname "$0")"
rm -rf .site && mkdir -p .site/data .site/functions
cp cibles.html dashboard.html .site/
cp functions/_middleware.js .site/functions/
cp data/*.json .site/data/ 2>/dev/null || true
printf '<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0;url=cibles.html">' > .site/index.html
wrangler pages deploy .site --project-name checking-ma --commit-dirty=true
