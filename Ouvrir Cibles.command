#!/bin/bash
# Double-clic : sert le dossier en local et ouvre la page Cibles (les fichiers data/ ne se lisent pas en file://)
cd "$(dirname "$0")"
lsof -i :8765 >/dev/null 2>&1 || (python3 -m http.server 8765 >/dev/null 2>&1 &)
sleep 1
open "http://localhost:8765/cibles.html"
