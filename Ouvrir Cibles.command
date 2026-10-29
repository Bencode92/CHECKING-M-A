#!/bin/bash
# Double-clic : lance le serveur local (serve.py : sert le dossier ET sauvegarde le suivi dans data/annotations.json) et ouvre la page
cd "$(dirname "$0")"
# serve.py doit être le SEUL sur le port : tout autre serveur (python -m http.server…) servirait la page sans rien sauvegarder
if ! curl -s -m 2 http://localhost:8765/api/ping | grep -q '"ok"'; then
  lsof -ti :8765 | xargs kill 2>/dev/null; sleep 1
  (python3 serve.py 8765 >/dev/null 2>&1 &)
  sleep 1
fi
if curl -s -m 2 http://localhost:8765/api/ping | grep -q '"ok"' && curl -s -m 2 -g "http://[::1]:8765/api/ping" | grep -q '"ok"'; then
  open "http://localhost:8765/cibles.html"
else
  osascript -e 'display alert "Cibles : le serveur de sauvegarde ne démarre pas" message "Rien ne serait enregistré. Ne travaille pas dans la page et préviens Claude."'
fi
