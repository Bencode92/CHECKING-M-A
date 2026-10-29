#!/usr/bin/env python3
"""Serveur local de l'outil Cibles : sert le dossier ET écrit le suivi dans data/annotations.json.

La page envoie son suivi en POST /api/save à chaque modification — aucun fichier à brancher.
Écoute uniquement en local (127.0.0.1 et ::1). Une copie horodatée est gardée dans data/.backups/ (une par heure, 200 max).
"""
import json
import os
import shutil
import socket
import sys
import threading
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ANN = ROOT / "data" / "annotations.json"
BACKUPS = ROOT / "data" / ".backups"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8765


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT), **kw)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/api/ping":
            return self._json(200, {"ok": True, "file": ANN.name})
        return super().do_GET()

    def do_POST(self):
        if self.path != "/api/save":
            return self._json(404, {"error": "inconnu"})
        # uniquement depuis la page locale (pas un autre site ouvert dans le navigateur)
        origin = self.headers.get("Origin", "")
        if origin and not origin.startswith(("http://localhost:", "http://127.0.0.1:")):
            return self._json(403, {"error": "origine refusée"})
        try:
            data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            assert isinstance(data, dict)
        except Exception:
            return self._json(400, {"error": "JSON invalide"})
        old = {}
        if ANN.exists():
            try:
                old = json.loads(ANN.read_text() or "{}")
            except Exception:
                pass
        # garde-fou : jamais remplacer un suivi rempli par un suivi vide
        if old and not data:
            return self._json(409, {"error": "refus d'écraser le suivi par un suivi vide"})
        if old:
            BACKUPS.mkdir(exist_ok=True)
            bk = BACKUPS / f"annotations-{datetime.now():%Y%m%d-%H}.json"
            if not bk.exists():
                shutil.copy2(ANN, bk)
                for f in sorted(BACKUPS.glob("annotations-*.json"))[:-200]:
                    f.unlink()
        tmp = ANN.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
        os.replace(tmp, ANN)
        return self._json(200, {"ok": True, "n": len(data)})

    def log_message(self, *a):
        pass


class Server6(ThreadingHTTPServer):
    address_family = socket.AF_INET6


if __name__ == "__main__":
    # « localhost » peut viser 127.0.0.1 OU ::1 selon le navigateur : on écoute sur les deux,
    # sinon un autre serveur (python -m http.server) peut prendre l'autre adresse et capter la page sans rien sauvegarder
    servers = [ThreadingHTTPServer(("127.0.0.1", PORT), Handler)]
    try:
        servers.append(Server6(("::1", PORT), Handler))
    except OSError as e:
        sys.exit(f"Port {PORT} déjà pris en IPv6 ({e}) — arrête l'autre serveur : lsof -i :{PORT}")
    print(f"Cibles → http://localhost:{PORT}/cibles.html  (suivi écrit dans {ANN})")
    threading.Thread(target=servers[1].serve_forever, daemon=True).start()
    servers[0].serve_forever()
