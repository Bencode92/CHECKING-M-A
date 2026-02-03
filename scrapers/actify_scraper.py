#!/usr/bin/env python3
"""
Actify Scraper v4.1 — Listing page crawl + Expired filter + RGPD + quality
===========================================================================
Changes from v4:
  - CRITICAL: NEW Strategy 0 — direct crawl of paginated listing pages
    (https://actify.fr/entreprises-liquidation-judiciaire/page/N/)
    This is the most reliable discovery source, same as what users see.
  - Strategy 3 (sectors) now ALWAYS runs (was conditional on < 5 results)
  - Also discovers via /fonds-de-commerce/ index pages

Changes from v3 (inherited in v4):
  - CRITICAL: expired listings (DLDO < today) excluded from output
  - CRITICAL: departement fixed for DOM-TOM (971-976) and Corse (2A/2B)
  - RGPD: emails/phones stripped from description field
  - Secteur: generic tags ("Entreprises à reprendre") skipped
  - _parse_range(): handles k€ / M€ multipliers
  - fetch(): uses requests.Session (keep-alive) + handles HTTP 429
  - Address: structured parsing (rue/ville/CP/dept)
  - Quality metric: alerts if parse success rate drops (template change)
  - JSON output: metadata count_active / count_expired

Backward-compatible with dashboard.html field names.

Discovery strategies (v4.1):
  0. **PRIMARY** Direct crawl of paginated listing index pages
  1. WordPress sitemap XML
  2. WordPress REST API /wp-json/wp/v2/posts
  3. Crawl sector pages (always runs)
  4. Scrape each detail page (server-rendered)
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
import logging
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, date
from urllib.parse import urljoin, urlparse

# ═══════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("actify")

# ═══════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

BASE_URL = "https://actify.fr"
LISTING_PREFIX = "/entreprises-liquidation-judiciaire/"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Pages index à crawler (pagination) — source principale d'annonces
LISTING_INDEX_URLS = [
    f"{BASE_URL}/entreprises-liquidation-judiciaire/",
    f"{BASE_URL}/fonds-de-commerce/",
]

# HTTP Session (keep-alive, cookie persistence)
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Pages connues qui ne sont PAS des annonces
EXCLUDED_SLUGS = {
    "entreprises-liquidation-judiciaire",
    "a-propos",
    "actify-reprendre",
    "contact-actify-cnajmj",
    "actifs-entreprises-liquidation-cnajmj",
    "vente-actifs",
    "fonds-de-commerce",
    "inscription-actify",
    "login-actify-marketplace-reglementee",
    "login-etude-actify-marketplace-reglementee",
    "comment-acheter-actif-liquidation-judiciaire",
    "mentions-legales",
    "politique-de-confidentialite",
}

# Tags WordPress génériques (pas un vrai secteur)
GENERIC_TAGS = {
    "Entreprises à reprendre",
    "Fonds de commerce à reprendre",
    "Actifs à reprendre",
    "Reprendre une entreprise",
}

# ═══════════════════════════════════════
# SECTION LABELS (pour le parsing par blocs)
# ═══════════════════════════════════════
STOP_LABELS = [
    "Chiffre d'affaires",
    "Ancienneté de l'entreprise",
    "Nombre de salariés",
    "Code ape / NAF",
    "Déficit reportable",
    "Adresse",
    "Description",
    "Pour tout renseignement",
    "Contact",
    "Manifester mon intérêt",
    "Ajouter aux favoris",
    "Inscrivez-vous",
]

MONTHS_FR = {
    "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "décembre": "12", "decembre": "12",
}

# Champs attendus pour le quality check
EXPECTED_FIELDS = {"titre", "secteur", "chiffre_affaires", "salaries", "adresse", "description"}


# ═══════════════════════════════════════
# HELPERS : normalisation & extraction
# ═══════════════════════════════════════
def _norm(s: str) -> str:
    """Normalize string for comparison: lowercase, strip accents, collapse whitespace."""
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\u2019", "'")
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s)
                if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s)


def _lines_from_soup(soup) -> list[str]:
    """Extract clean lines from BeautifulSoup, preserving \\n structure."""
    raw = soup.get_text("\n", strip=True)
    lines = []
    for ln in raw.splitlines():
        ln = ln.lstrip("#").strip()
        ln = re.sub(r"\s+", " ", ln).strip()
        if ln:
            lines.append(ln)
    return lines


def _is_stop_label(line: str) -> bool:
    """Check if a line is a section boundary (stop label)."""
    nline = _norm(line)
    for label in STOP_LABELS:
        nlabel = _norm(label)
        if nline == nlabel or nline.startswith(nlabel):
            return True
    return False


def _find_line_index(lines: list[str], label: str) -> int | None:
    """Find index of the line matching a section label."""
    nlabel = _norm(label)
    for i, ln in enumerate(lines):
        nln = _norm(ln)
        if nln == nlabel or nln.startswith(nlabel + " :") or nln.startswith(nlabel + ":"):
            return i
    return None


def _next_value(lines: list[str], label: str) -> str | None:
    """Get first non-empty, non-label line after the section label."""
    i = _find_line_index(lines, label)
    if i is None:
        return None
    for j in range(i + 1, min(i + 6, len(lines))):
        if lines[j] and not _is_stop_label(lines[j]):
            return lines[j]
    return None


def _block_after(lines: list[str], label: str) -> str | None:
    """Get all lines between label and next stop label, as a block."""
    i = _find_line_index(lines, label)
    if i is None:
        return None
    out = []
    for j in range(i + 1, len(lines)):
        if _is_stop_label(lines[j]):
            break
        out.append(lines[j])
    block = "\n".join(out).strip()
    return block or None


# ═══════════════════════════════════════
# HELPERS : parsing spécialisé
# ═══════════════════════════════════════
def _parse_range(s: str) -> tuple[int | None, int | None]:
    """Parse a French range string with support for k€ / M€ multipliers.

    Examples:
        'De 0 à 250 000'      → (0, 250000)
        'Entre 0 et 5'        → (0, 5)
        'Plus de 100 000'     → (100000, None)
        '1,2 M€'              → (1200000, 1200000)
        '250 k€'              → (250000, 250000)
    """
    if not s:
        return (None, None)
    ns = _norm(s)
    if "non" in ns and "renseigne" in ns:
        return (None, None)

    # Detect multiplier
    multiplier = 1
    if re.search(r"m\s*€|millions?", ns):
        multiplier = 1_000_000
    elif re.search(r"k\s*€|milliers?", ns):
        multiplier = 1_000

    nums = re.findall(r"\d[\d\s\u00a0,\.]*", s)
    parsed = []
    for x in nums:
        clean = re.sub(r"[\s\u00a0]", "", x).replace(",", ".")
        if clean and clean.replace(".", "", 1).isdigit():
            parsed.append(int(float(clean) * multiplier))
    if not parsed:
        return (None, None)

    if "plus" in ns:
        return (parsed[0], None)
    if "moins" in ns:
        return (None, parsed[0])
    if len(parsed) >= 2:
        return (min(parsed), max(parsed))
    return (parsed[0], parsed[0])


def _parse_dldo(lines: list[str]) -> tuple[str | None, str | None]:
    """Extract DLDO. Returns (raw_string, iso_date_string)."""
    full = "\n".join(lines)

    # Pattern 1: "Date limite de dépôt des offres : 05/05/2023"
    m = re.search(
        r"Date limite de d[ée]p[oô]t des offres\s*:\s*(\d{2}/\d{2}/\d{4})",
        full, re.I
    )
    if m:
        raw = m.group(0).strip()
        try:
            d = datetime.strptime(m.group(1), "%d/%m/%Y").date()
            return (raw, d.isoformat())
        except ValueError:
            return (raw, None)

    # Pattern 2: "Jusqu'au 05/05/2023"
    m = re.search(r"Jusqu.au\s+(\d{1,2}/\d{2}/\d{4})", full, re.I)
    if m:
        raw = m.group(0).strip()
        try:
            d = datetime.strptime(m.group(1), "%d/%m/%Y").date()
            return (raw, d.isoformat())
        except ValueError:
            return (raw, None)

    # Pattern 3: "Jusqu'au 5 mai 2023 à 12h00"
    m = re.search(r"Jusqu.au\s+(\d{1,2})\s+(\w+)\s+(\d{4})", full, re.I)
    if m:
        raw = m.group(0).strip()[:80]
        day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
        month_num = MONTHS_FR.get(month_name)
        if month_num:
            try:
                d = datetime.strptime(f"{day}/{month_num}/{year}", "%d/%m/%Y").date()
                return (raw, d.isoformat())
            except ValueError:
                return (raw, None)
        return (raw, None)

    return (None, None)


def _dept_from_cp(cp: str) -> str | None:
    """Extract département from code postal (handles DOM-TOM + Corse)."""
    if not cp or len(cp) < 2:
        return None
    # DOM/COM: 971, 972, 973, 974, 976, 98xxx
    if cp.startswith(("97", "98")) and len(cp) >= 3:
        return cp[:3]
    # Corse: 20000-20199 → 2A, 20200-20620 → 2B
    if cp.startswith("20"):
        try:
            return "2A" if int(cp) < 20200 else "2B"
        except ValueError:
            return "20"
    return cp[:2]


def _split_address(addr_block: str) -> dict:
    """Parse address block into structured fields.

    Returns dict with keys: adresse_detail, ville, code_postal, departement.
    """
    out = {}
    lines = [ln.strip(" ,/") for ln in addr_block.splitlines() if ln.strip()]
    if not lines:
        return out

    # Code postal: cherche un 5-digit dans tout le bloc
    m_cp = None
    for ln in reversed(lines):
        m_cp = re.search(r"\b(\d{5})\b", ln)
        if m_cp:
            break
    if not m_cp:
        m_cp = re.search(r"\b(\d{5})\b", addr_block)

    if m_cp:
        cp = m_cp.group(1)
        out["code_postal"] = cp
        out["departement"] = _dept_from_cp(cp)

    # Ville: texte avant le CP sur la ligne contenant le CP
    for ln in reversed(lines):
        m_city = re.search(r"(.+?)\s*[,/\s]\s*\d{5}\b", ln)
        if m_city:
            ville = m_city.group(1).strip(" ,/").strip()
            if ville and len(ville) > 1:
                out["ville"] = ville
            break

    # Adresse détail: lignes qui ne contiennent pas le CP
    detail_lines = [ln for ln in lines if not re.search(r"\b\d{5}\b", ln)]
    if detail_lines:
        out["adresse_detail"] = ", ".join(detail_lines)

    return out


# ═══════════════════════════════════════
# RGPD : nettoyage des données personnelles
# ═══════════════════════════════════════
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+", re.I)
_PHONE_RE = re.compile(r"\b(?:0|\+33)\s*[1-9](?:[\s.\-]*\d{2}){4}\b")


def _sanitize_description(desc: str) -> str:
    """Strip PII (emails, phones, contact lines) from description."""
    out_lines = []
    for ln in desc.splitlines():
        n = _norm(ln)
        # Skip entire contact lines
        if n.startswith("contact") or "contact :" in n or "pour tout renseignement" in n:
            continue
        if _EMAIL_RE.search(ln) or _PHONE_RE.search(ln):
            continue
        out_lines.append(ln)
    cleaned = "\n".join(out_lines).strip()
    # Safety net: redact any remaining PII
    cleaned = _EMAIL_RE.sub("[email-redacted]", cleaned)
    cleaned = _PHONE_RE.sub("[tel-redacted]", cleaned)
    return cleaned


# ═══════════════════════════════════════
# HTTP : fetch avec Session + gestion 429
# ═══════════════════════════════════════
def fetch(url, retries=3, delay=2):
    """Fetch URL avec retries et gestion du rate-limiting (429)."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30)
            # Rate-limit: back off and retry
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                log.warning(f"Rate-limité (429), attente {retry_after}s... {url}")
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            log.warning(f"Erreur {url}: {e} (tentative {attempt+1}/{retries})")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


# ═══════════════════════════════════════
# URL VALIDATION
# ═══════════════════════════════════════
def is_listing_url(url: str) -> bool:
    """Vérifie qu'une URL est bien une annonce individuelle (pas index, pas secteur).

    Accepte les URLs sous:
      - /entreprises-liquidation-judiciaire/<slug>/
      - /fonds-de-commerce/<slug>/
    """
    path = urlparse(url).path.strip("/")

    # Check accepted prefixes
    accepted_prefixes = [
        "entreprises-liquidation-judiciaire",
        "fonds-de-commerce",
    ]
    matched_prefix = None
    for prefix in accepted_prefixes:
        if path.startswith(prefix):
            matched_prefix = prefix
            break

    if not matched_prefix:
        return False

    slug = path[len(matched_prefix):].strip("/")
    if not slug or slug in EXCLUDED_SLUGS:
        return False

    # Exclude pagination and sector pages
    if "/page/" in path or "/secteurs/" in path:
        return False

    # Exclude sub-paths (slug should be a single segment)
    if "/" in slug:
        return False

    return True


# ═══════════════════════════════════════
# STRATÉGIE 0 : Crawl direct des pages listing paginées
# ═══════════════════════════════════════
def discover_via_listing_pages(max_pages=30):
    """Découvrir les URLs en crawlant les pages listing paginées.

    C'est la stratégie la plus fiable : elle voit exactement ce que
    l'utilisateur voit sur actify.fr/entreprises-liquidation-judiciaire/.
    """
    urls = set()
    log.info("Stratégie 0 : Crawl direct des pages listing paginées...")

    for index_url in LISTING_INDEX_URLS:
        section_name = index_url.rstrip("/").split("/")[-1]
        page_num = 1
        consecutive_empty = 0

        while page_num <= max_pages:
            if page_num == 1:
                page_url = index_url
            else:
                page_url = f"{index_url}page/{page_num}/"

            resp = fetch(page_url)
            if not resp:
                log.info(f"  └─ {section_name} page {page_num}: pas de réponse, arrêt")
                break

            # Si la page retourne une 404 ou redirect vers la page 1, arrêter
            if resp.status_code == 404:
                log.info(f"  └─ {section_name} page {page_num}: 404, fin de pagination")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found_on_page = 0

            # Chercher tous les liens <a> qui pointent vers des annonces
            for a_tag in soup.find_all("a", href=True):
                href = urljoin(BASE_URL, a_tag["href"])
                if is_listing_url(href) and href not in urls:
                    urls.add(href)
                    found_on_page += 1

            log.info(f"  └─ {section_name} page {page_num}: {found_on_page} nouvelles URLs")

            if found_on_page == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    log.info(f"  └─ {section_name}: 2 pages vides consécutives, arrêt")
                    break
            else:
                consecutive_empty = 0

            page_num += 1
            time.sleep(1.0)  # Délai plus long pour les pages index

    log.info(f"  → {len(urls)} URLs d'annonces via listing pages")
    return urls


# ═══════════════════════════════════════
# STRATÉGIE 1 : WordPress Sitemap XML
# ═══════════════════════════════════════
def discover_via_sitemap():
    """Découvrir les URLs via les sitemaps WordPress."""
    urls = set()
    sitemap_candidates = [
        f"{BASE_URL}/wp-sitemap.xml",
        f"{BASE_URL}/sitemap_index.xml",
        f"{BASE_URL}/sitemap.xml",
        f"{BASE_URL}/post-sitemap.xml",
        f"{BASE_URL}/wp-sitemap-posts-post-1.xml",
        f"{BASE_URL}/wp-sitemap-posts-page-1.xml",
    ]
    log.info("Stratégie 1 : Recherche sitemaps WordPress...")
    for sitemap_url in sitemap_candidates:
        resp = fetch(sitemap_url)
        if not resp or resp.status_code != 200:
            continue
        log.info(f"  Sitemap trouvé : {sitemap_url}")
        try:
            content = re.sub(r'\sxmlns[^"]*"[^"]*"', '', resp.text)
            root = ET.fromstring(content)
            locs = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
            if not locs:
                locs = root.findall(".//loc")
            for loc in locs:
                u = loc.text.strip() if loc.text else ""
                if u.endswith(".xml"):
                    log.info(f"  └─ Sous-sitemap : {u}")
                    sub_urls = _parse_sub_sitemap(u)
                    urls.update(sub_urls)
                elif is_listing_url(u):
                    urls.add(u)
        except ET.ParseError:
            found = re.findall(r"<loc>(https?://actify\.fr/[^<]+)</loc>", resp.text)
            for u in found:
                if is_listing_url(u):
                    urls.add(u)
    log.info(f"  → {len(urls)} URLs d'annonces via sitemap")
    return urls


def _parse_sub_sitemap(url):
    """Parser un sous-sitemap."""
    urls = set()
    resp = fetch(url)
    if not resp:
        return urls
    try:
        content = re.sub(r'\sxmlns[^"]*"[^"]*"', '', resp.text)
        root = ET.fromstring(content)
        locs = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if not locs:
            locs = root.findall(".//loc")
        for loc in locs:
            u = loc.text.strip() if loc.text else ""
            if is_listing_url(u):
                urls.add(u)
    except ET.ParseError:
        found = re.findall(r"<loc>(https?://actify\.fr/[^<]+)</loc>", resp.text)
        for u in found:
            if is_listing_url(u):
                urls.add(u)
    return urls


# ═══════════════════════════════════════
# STRATÉGIE 2 : WordPress REST API
# ═══════════════════════════════════════
def discover_via_wp_api(max_pages=10):
    """Découvrir les URLs via l'API REST WordPress."""
    urls = set()
    log.info("Stratégie 2 : WordPress REST API...")
    api_endpoints = [
        f"{BASE_URL}/wp-json/wp/v2/posts",
        f"{BASE_URL}/wp-json/wp/v2/pages",
    ]
    for endpoint in api_endpoints:
        page = 1
        while page <= max_pages:
            api_url = f"{endpoint}?per_page=100&page={page}&status=publish"
            resp = fetch(api_url)
            if not resp or resp.status_code != 200:
                break
            try:
                posts = resp.json()
                if not posts:
                    break
                log.info(f"  API {endpoint.split('/')[-1]} page {page}: {len(posts)} posts")
                for post in posts:
                    link = post.get("link", "")
                    if is_listing_url(link):
                        urls.add(link)
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1
                time.sleep(0.5)
            except (json.JSONDecodeError, ValueError):
                break
    log.info(f"  → {len(urls)} URLs d'annonces via API REST")
    return urls


# ═══════════════════════════════════════
# STRATÉGIE 3 : Crawl des pages secteurs
# ═══════════════════════════════════════
def discover_via_sectors():
    """Découvrir les URLs en crawlant les pages par secteur."""
    urls = set()
    log.info("Stratégie 3 : Crawl des pages secteurs...")
    sector_urls = [
        f"{BASE_URL}/secteurs/annonces-secteur-industrie/",
        f"{BASE_URL}/secteurs/annonces-secteur-artisanat/",
        f"{BASE_URL}/secteurs/annonces-secteur-btp/",
        f"{BASE_URL}/secteurs/annonces-secteur-commerce-dalimentation/",
        f"{BASE_URL}/secteurs/annonces-secteur-boulangerie/",
        f"{BASE_URL}/secteurs/annonces-secteur-boucherie/",
        f"{BASE_URL}/secteurs/annonces-secteur-restaurants-cafes/",
        f"{BASE_URL}/secteurs/annonces-secteur-habillement-textile-retail/",
        f"{BASE_URL}/secteurs/annonces-secteur-hotel/",
        f"{BASE_URL}/secteurs/annonce-cession-actif-immobilier/",
        f"{BASE_URL}/secteurs/annonces-secteur-informatique-tech/",
        f"{BASE_URL}/secteurs/annonces-secteur-medical-pharmaceutique/",
        f"{BASE_URL}/secteurs/annonces-secteur-services-professionnels/",
        f"{BASE_URL}/secteurs/annonces-secteur-services-particuliers/",
        f"{BASE_URL}/secteurs/annonces-secteur-startup-tech/",
        f"{BASE_URL}/secteurs/annonces-secteur-transport-logistique/",
        f"{BASE_URL}/secteurs/annonces-secteur-autres/",
        f"{BASE_URL}/secteurs/annonces-secteur-beaute-coiffure/",
        f"{BASE_URL}/secteurs/annonces-secteur-design/",
        f"{BASE_URL}/secteurs/annonces-secteur-bars-discotheques/",
        f"{BASE_URL}/secteurs/annonces-secteur-tabac/",
        f"{BASE_URL}/secteurs/annonces-secteur-agriculture/",
        f"{BASE_URL}/secteurs/annonces-secteur-activites-culturelles/",
        f"{BASE_URL}/secteurs/annonces-secteur-administration-comptabilite-juridique-2/",
        f"{BASE_URL}/secteurs/annonce-cession-actif-alimentation-agro-alimentaire/",
    ]
    for sector_url in sector_urls:
        resp = fetch(sector_url)
        if not resp:
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=True)
        sector_count = 0
        for link in links:
            href = urljoin(BASE_URL, link["href"])
            if is_listing_url(href) and href not in urls:
                urls.add(href)
                sector_count += 1
        if sector_count > 0:
            sector_name = sector_url.split("/")[-2].replace("annonces-secteur-", "").replace("annonce-cession-actif-", "")
            log.info(f"  └─ {sector_name}: {sector_count} annonces")
        time.sleep(0.5)
    log.info(f"  → {len(urls)} URLs d'annonces via secteurs")
    return urls


# ═══════════════════════════════════════
# SCRAPING DES PAGES DE DÉTAIL (v4)
# ═══════════════════════════════════════
def parse_detail_page(html, url):
    """Parser une page de détail Actify avec extraction par sections.

    v4: RGPD sanitize, DOM-TOM dept, generic tag skip, structured address.
    """
    soup = BeautifulSoup(html, "html.parser")
    data = {"url": url}

    # ── Titre ──
    h1 = soup.find("h1")
    if h1:
        data["titre"] = h1.get_text(strip=True)

    # ── Secteur (tags WordPress) — skip generic tags ──
    tags = []
    for a in soup.select("a[rel='tag']"):
        t = a.get_text(strip=True)
        if t and t not in tags:
            tags.append(t)

    if tags:
        data["tags"] = tags
        # Secteur = premier tag non-générique
        secteur = next((t for t in tags if t not in GENERIC_TAGS), None)
        if secteur:
            data["secteur"] = secteur
        # Catégorie = tag générique (pour référence)
        categorie = next((t for t in tags if t in GENERIC_TAGS), None)
        if categorie:
            data["categorie"] = categorie

    # ── Lignes structurées ──
    main = soup.find("main") or soup
    lines = _lines_from_soup(main)

    # ── DLDO ──
    dldo_raw, dldo_iso = _parse_dldo(lines)
    if dldo_raw:
        data["date_limite_offres"] = dldo_raw
    if dldo_iso:
        data["date_limite_offres_iso"] = dldo_iso

    # ── Statut ──
    full_text = "\n".join(lines)
    if re.search(r"\bNon en activité\b", full_text, re.I):
        data["statut"] = "Non en activité"
    elif re.search(r"\bEn activité\b", full_text, re.I):
        data["statut"] = "En activité"

    # ── Chiffre d'affaires (bucket) ──
    ca_band = _next_value(lines, "Chiffre d'affaires")
    if ca_band:
        data["chiffre_affaires"] = ca_band
        ca_min, ca_max = _parse_range(ca_band)
        data["ca_min_eur"] = ca_min
        data["ca_max_eur"] = ca_max

    # ── Nombre de salariés (bucket) ──
    sal_band = _next_value(lines, "Nombre de salariés")
    if sal_band:
        data["salaries"] = sal_band
        sal_min, sal_max = _parse_range(sal_band)
        data["sal_min"] = sal_min
        data["sal_max"] = sal_max

    # ── Ancienneté ──
    anc = _next_value(lines, "Ancienneté de l'entreprise")
    if anc:
        data["anciennete"] = anc

    # ── Déficit reportable ──
    deficit = _next_value(lines, "Déficit reportable")
    if deficit:
        data["deficit_reportable"] = deficit

    # ── Code NAF ──
    naf = _next_value(lines, "Code ape / NAF")
    if naf:
        data["code_naf"] = naf

    # ── Adresse (structured parsing) ──
    addr_block = _block_after(lines, "Adresse")
    if addr_block:
        data["adresse"] = addr_block  # compat dashboard
        addr_parts = _split_address(addr_block)
        data.update(addr_parts)
        # Backward compat: 'lieu' pour le dashboard
        data["lieu"] = data.get("ville") or addr_block.splitlines()[0].strip()

    # ── Description (bloc + RGPD sanitize) ──
    desc_block = _block_after(lines, "Description")
    if desc_block:
        desc_block = _sanitize_description(desc_block)
        data["description"] = desc_block
        data["description_resume"] = (desc_block.splitlines()[0][:200]
                                      if desc_block else "")

        # Surface (dans description ou adresse)
        blob = desc_block + "\n" + (addr_block or "")
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*m[²2]\b", blob)
        if m:
            data["surface_m2"] = m.group(1).replace(",", ".")

        # Loyer (dans description)
        m = re.search(r"[Ll]oyer\s*:?\s*([\d\s,.]+\s*€[^.\n]*)", desc_block)
        if m:
            data["loyer"] = m.group(0).strip()[:80]

        # Activité (première ligne utile si pas de secteur)
        if not data.get("secteur"):
            first = desc_block.splitlines()[0] if desc_block else ""
            if len(first) > 10:
                data["activite"] = first[:200]

    # ── CA historique détaillé (années + montants dans le texte) ──
    ca_details = re.findall(
        r"(?:Au\s+)?(?:\d{2}/\d{2}/)?\u200b?(\d{4})\s*"
        r"(?:\(\d+\s*m[io]s?\))?\s*(?::)?\s*(?:CA\s*:?\s*)?"
        r"([\d][\d\s,.]*?)\s*[kK]?€",
        full_text
    )
    if ca_details:
        data["ca_historique"] = {}
        for year, montant in ca_details:
            clean = montant.replace(" ", "").replace(",", ".")
            data["ca_historique"][year.replace("\u200b", "")] = clean

    # ── RGPD : PAS d'extraction de contacts (repo public) ──

    # Nettoyage final
    return {k: v for k, v in data.items() if v is not None}


# ═══════════════════════════════════════
# QUALITY CHECK
# ═══════════════════════════════════════
def _parse_quality(data: dict) -> float:
    """Score de qualité du parsing (0.0 à 1.0)."""
    return len(EXPECTED_FIELDS & data.keys()) / len(EXPECTED_FIELDS)


def _is_expired(listing: dict) -> bool:
    """Vérifie si une annonce est expirée (DLDO < aujourd'hui)."""
    dldo_iso = listing.get("date_limite_offres_iso")
    if not dldo_iso:
        return False  # Pas de DLDO → on garde (on ne peut pas savoir)
    try:
        dldo_date = date.fromisoformat(dldo_iso)
        return dldo_date < date.today()
    except (ValueError, TypeError):
        return False


# ═══════════════════════════════════════
# ORCHESTRATION PRINCIPALE
# ═══════════════════════════════════════
def scrape_actify(max_pages=10, max_details=500):
    """Scraper principal Actify v4.1.

    - Découvre via listing pages + sitemap + API REST + secteurs
    - Scrape les pages de détail
    - Exclut les annonces expirées (DLDO < aujourd'hui)
    - Trie par DLDO croissante (plus urgentes en premier)
    - Vérifie la qualité du parsing
    """
    log.info("=" * 60)
    log.info("ACTIFY SCRAPER v4.1 — Listing crawl + Expired filter + RGPD")
    log.info("  Découverte via listing pages + sitemap + API REST + secteurs")
    log.info("=" * 60)

    all_urls = set()

    # Stratégie 0 : Crawl direct des pages listing paginées (PRIMAIRE)
    listing_urls = discover_via_listing_pages(max_pages=30)
    all_urls.update(listing_urls)

    # Stratégie 1 : Sitemap
    sitemap_urls = discover_via_sitemap()
    new_from_sitemap = sitemap_urls - all_urls
    if new_from_sitemap:
        log.info(f"  +{len(new_from_sitemap)} nouvelles URLs via sitemap (pas dans listing pages)")
    all_urls.update(sitemap_urls)

    # Stratégie 2 : WordPress REST API
    api_urls = discover_via_wp_api(max_pages=max_pages)
    new_from_api = api_urls - all_urls
    if new_from_api:
        log.info(f"  +{len(new_from_api)} nouvelles URLs via API REST")
    all_urls.update(api_urls)

    # Stratégie 3 : Crawl des secteurs (TOUJOURS, pas conditionnel)
    sector_urls = discover_via_sectors()
    new_from_sectors = sector_urls - all_urls
    if new_from_sectors:
        log.info(f"  +{len(new_from_sectors)} nouvelles URLs via secteurs")
    all_urls.update(sector_urls)

    if not all_urls:
        log.error("Aucune URL d'annonce découverte.")
        log.error("Actify.fr a peut-être changé de structure.")
        return [], 0

    log.info(f"{'='*60}")
    log.info(f"Total URLs uniques découvertes : {len(all_urls)}")
    log.info(f"{'='*60}")

    urls_to_scrape = sorted(all_urls)[:max_details]
    if len(all_urls) > max_details:
        log.warning(f"Limité à {max_details} annonces (sur {len(all_urls)})")

    # ── Scraper les pages de détail ──
    all_listings = []
    for i, url in enumerate(urls_to_scrape, 1):
        slug = url.rstrip("/").split("/")[-1]
        log.info(f"[{i}/{len(urls_to_scrape)}] {slug[:60]}")
        time.sleep(0.8)
        resp = fetch(url)
        if not resp:
            all_listings.append({"url": url, "titre": slug})
            continue
        detail = parse_detail_page(resp.text, url)
        if detail.get("titre"):
            all_listings.append(detail)
            if "date_limite_offres_iso" in detail:
                log.info(f"  DLDO: {detail['date_limite_offres_iso']}")
            if "salaries" in detail:
                log.info(f"  Effectif: {detail['salaries']}")
            if "departement" in detail:
                log.info(f"  Dept: {detail['departement']}")
        else:
            log.warning(f"  Page sans titre — skip")

    # ── Quality check ──
    if all_listings:
        qualities = [_parse_quality(d) for d in all_listings]
        avg_quality = sum(qualities) / len(qualities)
        log.info(f"Parse quality: {avg_quality:.0%} (moyenne sur {len(all_listings)} annonces)")
        if avg_quality < 0.4:
            log.error("ALERTE: qualité < 40%% — Actify a probablement changé de template !")

    # ── Filtrer les annonces expirées ──
    active_listings = [l for l in all_listings if not _is_expired(l)]
    expired_count = len(all_listings) - len(active_listings)

    log.info(f"{'='*60}")
    log.info(f"Résultats: {len(active_listings)} actives, {expired_count} expirées (exclues)")
    log.info(f"{'='*60}")

    if expired_count > 0:
        log.info(f"  {expired_count} annonces avec DLDO passée ont été exclues du JSON.")
    if len(active_listings) == 0 and len(all_listings) > 0:
        log.warning("TOUTES les annonces sont expirées ! Vérifier si Actify a de nouvelles offres.")

    # ── Trier par DLDO croissante (plus urgentes en premier) ──
    def _sort_key(listing):
        dldo = listing.get("date_limite_offres_iso")
        if dldo:
            try:
                return (0, date.fromisoformat(dldo))
            except (ValueError, TypeError):
                pass
        # Sans DLDO → à la fin
        return (1, date.max)

    active_listings.sort(key=_sort_key)

    return active_listings, expired_count


def save_results(listings, expired_count=0):
    """Sauvegarder en JSON avec métadonnées enrichies."""
    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "source": "actify.fr",
        "scraped_at": datetime.now().isoformat(),
        "version": "v4.1",
        "count": len(listings),
        "count_expired_excluded": expired_count,
        "listings": listings,
    }
    filepath = os.path.join(DATA_DIR, "actify_listings.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info(f"Sauvegardé: {filepath} ({len(listings)} annonces actives, {expired_count} expirées exclues)")
    return filepath


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scraper Actify v4.1")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Pages max pour API REST (défaut: 10)")
    parser.add_argument("--max-details", type=int, default=500,
                        help="Max annonces à scraper en détail (défaut: 500)")
    args = parser.parse_args()
    result = scrape_actify(max_pages=args.max_pages, max_details=args.max_details)
    if isinstance(result, tuple):
        listings, expired_count = result
    else:
        listings, expired_count = result, 0
    save_results(listings, expired_count)
