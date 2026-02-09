#!/usr/bin/env python3
"""
Actify Scraper v5.1 — Playwright + AJAX discovery + print-posts fallback
=========================================================================
Changes from v5.0:
  - FIX: _parse_dldo now handles 2-digit years (JJ/MM/AA → 20XX)
  - NEW: fetch_detail_with_fallback tries ?print-posts=print if standard parse fails
  - NEW: "Date de fin de commercialisation" used as DLDO fallback
  - All v5.0 strategies preserved (Playwright, AJAX discovery, sitemap, sectors)

Discovery strategies (v5.1):
  0a. **PRIMARY** Playwright headless crawl of paginated listing pages
  0b. **FAST ALT** Auto-discover AJAX/REST endpoint from page JS
  0c. Static listing crawl (fallback)
  1. WordPress sitemap XML
  2. WordPress REST API /wp-json/wp/v2/posts + custom post types
  3. Crawl sector pages (always runs)
  4. Scrape each detail page (server-rendered, with print-posts fallback)
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

# ALL index pages to crawl (v5: expanded)
LISTING_INDEX_URLS = [
    f"{BASE_URL}/entreprises-liquidation-judiciaire/",
    f"{BASE_URL}/fonds-de-commerce/",
    f"{BASE_URL}/vente-actifs/",
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
    "reprendre-une-entreprise",
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
    "Date de fin de commercialisation",
]

MONTHS_FR = {
    "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
    "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
    "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
    "novembre": "11", "décembre": "12", "decembre": "12",
}

# Champs attendus pour le quality check
EXPECTED_FIELDS = {"titre", "secteur", "chiffre_affaires", "salaries", "adresse", "description"}

# Accepted URL path prefixes for individual listings
ACCEPTED_PREFIXES = [
    "entreprises-liquidation-judiciaire",
    "fonds-de-commerce",
    "vente-actifs",
]


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
    nline = _norm(line)
    for label in STOP_LABELS:
        nlabel = _norm(label)
        if nline == nlabel or nline.startswith(nlabel):
            return True
    return False


def _find_line_index(lines: list[str], label: str) -> int | None:
    nlabel = _norm(label)
    for i, ln in enumerate(lines):
        nln = _norm(ln)
        if nln == nlabel or nln.startswith(nlabel + " :") or nln.startswith(nlabel + ":"):
            return i
    return None


def _next_value(lines: list[str], label: str) -> str | None:
    i = _find_line_index(lines, label)
    if i is None:
        return None
    for j in range(i + 1, min(i + 6, len(lines))):
        if lines[j] and not _is_stop_label(lines[j]):
            return lines[j]
    return None


def _block_after(lines: list[str], label: str) -> str | None:
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
    if not s:
        return (None, None)
    ns = _norm(s)
    if "non" in ns and "renseigne" in ns:
        return (None, None)

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


def _parse_fr_slash_date(s: str) -> date | None:
    """Parse a French date string JJ/MM/AAAA or JJ/MM/AA → date object.

    v5.1: supports 2-digit years (19/01/26 → 2026-01-19).
    """
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", s)
    if not m:
        return None
    d_str, mo_str, y_str = m.group(1), m.group(2), m.group(3)
    if len(y_str) == 2:
        y_str = "20" + y_str
    try:
        return datetime.strptime(f"{d_str}/{mo_str}/{y_str}", "%d/%m/%Y").date()
    except ValueError:
        return None


def _parse_dldo(lines: list[str]) -> tuple[str | None, str | None]:
    """Extract DLDO from lines. Returns (raw_string, iso_date_string).

    v5.1 improvements:
      - Supports 2-digit years (JJ/MM/AA)
      - Also checks "Date de fin de commercialisation" as fallback
    """
    full = "\n".join(lines)

    # Pattern 1: "Date limite de dépôt des offres : DD/MM/YYYY" or "DD/MM/YY"
    m = re.search(
        r"Date limite de d[ée]p[oô]t des offres\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        full, re.I
    )
    if m:
        raw = m.group(0).strip()
        d = _parse_fr_slash_date(m.group(1))
        if d:
            return (raw, d.isoformat())
        return (raw, None)

    # Pattern 2: "Jusqu'au DD/MM/YYYY" or "DD/MM/YY"
    m = re.search(r"Jusqu.au\s+(\d{1,2}/\d{1,2}/\d{2,4})", full, re.I)
    if m:
        raw = m.group(0).strip()
        d = _parse_fr_slash_date(m.group(1))
        if d:
            return (raw, d.isoformat())
        return (raw, None)

    # Pattern 3: "Jusqu'au 5 mai 2023 à 12h00" or "Jusqu'au 30 mars 2026"
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

    # Pattern 4 (v5.1): "Date de fin de commercialisation : DD/MM/YYYY" or "DD/MM/YY"
    m = re.search(
        r"Date de fin de commercialisation\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        full, re.I
    )
    if m:
        raw = m.group(0).strip()
        d = _parse_fr_slash_date(m.group(1))
        if d:
            return (raw, d.isoformat())
        return (raw, None)

    # Pattern 5: any standalone date near DLDO keywords
    for keyword in ["date limite", "dldo", "fin de commercialisation", "offres"]:
        idx = full.lower().find(keyword)
        if idx >= 0:
            snippet = full[idx:idx+200]
            d = _parse_fr_slash_date(snippet)
            if d:
                return (snippet[:80].strip(), d.isoformat())

    return (None, None)


def _dept_from_cp(cp: str) -> str | None:
    if not cp or len(cp) < 2:
        return None
    if cp.startswith(("97", "98")) and len(cp) >= 3:
        return cp[:3]
    if cp.startswith("20"):
        try:
            return "2A" if int(cp) < 20200 else "2B"
        except ValueError:
            return "20"
    return cp[:2]


def _split_address(addr_block: str) -> dict:
    out = {}
    lines = [ln.strip(" ,/") for ln in addr_block.splitlines() if ln.strip()]
    if not lines:
        return out

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

    for ln in reversed(lines):
        m_city = re.search(r"(.+?)\s*[,/\s]\s*\d{5}\b", ln)
        if m_city:
            ville = m_city.group(1).strip(" ,/").strip()
            if ville and len(ville) > 1:
                out["ville"] = ville
            break

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
    out_lines = []
    for ln in desc.splitlines():
        n = _norm(ln)
        if n.startswith("contact") or "contact :" in n or "pour tout renseignement" in n:
            continue
        if _EMAIL_RE.search(ln) or _PHONE_RE.search(ln):
            continue
        out_lines.append(ln)
    cleaned = "\n".join(out_lines).strip()
    cleaned = _EMAIL_RE.sub("[email-redacted]", cleaned)
    cleaned = _PHONE_RE.sub("[tel-redacted]", cleaned)
    return cleaned


# ═══════════════════════════════════════
# HTTP : fetch avec Session + gestion 429
# ═══════════════════════════════════════
def fetch(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=30)
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
# URL VALIDATION (v5: expanded prefixes)
# ═══════════════════════════════════════
def is_listing_url(url: str) -> bool:
    path = urlparse(url).path.strip("/")

    matched_prefix = None
    for prefix in ACCEPTED_PREFIXES:
        if path.startswith(prefix):
            matched_prefix = prefix
            break

    if not matched_prefix:
        return False

    slug = path[len(matched_prefix):].strip("/")
    if not slug or slug in EXCLUDED_SLUGS:
        return False

    if "/page/" in path or "/secteurs/" in path:
        return False

    if "/" in slug:
        return False

    return True


# ═══════════════════════════════════════
# STRATÉGIE 0a : Playwright headless crawl (NEW v5)
# ═══════════════════════════════════════
def _check_playwright_available() -> bool:
    try:
        from playwright.sync_api import sync_playwright
        return True
    except ImportError:
        return False


def discover_via_playwright(max_pages=20) -> set[str]:
    """Crawl listing pages using Playwright to render JavaScript.

    The Actify listing pages load card content via JS/AJAX.
    requests+BeautifulSoup only sees empty placeholder divs.
    Playwright renders the full page including JS-loaded content.
    """
    if not _check_playwright_available():
        log.warning("Stratégie 0a: Playwright non installé — skip")
        log.warning("  Install: pip install playwright && playwright install chromium")
        return set()

    from playwright.sync_api import sync_playwright

    urls = set()
    log.info("Stratégie 0a: Playwright headless crawl des pages listing...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="fr-FR",
        )
        page = context.new_page()

        for index_url in LISTING_INDEX_URLS:
            section_name = index_url.rstrip("/").split("/")[-1]
            page_num = 1
            consecutive_empty = 0

            while page_num <= max_pages:
                if page_num == 1:
                    page_url = index_url
                else:
                    page_url = f"{index_url}page/{page_num}/"

                try:
                    log.info(f"  Playwright → {section_name} page {page_num}")
                    page.goto(page_url, wait_until="networkidle", timeout=30000)

                    # Wait for cards to load (AJAX content)
                    page.wait_for_timeout(3000)

                    # Extract all links from the rendered page
                    links = page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(el => el.href)"
                    )

                    found_on_page = 0
                    for href in links:
                        full_url = urljoin(BASE_URL, href)
                        if is_listing_url(full_url) and full_url not in urls:
                            urls.add(full_url)
                            found_on_page += 1

                    log.info(f"    └─ {found_on_page} nouvelles URLs")

                    if found_on_page == 0:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            log.info(f"    └─ 2 pages vides consécutives, arrêt pagination {section_name}")
                            break
                    else:
                        consecutive_empty = 0

                    # Also try to extract card links from common selectors
                    _extract_card_previews(page, urls)

                except Exception as e:
                    log.warning(f"    └─ Erreur Playwright page {page_num}: {e}")
                    break

                page_num += 1
                time.sleep(1.5)

        browser.close()

    log.info(f"  → {len(urls)} URLs d'annonces via Playwright")
    return urls


def _extract_card_previews(page, urls_set):
    """Try to extract additional card links from rendered listing page."""
    try:
        cards = page.query_selector_all("article, .card, .listing-card, .annonce-card, [class*='card'], [class*='listing']")
        for card in cards:
            try:
                link = card.query_selector("a[href]")
                if link:
                    href = link.get_attribute("href")
                    if href:
                        full_url = urljoin(BASE_URL, href)
                        urls_set.add(full_url)
            except Exception:
                pass
    except Exception:
        pass


# ═══════════════════════════════════════
# STRATÉGIE 0b : Auto-discover AJAX endpoint (NEW v5)
# ═══════════════════════════════════════
def discover_via_ajax_endpoint() -> set[str]:
    """Try to discover and call the AJAX/REST endpoint that loads cards."""
    urls = set()
    log.info("Stratégie 0b: Recherche endpoint AJAX/REST interne...")

    # 1. Try WP REST API with various custom post types
    custom_types = [
        "annonces", "annonce", "listings", "listing",
        "offres", "offre", "entreprises", "entreprise",
        "cessions", "cession", "actifs", "actif",
        "ads", "properties", "biens", "lots",
    ]

    for cpt in custom_types:
        api_url = f"{BASE_URL}/wp-json/wp/v2/{cpt}?per_page=100&status=publish"
        resp = fetch(api_url, retries=1, delay=1)
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if data and isinstance(data, list):
                    total = resp.headers.get("X-WP-Total", len(data))
                    log.info(f"  ✅ Custom post type '{cpt}' trouvé: {total} items!")
                    for item in data:
                        link = item.get("link", "")
                        if link and is_listing_url(link):
                            urls.add(link)
                    # Paginate if more pages
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    for page_num in range(2, min(total_pages + 1, 50)):
                        page_url = f"{BASE_URL}/wp-json/wp/v2/{cpt}?per_page=100&page={page_num}&status=publish"
                        resp2 = fetch(page_url, retries=1, delay=1)
                        if resp2 and resp2.status_code == 200:
                            for item in resp2.json():
                                link = item.get("link", "")
                                if link and is_listing_url(link):
                                    urls.add(link)
                        time.sleep(0.5)
                    break
            except (json.JSONDecodeError, ValueError):
                pass
        time.sleep(0.3)

    # 2. Try admin-ajax.php with common filter actions
    ajax_actions = [
        "actify_filter", "actify_load_more", "actify_get_listings",
        "load_more_posts", "get_posts", "filter_posts",
        "load_listings", "get_listings", "filter_listings",
        "load_more", "ajax_filter", "get_annonces",
        "wpajax_filter", "archive_filter",
    ]

    for action in ajax_actions:
        ajax_url = f"{BASE_URL}/wp-admin/admin-ajax.php"
        try:
            resp = SESSION.post(ajax_url, data={
                "action": action,
                "page": 1,
                "per_page": 50,
                "paged": 1,
                "posts_per_page": 50,
            }, timeout=10)
            if resp.status_code == 200 and resp.text and resp.text not in ("0", "-1", ""):
                log.info(f"  ✅ AJAX action '{action}' répond: {len(resp.text)} chars")
                if "<a " in resp.text:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    for a_tag in soup.find_all("a", href=True):
                        href = urljoin(BASE_URL, a_tag["href"])
                        if is_listing_url(href):
                            urls.add(href)
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        for key in ["html", "content", "data", "posts", "listings"]:
                            if key in data and isinstance(data[key], str) and "<a " in data[key]:
                                soup = BeautifulSoup(data[key], "html.parser")
                                for a_tag in soup.find_all("a", href=True):
                                    href = urljoin(BASE_URL, a_tag["href"])
                                    if is_listing_url(href):
                                        urls.add(href)
                except (json.JSONDecodeError, ValueError):
                    pass
        except Exception:
            pass
        time.sleep(0.3)

    # 3. Try to find endpoints from theme JS files
    resp = fetch(f"{BASE_URL}/entreprises-liquidation-judiciaire/")
    if resp:
        soup = BeautifulSoup(resp.text, "html.parser")
        for script in soup.find_all("script", src=True):
            src = script["src"]
            if "actify" in src.lower() or "theme" in src.lower() or "custom" in src.lower():
                js_resp = fetch(urljoin(BASE_URL, src), retries=1)
                if js_resp:
                    js_text = js_resp.text
                    actions_found = re.findall(r"['\"]action['\"]:\s*['\"](\w+)['\"]", js_text)
                    api_paths = re.findall(r"/wp-json/([^'\"\\]+)", js_text)
                    for af in actions_found:
                        if af not in ajax_actions:
                            log.info(f"  JS trouvé action AJAX: '{af}'")
                            try:
                                r = SESSION.post(
                                    f"{BASE_URL}/wp-admin/admin-ajax.php",
                                    data={"action": af, "page": 1, "paged": 1},
                                    timeout=10,
                                )
                                if r.status_code == 200 and len(r.text) > 10 and r.text not in ("0", "-1"):
                                    log.info(f"    └─ Action '{af}' active: {len(r.text)} chars")
                                    if "<a " in r.text:
                                        s = BeautifulSoup(r.text, "html.parser")
                                        for a_tag in s.find_all("a", href=True):
                                            href = urljoin(BASE_URL, a_tag["href"])
                                            if is_listing_url(href):
                                                urls.add(href)
                            except Exception:
                                pass
                    for ap in api_paths:
                        log.info(f"  JS trouvé API path: /wp-json/{ap}")

    if urls:
        log.info(f"  → {len(urls)} URLs d'annonces via AJAX/REST discovery")
    else:
        log.info("  → Aucun endpoint AJAX/REST découvert (attendu si custom theme)")
    return urls


# ═══════════════════════════════════════
# STRATÉGIE 0c : requests-based listing crawl (legacy, fallback)
# ═══════════════════════════════════════
def discover_via_listing_pages(max_pages=30):
    """Fallback: crawl listing pages with requests (may miss JS-rendered content)."""
    urls = set()
    log.info("Stratégie 0c: Crawl statique des pages listing (fallback)...")

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
                break

            if resp.status_code == 404:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found_on_page = 0

            for a_tag in soup.find_all("a", href=True):
                href = urljoin(BASE_URL, a_tag["href"])
                if is_listing_url(href) and href not in urls:
                    urls.add(href)
                    found_on_page += 1

            log.info(f"  └─ {section_name} page {page_num}: {found_on_page} nouvelles URLs")

            if found_on_page == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
            else:
                consecutive_empty = 0

            page_num += 1
            time.sleep(1.0)

    log.info(f"  → {len(urls)} URLs d'annonces via listing pages (statique)")
    return urls


# ═══════════════════════════════════════
# STRATÉGIE 1 : WordPress Sitemap XML
# ═══════════════════════════════════════
def discover_via_sitemap():
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
# SCRAPING DES PAGES DE DÉTAIL (v5.1: print-posts fallback)
# ═══════════════════════════════════════
def parse_detail_page(html, url):
    """Parser une page de détail Actify avec extraction par sections."""
    soup = BeautifulSoup(html, "html.parser")
    data = {"url": url}

    h1 = soup.find("h1")
    if h1:
        data["titre"] = h1.get_text(strip=True)

    tags = []
    for a in soup.select("a[rel='tag']"):
        t = a.get_text(strip=True)
        if t and t not in tags:
            tags.append(t)

    if tags:
        data["tags"] = tags
        secteur = next((t for t in tags if t not in GENERIC_TAGS), None)
        if secteur:
            data["secteur"] = secteur
        categorie = next((t for t in tags if t in GENERIC_TAGS), None)
        if categorie:
            data["categorie"] = categorie

    main = soup.find("main") or soup
    lines = _lines_from_soup(main)

    dldo_raw, dldo_iso = _parse_dldo(lines)
    if dldo_raw:
        data["date_limite_offres"] = dldo_raw
    if dldo_iso:
        data["date_limite_offres_iso"] = dldo_iso

    # v5.1: Also extract "Date de fin de commercialisation" separately
    full_text = "\n".join(lines)
    m_comm = re.search(
        r"Date de fin de commercialisation\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        full_text, re.I
    )
    if m_comm:
        d_comm = _parse_fr_slash_date(m_comm.group(1))
        if d_comm:
            data["date_fin_commercialisation_iso"] = d_comm.isoformat()
            # Use as DLDO fallback if no DLDO found
            if not dldo_iso:
                data["date_limite_offres_iso"] = d_comm.isoformat()
                data["date_limite_offres"] = m_comm.group(0).strip()

    if re.search(r"\bNon en activité\b", full_text, re.I):
        data["statut"] = "Non en activité"
    elif re.search(r"\bEn activité\b", full_text, re.I):
        data["statut"] = "En activité"

    ca_band = _next_value(lines, "Chiffre d'affaires")
    if ca_band:
        data["chiffre_affaires"] = ca_band
        ca_min, ca_max = _parse_range(ca_band)
        data["ca_min_eur"] = ca_min
        data["ca_max_eur"] = ca_max

    sal_band = _next_value(lines, "Nombre de salariés")
    if sal_band:
        data["salaries"] = sal_band
        sal_min, sal_max = _parse_range(sal_band)
        data["sal_min"] = sal_min
        data["sal_max"] = sal_max

    anc = _next_value(lines, "Ancienneté de l'entreprise")
    if anc:
        data["anciennete"] = anc

    deficit = _next_value(lines, "Déficit reportable")
    if deficit:
        data["deficit_reportable"] = deficit

    naf = _next_value(lines, "Code ape / NAF")
    if naf:
        data["code_naf"] = naf

    addr_block = _block_after(lines, "Adresse")
    if addr_block:
        data["adresse"] = addr_block
        addr_parts = _split_address(addr_block)
        data.update(addr_parts)
        data["lieu"] = data.get("ville") or addr_block.splitlines()[0].strip()

    desc_block = _block_after(lines, "Description")
    if desc_block:
        desc_block = _sanitize_description(desc_block)
        data["description"] = desc_block
        data["description_resume"] = (desc_block.splitlines()[0][:200]
                                      if desc_block else "")

        blob = desc_block + "\n" + (addr_block or "")
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*m[²2]\b", blob)
        if m:
            data["surface_m2"] = m.group(1).replace(",", ".")

        m = re.search(r"[Ll]oyer\s*:?\s*([\d\s,.]+\s*€[^.\n]*)", desc_block)
        if m:
            data["loyer"] = m.group(0).strip()[:80]

        if not data.get("secteur"):
            first = desc_block.splitlines()[0] if desc_block else ""
            if len(first) > 10:
                data["activite"] = first[:200]

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

    return {k: v for k, v in data.items() if v is not None}


def fetch_detail_with_fallback(url: str) -> dict:
    """Fetch and parse a detail page, with ?print-posts=print fallback.

    v5.1: If the standard page parse has low quality (missing key fields),
    try the WordPress print view which often has cleaner, more structured content.
    """
    # Try standard page first
    resp = fetch(url)
    if resp:
        data = parse_detail_page(resp.text, url)
        quality = _parse_quality(data)

        # If quality is acceptable (≥50%), return immediately
        if quality >= 0.5 and data.get("titre"):
            return data

        # Otherwise, try print-posts fallback
        sep = "&" if "?" in url else "?"
        print_url = url.rstrip("/") + f"/{sep}print-posts=print"
        log.info(f"  → Fallback print-posts pour {url.split('/')[-2]}")
        resp_print = fetch(print_url, retries=1, delay=1)
        if resp_print and resp_print.status_code == 200:
            data_print = parse_detail_page(resp_print.text, url)
            quality_print = _parse_quality(data_print)

            # Use print version if it's better
            if quality_print > quality and data_print.get("titre"):
                log.info(f"    └─ Print version meilleure: {quality_print:.0%} vs {quality:.0%}")
                return data_print

        # Fall back to standard version even if low quality
        if data.get("titre"):
            return data

    return {"url": url, "titre": url.rstrip("/").split("/")[-1]}


# ═══════════════════════════════════════
# QUALITY CHECK
# ═══════════════════════════════════════
def _parse_quality(data: dict) -> float:
    return len(EXPECTED_FIELDS & data.keys()) / len(EXPECTED_FIELDS)


def _is_expired(listing: dict) -> bool:
    dldo_iso = listing.get("date_limite_offres_iso")
    if not dldo_iso:
        return False
    try:
        dldo_date = date.fromisoformat(dldo_iso)
        return dldo_date < date.today()
    except (ValueError, TypeError):
        return False


# ═══════════════════════════════════════
# ORCHESTRATION PRINCIPALE (v5.1)
# ═══════════════════════════════════════
def scrape_actify(max_pages=10, max_details=500, use_playwright=True):
    log.info("=" * 60)
    log.info("ACTIFY SCRAPER v5.1 — Playwright + print-posts fallback")
    log.info("=" * 60)

    all_urls = set()
    discovery_stats = {}

    # Stratégie 0a: Playwright (si disponible)
    if use_playwright and _check_playwright_available():
        pw_urls = discover_via_playwright(max_pages=20)
        discovery_stats["playwright"] = len(pw_urls)
        all_urls.update(pw_urls)
    elif use_playwright:
        log.info("Playwright non disponible, skip stratégie 0a")
        discovery_stats["playwright"] = 0

    # Stratégie 0b: AJAX/REST endpoint discovery
    ajax_urls = discover_via_ajax_endpoint()
    discovery_stats["ajax_discovery"] = len(ajax_urls)
    all_urls.update(ajax_urls)

    # Stratégie 0c: Static listing crawl (always run as fallback)
    listing_urls = discover_via_listing_pages(max_pages=30)
    discovery_stats["static_listing"] = len(listing_urls)
    all_urls.update(listing_urls)

    # Stratégie 1: Sitemap
    sitemap_urls = discover_via_sitemap()
    discovery_stats["sitemap"] = len(sitemap_urls)
    all_urls.update(sitemap_urls)

    # Stratégie 2: WordPress REST API
    api_urls = discover_via_wp_api(max_pages=max_pages)
    discovery_stats["wp_rest_api"] = len(api_urls)
    all_urls.update(api_urls)

    # Stratégie 3: Crawl secteurs (TOUJOURS)
    sector_urls = discover_via_sectors()
    discovery_stats["sectors"] = len(sector_urls)
    all_urls.update(sector_urls)

    # Discovery summary
    log.info(f"{'='*60}")
    log.info(f"DISCOVERY SUMMARY:")
    for strategy, count in discovery_stats.items():
        log.info(f"  {strategy}: {count} URLs")
    log.info(f"  TOTAL UNIQUE: {len(all_urls)}")
    log.info(f"{'='*60}")

    if not all_urls:
        log.error("Aucune URL d'annonce découverte.")
        return [], 0

    urls_to_scrape = sorted(all_urls)[:max_details]
    if len(all_urls) > max_details:
        log.warning(f"Limité à {max_details} annonces (sur {len(all_urls)})")

    # Scraper les pages de détail (v5.1: with print-posts fallback)
    all_listings = []
    for i, url in enumerate(urls_to_scrape, 1):
        slug = url.rstrip("/").split("/")[-1]
        log.info(f"[{i}/{len(urls_to_scrape)}] {slug[:60]}")
        time.sleep(0.8)

        detail = fetch_detail_with_fallback(url)
        all_listings.append(detail)
        if "date_limite_offres_iso" in detail:
            log.info(f"  DLDO: {detail['date_limite_offres_iso']}")

    # Quality check
    if all_listings:
        qualities = [_parse_quality(d) for d in all_listings]
        avg_quality = sum(qualities) / len(qualities)
        log.info(f"Parse quality: {avg_quality:.0%} (moyenne sur {len(all_listings)} annonces)")
        if avg_quality < 0.4:
            log.error("ALERTE: qualité < 40% — Actify a probablement changé de template !")

    # Filtrer les annonces expirées
    active_listings = [l for l in all_listings if not _is_expired(l)]
    expired_count = len(all_listings) - len(active_listings)

    log.info(f"{'='*60}")
    log.info(f"Résultats: {len(active_listings)} actives, {expired_count} expirées (exclues)")
    log.info(f"{'='*60}")

    # Trier par DLDO croissante
    def _sort_key(listing):
        dldo = listing.get("date_limite_offres_iso")
        if dldo:
            try:
                return (0, date.fromisoformat(dldo))
            except (ValueError, TypeError):
                pass
        return (1, date.max)

    active_listings.sort(key=_sort_key)

    return active_listings, expired_count


def save_results(listings, expired_count=0):
    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "source": "actify.fr",
        "scraped_at": datetime.now().isoformat(),
        "version": "v5.1",
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
    parser = argparse.ArgumentParser(description="Scraper Actify v5.1")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Pages max pour API REST (défaut: 10)")
    parser.add_argument("--max-details", type=int, default=500,
                        help="Max annonces à scraper en détail (défaut: 500)")
    parser.add_argument("--no-playwright", action="store_true",
                        help="Désactiver Playwright (fallback strategies uniquement)")
    args = parser.parse_args()

    result = scrape_actify(
        max_pages=args.max_pages,
        max_details=args.max_details,
        use_playwright=not args.no_playwright,
    )
    if isinstance(result, tuple):
        listings, expired_count = result
    else:
        listings, expired_count = result, 0
    save_results(listings, expired_count)
