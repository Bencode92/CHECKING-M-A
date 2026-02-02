#!/usr/bin/env python3
"""
Actify Scraper v3 — Section-based parsing + normalized fields
=============================================================
Changes from v2:
  - is_listing_url() fixed with urlparse (no more index page in JSON)
  - parse_detail_page() rewritten: \n separator + section-based extraction
  - Normalized numeric fields: ca_min_eur/ca_max_eur, sal_min/sal_max, departement
  - DLDO parsed to ISO date (date_limite_offres_iso)
  - Contact data STRIPPED for RGPD (repo is public)
  - Backward-compatible field names for dashboard.html

Discovery strategies unchanged:
  1. WordPress sitemap XML
  2. WordPress REST API /wp-json/wp/v2/posts
  3. Crawl sector pages
  4. Scrape each detail page (server-rendered)
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin, urlparse

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

BASE_URL = "https://actify.fr"
LISTING_PREFIX = "/entreprises-liquidation-judiciaire/"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

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


# ═══════════════════════════════════════
# HELPERS : normalisation & extraction
# ═══════════════════════════════════════
def _norm(s: str) -> str:
    """Normalize string for comparison: lowercase, strip accents, collapse whitespace."""
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("'", "'")
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
        # Exact match or "label :" / "label:"
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


def _parse_range(s: str) -> tuple[int | None, int | None]:
    """Parse a French range string ('De 0 à 250 000', 'Entre 0 et 5', 'Plus de 100 000')."""
    if not s:
        return (None, None)
    ns = _norm(s)
    if "non" in ns and "renseigne" in ns:
        return (None, None)

    nums = re.findall(r"\d[\d\s\u00a0]*", s)
    nums = [int(re.sub(r"\D", "", x)) for x in nums if re.sub(r"\D", "", x)]
    if not nums:
        return (None, None)

    if "plus" in ns:
        return (nums[0], None)
    if "moins" in ns:
        return (None, nums[0])
    if len(nums) >= 2:
        return (min(nums), max(nums))
    return (nums[0], nums[0])


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


# ═══════════════════════════════════════
# URL VALIDATION (fixed)
# ═══════════════════════════════════════
def fetch(url, retries=3, delay=2):
    """Fetch URL avec retries."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            print(f"  ⚠ Erreur {url}: {e} (tentative {attempt+1}/{retries})")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


def is_listing_url(url: str) -> bool:
    """Vérifie qu'une URL est bien une annonce individuelle (pas index, pas secteur)."""
    path = urlparse(url).path.strip("/")
    prefix = LISTING_PREFIX.strip("/")

    if not path.startswith(prefix):
        return False

    slug = path[len(prefix):].strip("/")
    if not slug or slug in EXCLUDED_SLUGS:
        return False

    if "/page/" in path or "/secteurs/" in path:
        return False

    return True


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
    print("\nℹ️  Stratégie 1 : Recherche sitemaps WordPress...")
    for sitemap_url in sitemap_candidates:
        resp = fetch(sitemap_url)
        if not resp or resp.status_code != 200:
            continue
        print(f"  ✅ Sitemap trouvé : {sitemap_url}")
        try:
            content = re.sub(r'\sxmlns[^"]*"[^"]*"', '', resp.text)
            root = ET.fromstring(content)
            locs = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
            if not locs:
                locs = root.findall(".//loc")
            for loc in locs:
                u = loc.text.strip() if loc.text else ""
                if u.endswith(".xml"):
                    print(f"  └─ Sous-sitemap : {u}")
                    sub_urls = _parse_sub_sitemap(u)
                    urls.update(sub_urls)
                elif is_listing_url(u):
                    urls.add(u)
        except ET.ParseError:
            found = re.findall(r"<loc>(https?://actify\.fr/[^<]+)</loc>", resp.text)
            for u in found:
                if is_listing_url(u):
                    urls.add(u)
    print(f"  → {len(urls)} URLs d'annonces via sitemap")
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
    print("\nℹ️  Stratégie 2 : WordPress REST API...")
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
                print(f"  ✅ API {endpoint.split('/')[-1]} page {page}: {len(posts)} posts")
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
    print(f"  → {len(urls)} URLs d'annonces via API REST")
    return urls


# ═══════════════════════════════════════
# STRATÉGIE 3 : Crawl des pages secteurs
# ═══════════════════════════════════════
def discover_via_sectors():
    """Découvrir les URLs en crawlant les pages par secteur."""
    urls = set()
    print("\nℹ️  Stratégie 3 : Crawl des pages secteurs...")
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
            print(f"  └─ {sector_name}: {sector_count} annonces")
        time.sleep(0.5)
    print(f"  → {len(urls)} URLs d'annonces via secteurs")
    return urls


# ═══════════════════════════════════════
# SCRAPING DES PAGES DE DÉTAIL (v3)
# ═══════════════════════════════════════
def parse_detail_page(html, url):
    """Parser une page de détail Actify avec extraction par sections.

    Utilise get_text("\\n") pour préserver la structure en blocs,
    puis extrait chaque champ entre ses libellés de section.
    """
    soup = BeautifulSoup(html, "html.parser")
    data = {"url": url}

    # ── Titre ──
    h1 = soup.find("h1")
    if h1:
        data["titre"] = h1.get_text(strip=True)

    # ── Secteur (tags WordPress) ──
    tags = []
    for a in soup.select("a[rel='tag']"):
        t = a.get_text(strip=True)
        if t and t not in tags:
            tags.append(t)
    if tags:
        data["secteur"] = tags[0]
        data["tags"] = tags

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

    # ── Adresse (bloc) ──
    addr_block = _block_after(lines, "Adresse")
    if addr_block:
        data["adresse"] = addr_block
        # Code postal
        m = re.search(r"\b(\d{5})\b", addr_block)
        if m:
            cp = m.group(1)
            data["code_postal"] = cp
            data["departement"] = cp[:2]
        # Ville: texte avant le CP sur la même ligne
        for aline in addr_block.splitlines():
            m2 = re.search(
                r"([A-Za-zÀ-ÖØ-öø-ÿ\u0100-\u024F' -]+)\s*[,/]?\s*\d{5}",
                aline
            )
            if m2:
                data["ville"] = m2.group(1).strip().rstrip(",/ ")
                break
        # Backward compat: 'lieu' pour le dashboard
        data["lieu"] = data.get("ville", addr_block.splitlines()[0])

    # ── Description (bloc) ──
    desc_block = _block_after(lines, "Description")
    if desc_block:
        data["description"] = desc_block
        data["description_resume"] = desc_block.splitlines()[0][:200]

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
    # On garde juste l'URL vers Actify pour accéder aux coordonnées.

    # Nettoyage final
    return {k: v for k, v in data.items() if v is not None}


# ═══════════════════════════════════════
# ORCHESTRATION PRINCIPALE
# ═══════════════════════════════════════
def scrape_actify(max_pages=10, max_details=200):
    """Scraper principal Actify."""
    print("=" * 60)
    print("🔴 ACTIFY SCRAPER v3 — Section-based parsing")
    print("   Découverte via sitemap + API REST + secteurs")
    print("=" * 60)

    all_urls = set()

    # Stratégie 1 : Sitemap
    sitemap_urls = discover_via_sitemap()
    all_urls.update(sitemap_urls)

    # Stratégie 2 : WordPress REST API
    api_urls = discover_via_wp_api(max_pages=max_pages)
    all_urls.update(api_urls)

    # Stratégie 3 : Crawl des secteurs (si < 5 résultats)
    if len(all_urls) < 5:
        sector_urls = discover_via_sectors()
        all_urls.update(sector_urls)

    if not all_urls:
        print("\n❌ Aucune URL d'annonce découverte.")
        print("   Actify.fr a peut-être changé de structure.")
        return []

    print(f"\n{'='*60}")
    print(f"📊 Total URLs uniques découvertes : {len(all_urls)}")
    print(f"{'='*60}")

    urls_to_scrape = sorted(all_urls)[:max_details]
    if len(all_urls) > max_details:
        print(f"  ⚠ Limité à {max_details} annonces (sur {len(all_urls)})")

    # Scraper les pages de détail
    detailed_listings = []
    for i, url in enumerate(urls_to_scrape, 1):
        slug = url.rstrip("/").split("/")[-1]
        print(f"\n🔍 [{i}/{len(urls_to_scrape)}] {slug[:60]}...")
        time.sleep(0.8)
        resp = fetch(url)
        if not resp:
            detailed_listings.append({"url": url, "titre": slug})
            continue
        detail = parse_detail_page(resp.text, url)
        if detail.get("titre"):
            detailed_listings.append(detail)
            if "date_limite_offres" in detail:
                print(f"  ⏰ DLDO: {detail['date_limite_offres']}")
            if "salaries" in detail:
                print(f"  👥 Effectif: {detail['salaries']}")
            if "departement" in detail:
                print(f"  📍 Dept: {detail['departement']}")
        else:
            print(f"  ⚠ Page sans titre — skip")

    return detailed_listings


def save_results(listings):
    """Sauvegarder en JSON."""
    os.makedirs(DATA_DIR, exist_ok=True)
    output = {
        "source": "actify.fr",
        "scraped_at": datetime.now().isoformat(),
        "count": len(listings),
        "listings": listings,
    }
    filepath = os.path.join(DATA_DIR, "actify_listings.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Sauvegardé: {filepath} ({len(listings)} annonces)")
    return filepath


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scraper Actify v3")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Pages max pour API REST (défaut: 10)")
    parser.add_argument("--max-details", type=int, default=200,
                        help="Max annonces à scraper en détail (défaut: 200)")
    args = parser.parse_args()
    listings = scrape_actify(max_pages=args.max_pages, max_details=args.max_details)
    save_results(listings)
