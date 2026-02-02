#!/usr/bin/env python3
"""
Actify Scraper v2 — Découverte via sitemap WordPress + API REST
Actify.fr charge ses listings via AJAX, donc on ne peut pas scraper la page index.
Stratégie :
  1. WordPress sitemap XML → collecter toutes les URLs d’annonces
  2. WordPress REST API /wp-json/wp/v2/posts → fallback
  3. Crawl des pages par secteur → dernier recours
  4. Scraper chaque page de détail (server-rendered, fonctionne avec requests)
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin

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


def is_listing_url(url):
    """Vérifie qu’une URL est bien une annonce (pas une page statique)."""
    path = url.replace(BASE_URL, "").rstrip("/")
    if LISTING_PREFIX.rstrip("/") not in path:
        return False
    slug = path.replace(LISTING_PREFIX, "").rstrip("/")
    if not slug or slug in EXCLUDED_SLUGS:
        return False
    if "/page/" in url or "/secteurs/" in url:
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
    print(f"  → {len(urls)} URLs d’annonces via sitemap")
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
    """Découvrir les URLs via l’API REST WordPress."""
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
    print(f"  → {len(urls)} URLs d’annonces via API REST")
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
    print(f"  → {len(urls)} URLs d’annonces via secteurs")
    return urls


# ═══════════════════════════════════════
# SCRAPING DES PAGES DE DÉTAIL
# ═══════════════════════════════════════
def parse_detail_page(html, url):
    """Parser une page de détail Actify (server-rendered)."""
    soup = BeautifulSoup(html, "html.parser")
    data = {"url": url}
    text = soup.get_text(" ", strip=True)

    # Titre
    h1 = soup.find("h1")
    if h1:
        data["titre"] = h1.get_text(strip=True)

    # Date limite de dépôt des offres (CRITIQUE)
    dldo_patterns = [
        r"Date limite de d[ée]p[oô]t des offres?\s*:\s*(.+?)(?:\s*·|\s*Ajouter|$)",
        r"Jusqu.au\s+(\d{1,2}[\s/]+\w+[\s/]+\d{4}(?:\s+[àa]\s+\d{2}h?\d{0,2})?)",
        r"au plus tard[,\s]+le\s+(\d{1,2}\s+\w+\s+\d{4}\s+[àa]\s+\d{2}h?\d{0,2})",
        r"date limite.*?(\d{1,2}[\s/]+\w+[\s/]+\d{4}(?:\s+[àa]\s+\d{2}h?\d{0,2})?)",
    ]
    for pattern in dldo_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["date_limite_offres"] = match.group(1).strip()[:80]
            break

    # Secteur/catégorie
    breadcrumb = soup.select(".breadcrumb a, .entry-category a, a[rel='tag']")
    for bc in breadcrumb:
        t = bc.get_text(strip=True)
        if t and t.lower() not in ("accueil", "home", "reprendre une entreprise"):
            data.setdefault("secteur", t)

    # Chiffre d’affaires
    ca_patterns = [
        r"[Cc]hiffre d.affaires?\s*(?::\s*)?(.+?)(?:\n|Résultat|Effectif|Date|Localisation)",
        r"CA\s*:\s*(.+?)(?:\n|€|Résultat)",
    ]
    for pattern in ca_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["chiffre_affaires"] = match.group(1).strip()[:150]
            break

    # CA historique détaillé
    ca_details = re.findall(
        r"(?:Au\s+)?(?:\d{2}/\d{2}/)?(​?\d{4})\s*(?:\(\d+\s*m[io]s?\))?\s*(?::)?\s*(?:CA\s*:?\s*)?([\d][\d\s,.]*?)\s*[kK]?€",
        text
    )
    if ca_details:
        data["ca_historique"] = {}
        for year, montant in ca_details:
            clean = montant.replace(" ", "").replace(",", ".")
            data["ca_historique"][year.replace("\u200b", "")] = clean

    # Nombre de salariés
    sal_patterns = [
        r"[Ee]ffectif[s]?\s*(?:au\s+[\d/]+\s*)?:\s*(?:env\.?\s*)?(\d+)\s*salari",
        r"(\d+)\s*salari[ée]s?",
        r"[Ee]ffectif\s*(?:global)?\s*:\s*(?:env\.?\s*)?(\d+)",
    ]
    for pattern in sal_patterns:
        match = re.search(pattern, text)
        if match:
            data["salaries"] = match.group(1)
            break

    # Localisation
    loc_patterns = [
        r"(?:Localisation|Siège social|Lieu)\s*:\s*(.+?)(?:\n|Effectif|Date|CA|Activit)",
    ]
    for pattern in loc_patterns:
        match = re.search(pattern, text)
        if match:
            data["lieu"] = match.group(1).strip()[:100]
            break

    # Code postal + Ville (pattern adresse)
    addr_match = re.search(r"([A-ZÉÈÊËÀÂÙÛÎÏÔÖ][A-ZÉÈÊËÀÂÙÛÎÏÔÖa-zéèêëàâùûîïôö\s-]+?)\s*,?\s*(\d{5})", text)
    if addr_match:
        data.setdefault("lieu", addr_match.group(1).strip()[:80])
        data["code_postal"] = addr_match.group(2)
    else:
        cp_match = re.search(r"(\d{5})", text)
        if cp_match:
            data["code_postal"] = cp_match.group(1)

    # Description
    desc_el = soup.find(string=re.compile(r"Description"))
    if desc_el:
        parent = desc_el.find_parent(["div", "section"])
        if parent:
            data["description"] = parent.get_text("\n", strip=True)[:2000]

    # Activité
    act_match = re.search(r"Activit[ée]\s*(?:de l.entreprise|concernée)?\s*:\s*(.+?)(?:\n|Lieu|Local|Effectif)", text)
    if act_match:
        data["activite"] = act_match.group(1).strip()[:200]

    # Surface
    surf_match = re.search(r"(\d[\d\s]*)\s*m[²2]", text)
    if surf_match:
        data["surface_m2"] = surf_match.group(1).replace(" ", "")

    # Loyer
    loyer_match = re.search(r"[Ll]oyer\s*:?\s*([\d\s,.]+\s*€[^.]*)", text)
    if loyer_match:
        data["loyer"] = loyer_match.group(1).strip()[:80]

    # Contact
    contact = {}
    contact_patterns = [
        (r"(SELAS?U?\s+[\w\s&'-]+?)(?:,|\n|Administrateur|Mandataire|demeurant)", "etude"),
        (r"(SELARLU?\s+[\w\s&'-]+?)(?:,|\n|Administrateur|Mandataire|demeurant)", "etude"),
        (r"(SCP\s+[\w\s&'-]+?)(?:,|\n|Administrateur|Mandataire)", "etude"),
        (r"(CBF\s+ASSOCIES|FHB|AJ\s+[\w]+)", "etude"),
        (r"Maître\s+([\w\s]+?)(?:,|\n|demeurant)", "nom"),
    ]
    contact_section = soup.find(string=re.compile(r"contacter|renseignement|Contact", re.IGNORECASE))
    search_text = text
    if contact_section:
        parent = contact_section.find_parent(["div", "section", "aside"])
        if parent:
            search_text = parent.get_text("\n", strip=True)
            email_link = parent.find("a", href=re.compile(r"mailto:"))
            if email_link:
                contact["email"] = email_link["href"].replace("mailto:", "")
    for pattern, key in contact_patterns:
        match = re.search(pattern, search_text)
        if match:
            contact[key] = match.group(1).strip()[:100]
            break
    if "email" not in contact:
        email_match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", search_text)
        if email_match:
            contact["email"] = email_match.group(0)
    tel_match = re.search(r"(?:Tél\.?\s*:?\s*)?(\d{2}[\s.]?\d{2}[\s.]?\d{2}[\s.]?\d{2}[\s.]?\d{2})", search_text)
    if tel_match:
        contact["telephone"] = tel_match.group(1)
    if contact:
        data["contact"] = contact

    return data


# ═══════════════════════════════════════
# ORCHESTRATION PRINCIPALE
# ═══════════════════════════════════════
def scrape_actify(max_pages=10, max_details=200):
    """Scraper principal Actify."""
    print("=" * 60)
    print("🔴 ACTIFY SCRAPER v2 — Entreprises en procédure collective")
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
    parser = argparse.ArgumentParser(description="Scraper Actify v2")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Pages max pour API REST (défaut: 10)")
    parser.add_argument("--max-details", type=int, default=200,
                        help="Max annonces à scraper en détail (défaut: 200)")
    args = parser.parse_args()
    listings = scrape_actify(max_pages=args.max_pages, max_details=args.max_details)
    save_results(listings)
