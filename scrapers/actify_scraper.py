#!/usr/bin/env python3
"""
Actify Scraper — Scrape entreprises en procédure collective depuis actify.fr
Génère actify_listings.json pour le dashboard.
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
from datetime import datetime
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

BASE_URL = "https://actify.fr"
LISTING_URL = f"{BASE_URL}/entreprises-liquidation-judiciaire/"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def fetch_page(url, retries=3, delay=2):
    """Fetch a page with retries and delay."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            print(f"  ⚠ Erreur fetch {url}: {e} (tentative {attempt+1}/{retries})")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


def parse_listing_page(html):
    """Parse une page de listings Actify et retourne les cards."""
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    # Chercher les cards d'annonces — patterns WordPress courants
    cards = soup.select("article, .card, .listing-card, .post-card, .annonce, .offre-card")
    
    # Fallback: chercher les liens vers les pages de détail
    if not cards:
        cards = soup.select("a[href*='entreprises-liquidation-judiciaire/']")
        # Remonter au parent div/article
        seen = set()
        parent_cards = []
        for link in cards:
            parent = link.find_parent(["article", "div"])
            if parent and id(parent) not in seen:
                seen.add(id(parent))
                parent_cards.append(parent)
        if parent_cards:
            cards = parent_cards

    # Fallback 2: chercher les divs qui contiennent "Jusqu'au" (deadline)
    if not cards:
        deadline_els = soup.find_all(string=re.compile(r"Jusqu.au"))
        for el in deadline_els:
            parent = el.find_parent(["article", "div", "li"])
            if parent:
                cards.append(parent)

    for card in cards:
        listing = extract_card_data(card)
        if listing and listing.get("url"):
            listings.append(listing)

    return listings, soup


def extract_card_data(card):
    """Extraire les données d'une card de listing."""
    data = {}

    # Titre et URL
    link = card.find("a", href=re.compile(r"/entreprises-liquidation-judiciaire/.+"))
    if not link:
        link = card.find("a", href=True)
    
    if link:
        data["url"] = urljoin(BASE_URL, link.get("href", ""))
        data["titre"] = link.get_text(strip=True) or ""
    else:
        return None

    # Titre depuis h2/h3/h4
    for tag in ["h2", "h3", "h4", "h5"]:
        title_el = card.find(tag)
        if title_el:
            data["titre"] = title_el.get_text(strip=True)
            break

    # Date limite (Jusqu'au ...)
    deadline_text = card.find(string=re.compile(r"Jusqu.au"))
    if deadline_text:
        match = re.search(r"Jusqu.au\s+(\d{1,2}\s+\w+\s+\d{4})", str(deadline_text))
        if match:
            data["date_limite"] = match.group(1)

    # Lieu
    text = card.get_text(" ", strip=True)
    ville_match = re.search(r"([A-ZÉÈÊËÀÂÙÛÎÏÔÖ][A-ZÉÈÊËÀÂÙÛÎÏÔÖ\s-]{2,})\s*(?:\d{1,2}\s+\w+\s+\d{4}|\()", text)
    if ville_match:
        data["lieu"] = ville_match.group(1).strip()

    # Date publication
    date_match = re.search(r"(\d{1,2}\s+(?:Janvier|Février|Mars|Avril|Mai|Juin|Juillet|Août|Septembre|Octobre|Novembre|Décembre)\s+\d{4})", text, re.IGNORECASE)
    if date_match:
        data["date_publication"] = date_match.group(1)

    # Nombre de vues
    vues_match = re.search(r"(\d+)\s*[Vv]ues?", text)
    if vues_match:
        data["vues"] = int(vues_match.group(1))

    # Salariés
    sal_match = re.search(r"(\d+)\s*salari", text)
    if sal_match:
        data["salaries"] = int(sal_match.group(1))

    # Surface
    surf_match = re.search(r"(\d[\d\s]*)\s*m[²2]", text)
    if surf_match:
        data["surface_m2"] = surf_match.group(1).replace(" ", "")

    # Loyer
    loyer_match = re.search(r"([\d\s]+)€\s*(?:HC|HT)", text)
    if loyer_match:
        data["loyer_mensuel"] = loyer_match.group(1).replace(" ", "")

    return data


def parse_detail_page(html, url):
    """Parser une page de détail Actify."""
    soup = BeautifulSoup(html, "html.parser")
    data = {"url": url}
    text = soup.get_text(" ", strip=True)

    # Titre
    h1 = soup.find("h1")
    if h1:
        data["titre"] = h1.get_text(strip=True)

    # Référence
    ref_el = soup.find(string=re.compile(r"R[ée]f\s*:"))
    if ref_el:
        ref_link = ref_el.find_next("a")
        if ref_link:
            data["reference"] = ref_link.get_text(strip=True)

    # Date de publication
    pub_match = re.search(r"Date de publication\s*:\s*([\d/]+)", text)
    if pub_match:
        data["date_publication"] = pub_match.group(1)

    # Nombre de vues
    vues_match = re.search(r"(\d+)\s*vues?", text, re.IGNORECASE)
    if vues_match:
        data["vues"] = int(vues_match.group(1))

    # Date limite de dépôt des offres (CRITIQUE)
    dldo_patterns = [
        r"Date limite de d[ée]p[oô]t des offres?\s*:\s*(.+?)(?:\n|$|<)",
        r"Jusqu.au\s+(\d{1,2}[\s/]+\w+[\s/]+\d{4}(?:\s+[àa]\s+\d{2}h?\d{0,2})?)",
        r"date limite.*?(\d{1,2}\s+\w+\s+\d{4}(?:\s+[àa]\s+\d{2}h?\d{0,2})?)",
    ]
    for pattern in dldo_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["date_limite_offres"] = match.group(1).strip()
            break

    # Statut (EN COMMERCIALISATION, etc.)
    statut_el = soup.find(string=re.compile(r"EN COMMERCIALISATION|EN COURS|VENDU|CLOTUR"))
    if statut_el:
        data["statut"] = statut_el.strip()

    # Chiffre d'affaires
    ca_match = re.search(r"CHIFFRE D.AFFAIRES.*?(?:De\s+)?([\d\s]+(?:\.\d+)?)\s*€?\s*(?:à|-)?\s*([\d\s]+(?:\.\d+)?)?\s*€?", text, re.IGNORECASE)
    if ca_match:
        data["chiffre_affaires"] = ca_match.group(0).strip()[:100]

    # CA détaillé dans description
    ca_details = re.findall(r"[-–]\s*(\d{4})\s*:\s*([\d\s]+)\s*€", text)
    if ca_details:
        data["ca_historique"] = {year: montant.replace(" ", "") for year, montant in ca_details}

    # Ancienneté
    anc_match = re.search(r"ANCIENNET[ÉE].*?(?:Entre|Plus de|Moins de)\s+([\w\s]+\bans?\b)", text, re.IGNORECASE)
    if anc_match:
        data["anciennete"] = anc_match.group(0).strip()[:80]

    # Nombre de salariés
    sal_patterns = [
        r"NOMBRE DE SALARI[ÉE]S.*?(?:Entre\s+)?([\d]+(?:\s*[-–]\s*\d+)?)",
        r"(\d+)\s*salari[ée]s?",
    ]
    for pattern in sal_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            data["salaries"] = match.group(1) if "NOMBRE" in pattern else match.group(1)
            break

    # Surface
    surf_match = re.search(r"(?:Bail commercial|Surface)\s*:\s*([\d\s]+)\s*m[²2]", text, re.IGNORECASE)
    if not surf_match:
        surf_match = re.search(r"([\d\s]+)\s*m[²2]", text)
    if surf_match:
        data["surface_m2"] = surf_match.group(1).strip()

    # Loyer
    loyer_match = re.search(r"(?:Loyer)\s*:\s*([\d\s]+)\s*€\s*(.*?)(?:\n|$)", text, re.IGNORECASE)
    if not loyer_match:
        loyer_match = re.search(r"([\d\s]+)\s*€\s*(?:HC|HT)\s*(?:et\s*HT)?\s*mensuel", text, re.IGNORECASE)
    if loyer_match:
        data["loyer"] = loyer_match.group(0).strip()[:80]

    # Déficit reportable
    deficit_match = re.search(r"D[ée]ficit reportable\s*:?\s*(Oui|Non)", text, re.IGNORECASE)
    if deficit_match:
        data["deficit_reportable"] = deficit_match.group(1)

    # Adresse
    addr_match = re.search(r"Adresse\s*:?\s*(.+?)(?:\d{5})", text)
    if addr_match:
        data["adresse"] = addr_match.group(0).strip()[:150]
    cp_match = re.search(r"(\d{5})", text)
    if cp_match:
        data["code_postal"] = cp_match.group(1)

    # Description complète
    desc_section = soup.find(string=re.compile(r"Description"))
    if desc_section:
        desc_parent = desc_section.find_parent(["div", "section"])
        if desc_parent:
            data["description"] = desc_parent.get_text("\n", strip=True)[:2000]

    # Activité
    act_match = re.search(r"Activit[ée]\s*:\s*(.+?)(?:\n|Lieu)", text)
    if act_match:
        data["activite"] = act_match.group(1).strip()

    # Lieu
    lieu_match = re.search(r"Lieu\s*:\s*(.+?)(?:\n|\d+\s*salari)", text)
    if lieu_match:
        data["lieu"] = lieu_match.group(1).strip()

    # Contact — administrateur/mandataire judiciaire
    contact = {}
    etude_patterns = [
        r"(SELAS?\s+[\w\s&]+?)(?:\n|$)",
        r"(SCP\s+[\w\s&]+?)(?:\n|$)",
        r"(Maître\s+[\w\s]+?)(?:\n|$)",
    ]
    contact_section = soup.find(string=re.compile(r"contacter|renseignement|contact", re.IGNORECASE))
    if contact_section:
        contact_parent = contact_section.find_parent(["div", "section", "aside"])
        if contact_parent:
            contact_text = contact_parent.get_text("\n", strip=True)
            for pattern in etude_patterns:
                match = re.search(pattern, contact_text)
                if match:
                    contact["etude"] = match.group(1).strip()
                    break
            name_lines = [l.strip() for l in contact_text.split("\n") if l.strip() and len(l.strip()) > 3]
            for line in name_lines:
                if re.match(r"^[A-ZÉÈÊËÀÂ][a-zéèêëàâ]+\s+[A-Za-zéèêëàâ]+$", line):
                    contact["nom"] = line
                    break
            email_link = contact_parent.find("a", href=re.compile(r"mailto:"))
            if email_link:
                contact["email"] = email_link["href"].replace("mailto:", "")

    if contact:
        data["contact"] = contact

    # Secteurs/tags
    tags = soup.select(".tag, .badge, .category-label, .sector-tag")
    if tags:
        data["secteurs"] = [t.get_text(strip=True) for t in tags]

    return data


def get_pagination_urls(soup, base_url):
    """Trouver les URLs de pagination."""
    urls = set()
    
    page_links = soup.select("a[href*='/page/']")
    for link in page_links:
        href = link.get("href", "")
        if href:
            urls.add(urljoin(base_url, href))

    page_links = soup.select("a[href*='page='], a[href*='paged=']")
    for link in page_links:
        href = link.get("href", "")
        if href:
            urls.add(urljoin(base_url, href))

    page_nums = []
    for link in soup.select(".pagination a, .nav-links a, .page-numbers a"):
        text = link.get_text(strip=True)
        if text.isdigit():
            page_nums.append(int(text))

    if page_nums:
        max_page = max(page_nums)
        for i in range(2, max_page + 1):
            urls.add(f"{base_url}page/{i}/")

    return sorted(urls)


def scrape_actify(max_pages=10):
    """Scraper principal Actify."""
    print("=" * 60)
    print("🔴 ACTIFY SCRAPER — Entreprises en procédure collective")
    print("=" * 60)
    
    all_listings = []
    seen_urls = set()

    print(f"\n📄 Fetching page 1: {LISTING_URL}")
    html = fetch_page(LISTING_URL)
    if not html:
        print("❌ Impossible de charger la page principale Actify")
        return []

    listings, soup = parse_listing_page(html)
    for l in listings:
        if l["url"] not in seen_urls:
            all_listings.append(l)
            seen_urls.add(l["url"])
    
    print(f"  → {len(listings)} annonces trouvées")

    pagination_urls = get_pagination_urls(soup, LISTING_URL)
    for i, page_url in enumerate(pagination_urls[:max_pages - 1], 2):
        print(f"\n📄 Fetching page {i}: {page_url}")
        time.sleep(1.5)
        html = fetch_page(page_url)
        if not html:
            continue
        listings, _ = parse_listing_page(html)
        for l in listings:
            if l["url"] not in seen_urls:
                all_listings.append(l)
                seen_urls.add(l["url"])
        print(f"  → {len(listings)} annonces trouvées")

    print(f"\n{'='*60}")
    print(f"📊 Total listings: {len(all_listings)}")
    print(f"{'='*60}")

    detailed_listings = []
    for i, listing in enumerate(all_listings, 1):
        url = listing["url"]
        print(f"\n🔍 [{i}/{len(all_listings)}] Détail: {listing.get('titre', url)[:60]}...")
        time.sleep(1.0)
        
        html = fetch_page(url)
        if not html:
            detailed_listings.append(listing)
            continue

        detail = parse_detail_page(html, url)
        merged = {**listing, **detail}
        detailed_listings.append(merged)
        
        if "date_limite_offres" in detail:
            print(f"  ⏰ DLDO: {detail['date_limite_offres']}")
        if "activite" in detail:
            print(f"  🏭 Activité: {detail['activite']}")

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
    parser = argparse.ArgumentParser(description="Scraper Actify")
    parser.add_argument("--max-pages", type=int, default=10, help="Nombre max de pages à scraper")
    parser.add_argument("--no-details", action="store_true", help="Ne pas scraper les pages de détail")
    args = parser.parse_args()

    listings = scrape_actify(max_pages=args.max_pages)
    save_results(listings)
