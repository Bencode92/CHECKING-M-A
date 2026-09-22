#!/usr/bin/env node
/**
 * Recherche des sites web SANS IA : moteur de recherche (Bing HTML) + vérification mécanique
 * (SIREN / adresse / nom sur le site → confiance %), puis classification par mots-clés
 * (secteurs servis, signaux groupe / réseau / multi-agences).
 *
 * Sortie : data/enrichment.json (même format que enrich_ai.mjs, source "bing+verif").
 * L'IA (enrich_ai.mjs) peut ensuite compléter uniquement les fiches qui en ont besoin.
 *
 *   node scrapers/find_sites.mjs                # toutes les sociétés sans site trouvé
 *   node scrapers/find_sites.mjs --limit 30
 *   node scrapers/find_sites.mjs --dept 92 --naf 78.20Z
 *   node scrapers/find_sites.mjs --force        # refait aussi celles déjà traitées
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { verifySite } from "./enrich_ai.mjs";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const IN = path.join(ROOT, "data", "pappers_results.json");
const OUT = path.join(ROOT, "data", "enrichment.json");
const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";
const PAUSE_MS = 2500;

const args = process.argv.slice(2);
const opt = (n, d) => { const i = args.indexOf(n); return i >= 0 ? args[i + 1] : d; };
const LIMIT = parseInt(opt("--limit", "0"), 10), DEPT = opt("--dept", null), NAF = opt("--naf", null), FORCE = args.includes("--force");

const EXCLUDE = /pappers|societe\.com|annuaire-entreprises|infogreffe|linkedin|indeed|facebook|instagram|pagesjaunes|kompass|verif\.com|manageo|lefigaro|lesechos|google\.|bing\.com|wikipedia|b-reputation|score3|ellisphere|creditsafe|dnb\.com|glassdoor|welcometothejungle|hellowork|jobteaser|apec\.fr|francetravail|pole-emploi|monster|jooble|leboncoin|yelp|trustpilot|europages|solocal|scores-et-decisions|rubypayeur|inpi\.fr|bodacc|legifrance|rocketreach|zoominfo|infonet|corporama|siren\.fr|sirene|prospectavenue|entreprises\.gouv|data\.gouv|societeinfo|dirigeants\.bfmtv|lannuaire|118|mappy|cylex|hoodspot|firmania|opendatasoft|figaro|capital\.fr|challenges|usinenouvelle|ouest-france|leparisien|youtube|twitter|x\.com|tiktok|pinterest|amazon|jobijoba|meteojob|regionsjob|talent\.com|glassdoor|jobrapido|optioncarriere|neuvoo|adzuna|staffme|qapa\.fr|interimaire\.fr|mission-interim|urssaf|impots|service-public|cci\.fr|cma-france|pappers\.fr|dnb|creditsafe|lebonagent|malt\.fr|freework|indeed|welcome/i;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms + Math.random() * 800));
const loadJSON = (p, d) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return d; } };
const saveJSON = (p, o) => fs.writeFileSync(p, JSON.stringify(o, null, 2));
const norm = (t) => (t || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();

async function bing(q) {
  const url = "https://www.bing.com/search?q=" + encodeURIComponent(q) + "&setlang=fr&cc=FR";
  try {
    const ctrl = new AbortController(); const to = setTimeout(() => ctrl.abort(), 15000);
    const r = await fetch(url, { signal: ctrl.signal, headers: { "User-Agent": UA, "Accept-Language": "fr-FR,fr;q=0.9" } });
    const html = await r.text();
    clearTimeout(to);
    const urls = [];
    for (const block of html.split('<li class="b_algo"').slice(1)) {
      const m = block.match(/<a[^>]+href="(https?:\/\/(?!r\.bing\.com)[^"]+)"/);
      if (!m) continue;
      let u = m[1].replace(/&amp;/g, "&");
      const b = u.match(/[?&]u=a1([A-Za-z0-9_-]+)/);
      if (b) { try { u = Buffer.from(b[1].replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8"); } catch {} }
      if (/^https?:\/\//.test(u)) urls.push(u);
    }
    return urls;
  } catch { return []; }
}

function slug(n) { return norm(n).replace(/\b(sas|sarl|sa|eurl|sasu|societe|groupe|france|paris|international)\b/g, "").replace(/[^a-z0-9]+/g, "").trim(); }
function guesses(c) {
  const out = new Set();
  for (const base of [slug(c.nom), ...(c.enseignes || "").split(",").map(slug)].filter((x) => x.length >= 4)) {
    for (const tld of ["fr", "com"]) { out.add(`https://www.${base}.${tld}`); out.add(`https://${base}-interim.${tld}`); }
  }
  return [...out].slice(0, 6);
}

const SECTEURS = {
  "BTP": /\b(btp|batiment|gros oeuvre|second oeuvre|chantier|macon|maconnerie|coffreur|electricien|plombier|couvreur|menuisier|travaux publics|genie civil)\b/,
  "Industrie": /\b(industrie|industriel|usine|production|maintenance industrielle|soudeur|usinage|chaudronn|mecanicien|operateur)\b/,
  "Logistique": /\b(logistique|entrepot|cariste|preparateur de commandes|manutention|magasinier|supply chain)\b/,
  "Transport": /\b(transport|chauffeur|livreur|livraison|poids lourd|vtc|conducteur)\b/,
  "Tertiaire": /\b(tertiaire|comptab|assistant|administratif|secretaire|gestionnaire|bureau|commercial|paie|rh|juridique|banque|assurance|finance)\b/,
  "Santé": /\b(sante|medical|infirmier|aide[- ]soignant|ehpad|pharmacie|hopital|clinique|paramedical|laboratoire)\b/,
  "Hôtellerie-restauration": /\b(hotellerie|restauration|cuisinier|serveur|hotel|traiteur|barman|plongeur|receptionniste)\b/,
  "Événementiel": /\b(evenementiel|evenement|salon|hotesse|hote|animation commerciale)\b/,
  "Luxe / Mode / Retail": /\b(luxe|mode|retail|boutique|vendeur|vente|pret[- ]a[- ]porter|maroquinerie|cosmetique|beaute)\b/,
  "IT / Digital": /\b(informatique|digital|developpeur|data|it\b|systemes d'information|cybersecurite|devops)\b/,
  "Aéronautique / Automobile": /\b(aeronautique|aerospatial|automobile|automotive|carrossier)\b/,
  "Propreté / Sécurité": /\b(proprete|nettoyage|agent d'entretien|securite|gardiennage|agent de securite)\b/,
  "Grande distribution": /\b(grande distribution|supermarche|hypermarche|caissier|employe libre[- ]service|mise en rayon)\b/,
  "Pharma / Chimie": /\b(pharmaceutique|chimie|chimique|agroalimentaire|cosmetique industrielle)\b/,
  "Ingénierie / Bureau d'études": /\b(ingenieur|bureau d'etudes|ingenierie|projeteur|dessinateur|technicien)\b/,
};
function classify(text) {
  const n = norm(text);
  const secteurs = Object.entries(SECTEURS).map(([k, re]) => [k, (n.match(new RegExp(re.source, "g")) || []).length]).filter(([, c]) => c >= 3).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([k]) => k);
  const signaux = [];
  if (/\b(filiale|groupe|holding)\b/.test(n)) signaux.push("groupe / filiale ?");
  if (/\b(franchise|franchis|reseau d'agences|licence de marque)\b/.test(n)) signaux.push("franchise / réseau");
  if (/\b(independant|familial|famille)\b/.test(n)) signaux.push("indépendant / familial");
  const ag = n.match(/\b(\d{1,3})\s+agences?\b/); if (ag && +ag[1] > 1) signaux.push(`${ag[1]} agences (site)`);
  if (/\bnos agences\b|\btrouver une agence\b/.test(n)) signaux.push("multi-agences");
  if (/\bqualiopi\b/.test(n)) signaux.push("Qualiopi"); if (/\bmase\b/.test(n)) signaux.push("MASE"); if (/\biso 9001\b/.test(n)) signaux.push("ISO 9001");
  if (/\b(cession|a vendre|reprise d'entreprise|cede)\b/.test(n)) signaux.push("mention cession");
  if (/\b(travail temporaire|interim|interimaire)\b/.test(n)) signaux.push("intérim confirmé");
  return { secteurs, signaux };
}

async function main() {
  const src = loadJSON(IN, null); if (!src) { console.error("❌ data/pappers_results.json introuvable"); process.exit(1); }
  let companies = [...(src.succession?.companies || []), ...(src.distressed?.companies || [])];
  if (DEPT) companies = companies.filter((c) => c.departement === DEPT);
  if (NAF) companies = companies.filter((c) => c.code_naf === NAF);
  const cache = loadJSON(OUT, {});
  let todo = companies.filter((c) => FORCE || !cache[c.siren] || (!cache[c.siren].site_web && cache[c.siren].source === undefined && cache[c.siren].error));
  todo = companies.filter((c) => FORCE || !cache[c.siren]);
  if (LIMIT) todo = todo.slice(0, LIMIT);
  console.log(`🔎 ${todo.length} sociétés (Bing + vérification, ${PAUSE_MS} ms entre requêtes ≈ ${Math.round(todo.length * 9 / 60)} min)`);
  let i = 0, found = 0, sure = 0;
  for (const c of todo) {
    i++;
    // 3 requêtes : nom+ville+intérim, SIREN (les mentions légales sont indexées), nom+ville
    const sirenFmt = c.siren.replace(/(\d{3})(\d{3})(\d{3})/, "$1 $2 $3");
    const queries = [`"${c.nom.replace(/'/g, " ")}" ${c.ville} intérim`, `"${sirenFmt}" OR "${c.siren}"`, `${c.nom.replace(/'/g, " ")} ${c.code_postal} ${c.ville}`];
    const origins = [];
    for (const q of queries) {
      const results = (await bing(q)).filter((u) => !EXCLUDE.test(u));
      for (const u of results) { try { const o = new URL(u).origin; if (!origins.includes(o)) origins.push(o); } catch {} }
      if (origins.length >= 6) break;
      await sleep(PAUSE_MS / 2);
    }
    const candidates = [...origins.slice(0, 6), ...guesses(c)];
    let best = null;
    for (const u of candidates) {
      const v = await verifySite(u, c);
      if (!v.pages) continue;
      if (!best || v.confiance > best.v.confiance) best = { u, v };
      if (v.confiance >= 90) break;
    }
    if (best && candidates.indexOf(best.u) >= 6) best.v.preuves.push("domaine deviné");
    const rec = { source: "bing+verif", enriched_at: new Date().toISOString(), candidats: candidates.slice(0, 6), trouve: false, site_web: "", secteurs: [], signaux: [], specialite: "", description: "", interet: null, raison: "" };
    if (best && best.v.confiance >= 45) {
      rec.trouve = best.v.confiance >= 70; rec.site_web = best.u; rec.verif = best.v;
      Object.assign(rec, classify(best.v._text || ""));
      found++; if (best.v.confiance >= 90) sure++;
    } else if (best) { rec.verif = best.v; rec.site_web = ""; rec.rejete = best.u; }
    cache[c.siren] = rec;
    console.log(`  [${String(i).padStart(3)}/${todo.length}] ${c.nom.slice(0, 34).padEnd(34)} ${rec.site_web ? `${best.v.label.padEnd(11)} ${String(best.v.confiance).padStart(3)}%  ${rec.site_web}` : "—"}${rec.secteurs.length ? "  · " + rec.secteurs.slice(0, 3).join(", ") : ""}`);
    if (i % 5 === 0) saveJSON(OUT, cache);
    await sleep(PAUSE_MS);
  }
  saveJSON(OUT, cache);
  console.log(`\n💾 ${OUT} — ${found} sites retenus dont ${sure} sûrs (SIREN sur le site) sur ${todo.length}`);
}
main();
