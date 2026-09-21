#!/usr/bin/env node
/**
 * Enrichissement IA des cibles (agences d'intérim) — Claude + recherche web.
 * Pour chaque société : site web, secteurs servis, spécialité, description,
 * signaux (groupe / multi-agences / franchise / à vendre…), intérêt 1-5 + raison.
 *
 * Entrée  : data/pappers_results.json (liste SIRENE)
 * Sortie  : data/enrichment.json  { siren: {...} }  — cache, reprise possible
 *
 *   export ANTHROPIC_API_KEY=sk-ant-...
 *   node scrapers/enrich_ai.mjs                 # tout ce qui n'est pas encore enrichi
 *   node scrapers/enrich_ai.mjs --limit 50      # les 50 premières (ordre du fichier = dept, CA)
 *   node scrapers/enrich_ai.mjs --dept 92       # un département
 *   node scrapers/enrich_ai.mjs --naf 78.20Z    # ETT seulement
 *   node scrapers/enrich_ai.mjs --siren 123456789 --force
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Anthropic from "@anthropic-ai/sdk";
import { zodOutputFormat } from "@anthropic-ai/sdk/helpers/zod";
import { z } from "zod";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const IN = path.join(ROOT, "data", "pappers_results.json");
const OUT = path.join(ROOT, "data", "enrichment.json");
const MODEL = "claude-opus-5";
const CONCURRENCY = 4;

// ---- args
const args = process.argv.slice(2);
const opt = (name, def) => { const i = args.indexOf(name); return i >= 0 ? args[i + 1] : def; };
const LIMIT = parseInt(opt("--limit", "0"), 10);
const DEPT = opt("--dept", null);
const NAF = opt("--naf", null);
const SIREN = opt("--siren", null);
const FORCE = args.includes("--force");
const VERIFY_ONLY = args.includes("--verify-only");

// ---- schéma de sortie
const Enrichment = z.object({
  site_web: z.string().describe("URL du site officiel de l'agence, ou chaîne vide si introuvable"),
  trouve: z.boolean().describe("true si le site / des informations fiables ont été trouvés"),
  secteurs: z.array(z.string()).describe("Secteurs servis, ex: BTP, industrie, logistique, tertiaire, santé, hôtellerie-restauration, luxe, IT, événementiel, aéronautique, transport"),
  specialite: z.string().describe("Positionnement en 1 ligne (ex: 'ETT spécialisée BTP gros œuvre IDF', 'généraliste multi-agences')"),
  description: z.string().describe("2-3 phrases factuelles : activité, implantation, clients types, taille perçue"),
  nb_agences: z.number().nullable().describe("Nombre d'agences physiques si connu, sinon null"),
  signaux: z.array(z.string()).describe("Signaux utiles pour une reprise : 'groupe / filiale', 'franchise / réseau', 'indépendant', 'famille', 'site à l'abandon', 'annonce de cession', 'levée de fonds', 'multi-sites', 'certification (Qualiopi, MASE…)'"),
  interet: z.number().int().min(1).max(5).describe("Intérêt pour un repreneur cherchant une agence d'intérim indépendante en IDF : 5 = très intéressant"),
  raison: z.string().describe("Pourquoi cette note, en 1-2 phrases"),
});

const SYSTEM = `Tu es analyste M&A pour un repreneur qui cherche à racheter une agence d'intérim / travail temporaire indépendante en Île-de-France (Paris, Hauts-de-Seine).
Pour la société fournie, trouve son site web officiel et qualifie-la : secteurs de délégation, positionnement, taille, appartenance à un groupe ou réseau, signaux de cession.
Règles :
- Utilise la recherche web (nom + ville, nom + "intérim", SIREN). 3 recherches maximum.
- Ne confonds pas avec un homonyme : le siège et le SIREN doivent correspondre. En cas de doute, trouve=false et site_web="".
- Intérêt élevé = ETT indépendante ou familiale, plusieurs agences ou spécialité claire, site vivant, pas de groupe national derrière. Intérêt faible = filiale de groupe (Adecco, Manpower, Synergie, Crit, Proman…), portage salarial pur, cabinet de recrutement / mannequins, société dormante.
- Réponds uniquement en français, factuel, sans inventer : si rien n'est trouvé, dis-le.`;

const client = new Anthropic();

// ---- vérification mécanique du site : on cherche des preuves (SIREN, CP, nom) dans les pages
const norm = (t) => (t || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
async function fetchPage(url) {
  try {
    const ctrl = new AbortController(); const to = setTimeout(() => ctrl.abort(), 12000);
    const r = await fetch(url, { signal: ctrl.signal, redirect: "follow", headers: { "User-Agent": "Mozilla/5.0 (Macintosh) CheckingMA/1.0", "Accept-Language": "fr" } });
    clearTimeout(to);
    if (!r.ok) return null;
    const html = await r.text();
    const text = html.replace(/<script[\s\S]*?<\/script>|<style[\s\S]*?<\/style>/gi, " ").replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/\s+/g, " ");
    const links = [...html.matchAll(/href=["']([^"'#?]+)["']/gi)].map((m) => m[1]);
    return { text, links, url: r.url };
  } catch { return null; }
}
export async function verifySite(url, c) {
  const out = { confiance: 0, label: "Non trouvé", preuves: [], pages: 0 };
  if (!url) return out;
  let base; try { base = new URL(url.startsWith("http") ? url : "https://" + url); } catch { return out; }
  const home = await fetchPage(base.href);
  if (!home) { out.label = "Site injoignable"; return out; }
  out.pages = 1; let text = home.text;
  // pages légales / contact trouvées dans les liens de l'accueil, puis chemins usuels
  const KEY = /mention|legal|légal|cgv|cgu|contact|propos|qui-sommes|societe|société|agence/i;
  const seen = new Set([home.url, base.href]);
  const cands = [];
  for (const l of home.links) { try { const u = new URL(l, home.url); if (u.origin === new URL(home.url).origin && KEY.test(u.pathname) && !seen.has(u.href)) { seen.add(u.href); cands.push(u.href); } } catch {} }
  for (const p of ["/mentions-legales", "/mentions-legales/", "/mentions_legales", "/legal", "/contact", "/qui-sommes-nous", "/a-propos"]) { const u = new URL(p, base.origin).href; if (!seen.has(u)) { seen.add(u); cands.push(u); } }
  for (const u of cands.slice(0, 10)) { const pg = await fetchPage(u); if (pg) { out.pages++; text += " " + pg.text; } if (text.length > 600000) break; }
  const digits = text.replace(/[\s.\u00a0-]/g, "");
  const siren = (c.siren || "").replace(/\D/g, "");
  if (siren && digits.includes(siren)) out.preuves.push("SIREN trouvé sur le site");
  const n = norm(text);
  const cp = c.code_postal || "";
  if (cp && n.includes(cp)) out.preuves.push("code postal du siège");
  const rue = norm((c.adresse || "").replace(/^\d+\s*(bis|ter)?\s*/, "")).split(cp)[0].trim();
  if (rue && rue.length > 6 && n.includes(rue)) out.preuves.push("adresse du siège");
  const nom = norm(c.nom).replace(/\b(sas|sarl|sa|eurl|sasu|societe|groupe)\b/g, "").trim();
  const ens = norm(c.enseignes).split(",").map((x) => x.trim()).filter((x) => x.length > 3);
  if ((nom.length > 3 && n.includes(nom)) || ens.some((e) => n.includes(e))) out.preuves.push("nom / enseigne");
  // nom distinctif dans le domaine (ex. "sbc" dans sbc-interim.fr)
  const host = norm(base.hostname).replace(/^www\./, "");
  const words = norm(c.nom + " " + (c.enseignes || "")).replace(/\b(sas|sarl|sa|eurl|sasu|societe|groupe|france|paris|interim|international|services?|conseil|rh)\b/g, " ").split(/[^a-z0-9]+/).filter((w) => w.length >= 3);
  if (words.some((w) => host.includes(w))) out.preuves.push("nom dans le domaine");
  const d = norm((c.dirigeant || {}).nom || "").split(" ").filter((x) => x.length > 3);
  if (d.length && d.every((x) => n.includes(x))) out.preuves.push("nom du dirigeant");
  const has = (k) => out.preuves.some((p) => p.startsWith(k));
  const nomOk = has("nom /") || has("nom dans");
  if (has("SIREN")) out.confiance = nomOk ? 98 : 92;
  else if (has("adresse") && nomOk) out.confiance = 85;
  else if (has("code postal") && nomOk) out.confiance = has("nom /") && has("nom dans") ? 78 : 70;
  else if (has("nom du dirigeant") && nomOk) out.confiance = 72;
  else if (has("nom /") && has("nom dans")) out.confiance = 60;
  else if (nomOk) out.confiance = 50;
  else if (has("code postal") || has("adresse")) out.confiance = 35;
  else out.confiance = 15;
  out.label = out.confiance >= 90 ? "Sûr" : out.confiance >= 70 ? "Probable" : out.confiance >= 45 ? "Douteux" : "Non vérifié";
  Object.defineProperty(out, "_text", { value: text.slice(0, 200000), enumerable: false }); // pour la classification, non persisté
  return out;
}

function loadJSON(p, def) { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return def; } }
function saveJSON(p, obj) { fs.writeFileSync(p, JSON.stringify(obj, null, 2)); }

async function enrichOne(c) {
  const d = c.dirigeant || {};
  const prompt = `Société : ${c.nom}${c.enseignes ? ` (enseigne : ${c.enseignes})` : ""}
SIREN : ${c.siren}
Activité (NAF) : ${c.code_naf} — ${c.libelle_naf}
Siège : ${c.adresse}, ${c.code_postal} ${c.ville} (${c.departement})
Créée en : ${(c.date_creation || "").slice(0, 4)}
Dirigeant : ${d.nom || "?"} (${d.age || "?"} ans, ${d.qualite || ""})
CA connu : ${c.chiffre_affaires ? c.chiffre_affaires.toLocaleString("fr-FR") + " € (" + c.annee_comptes + ")" : "non publié"}
Effectif : ${c.effectif} · Établissements : ${c.nb_etablissements}
Fiche annuaire : ${c.url_annuaire}`;

  const response = await client.beta.messages.create({
    model: MODEL,
    max_tokens: 4000,
    betas: ["server-side-fallback-2026-07-01"],
    fallbacks: "default",
    thinking: { type: "adaptive" },
    output_config: { effort: "low", format: zodOutputFormat(Enrichment) },
    tools: [{ type: "web_search_20260209", name: "web_search", max_uses: 3, user_location: { type: "approximate", country: "FR", city: "Paris" } }],
    system: SYSTEM,
    messages: [{ role: "user", content: prompt }],
  });

  if (response.stop_reason === "refusal") throw new Error("refusal: " + (response.stop_details?.category || ""));
  const text = response.content.filter((b) => b.type === "text").map((b) => b.text).join("");
  const parsed = Enrichment.parse(JSON.parse(text));
  const verif = await verifySite(parsed.site_web, c);
  return { ...parsed, verif, model: response.model, enriched_at: new Date().toISOString(),
    usage: { in: response.usage.input_tokens, out: response.usage.output_tokens, searches: response.usage.server_tool_use?.web_search_requests ?? null } };
}

async function main() {
  const src = loadJSON(IN, null);
  if (!src) { console.error("❌ data/pappers_results.json introuvable — lance sirene_hunter.py d'abord"); process.exit(1); }
  let companies = [...(src.succession?.companies || []), ...(src.distressed?.companies || [])];
  if (DEPT) companies = companies.filter((c) => c.departement === DEPT);
  if (NAF) companies = companies.filter((c) => c.code_naf === NAF);
  if (SIREN) companies = companies.filter((c) => c.siren === SIREN);

  const cache = loadJSON(OUT, {});
  if (VERIFY_ONLY) {
    const list = companies.filter((c) => cache[c.siren]?.site_web);
    console.log(`🔁 Re-vérification mécanique de ${list.length} sites (sans IA)`);
    for (const c of list) { cache[c.siren].verif = await verifySite(cache[c.siren].site_web, c); console.log(`  ${cache[c.siren].verif.label.padEnd(12)} ${String(cache[c.siren].verif.confiance).padStart(3)}%  ${c.nom.slice(0, 40)}  ${cache[c.siren].site_web}`); }
    saveJSON(OUT, cache); return;
  }
  let todo = companies.filter((c) => FORCE || !cache[c.siren]);
  if (LIMIT > 0) todo = todo.slice(0, LIMIT);
  console.log(`🔎 ${todo.length} sociétés à enrichir (${companies.length - todo.length} déjà en cache) · modèle ${MODEL} · ${CONCURRENCY} en parallèle`);

  let done = 0, errors = 0, tokIn = 0, tokOut = 0, searches = 0;
  const queue = [...todo];
  const worker = async () => {
    while (queue.length) {
      const c = queue.shift();
      try {
        const r = await enrichOne(c);
        cache[c.siren] = r;
        tokIn += r.usage.in; tokOut += r.usage.out; searches += r.usage.searches || 0;
        console.log(`  ✓ [${++done}/${todo.length}] ${c.nom.slice(0, 36).padEnd(36)} ${r.interet}/5  ${r.site_web || "(pas de site)"}${r.site_web ? `  [${r.verif.label} ${r.verif.confiance}%]` : ""}`);
      } catch (e) {
        errors++;
        cache[c.siren] = { error: String(e.message || e).slice(0, 200), enriched_at: new Date().toISOString() };
        console.log(`  ✗ [${++done}/${todo.length}] ${c.nom.slice(0, 36)} — ${String(e.message || e).slice(0, 80)}`);
      }
      if (done % 5 === 0) saveJSON(OUT, cache);
    }
  };
  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  saveJSON(OUT, cache);
  // Opus 5 : $5 / M in, $25 / M out ; web search ≈ $10 / 1000 requêtes
  const cost = tokIn / 1e6 * 5 + tokOut / 1e6 * 25 + searches / 1000 * 10;
  console.log(`\n💾 ${OUT}\n   ${done - errors} ok, ${errors} erreurs · ${tokIn} tok in, ${tokOut} tok out, ${searches} recherches ≈ $${cost.toFixed(2)}`);
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
