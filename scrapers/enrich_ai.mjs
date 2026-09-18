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
  return { ...parsed, model: response.model, enriched_at: new Date().toISOString(),
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
        console.log(`  ✓ [${++done}/${todo.length}] ${c.nom.slice(0, 36).padEnd(36)} ${r.interet}/5  ${r.site_web || "(pas de site)"}`);
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
main();
