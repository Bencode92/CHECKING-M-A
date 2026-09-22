#!/usr/bin/env node
// Re-vérifie (sans moteur de recherche ni IA) tous les sites déjà trouvés avec les règles de preuve courantes.
// Un site qui retombe sous 45 % est déclassé en « rejeté ».
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { verifySite } from "./enrich_ai.mjs";
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = JSON.parse(fs.readFileSync(path.join(ROOT, "data/pappers_results.json"), "utf8"));
const companies = [...(src.succession?.companies || []), ...(src.distressed?.companies || [])];
const OUT = path.join(ROOT, "data/enrichment.json");
const cache = JSON.parse(fs.readFileSync(OUT, "utf8"));
const MAX = parseInt(process.argv[process.argv.indexOf("--max") + 1] || "100", 10); // --max 69 : seulement les douteux
const list = companies.filter((c) => cache[c.siren]?.site_web && (cache[c.siren].verif?.confiance ?? 0) <= MAX);
console.log(`🔁 ${list.length} sites à re-vérifier`);
let i = 0, demoted = 0, changed = 0;
for (const c of list) {
  const rec = cache[c.siren], before = rec.verif?.confiance;
  const v = await verifySite(rec.site_web, c);
  if (!v.pages) { i++; continue; } // injoignable maintenant : on garde l'ancienne mesure
  rec.verif = v; rec.trouve = v.confiance >= 70;
  if (v.confiance !== before) changed++;
  if (v.confiance < 45) { rec.rejete = rec.site_web; rec.site_web = ""; rec.trouve = false; rec.secteurs = []; rec.signaux = []; demoted++; }
  console.log(`  [${String(++i).padStart(3)}/${list.length}] ${c.nom.slice(0, 34).padEnd(34)} ${String(before).padStart(3)}% → ${String(v.confiance).padStart(3)}% ${v.label.padEnd(11)} ${rec.site_web || "(rejeté " + rec.rejete + ")"}`);
  if (i % 10 === 0) fs.writeFileSync(OUT, JSON.stringify(cache, null, 2));
}
fs.writeFileSync(OUT, JSON.stringify(cache, null, 2));
console.log(`\n💾 ${changed} confiances modifiées, ${demoted} sites rejetés`);
