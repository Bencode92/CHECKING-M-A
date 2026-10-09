# REPRISE DU PROJET SUR UN NOUVEL ORDINATEUR

> Document de passation complet. Tout ce qui est décrit ici est dans le dépôt GitHub
> **https://github.com/Bencode92/CHECKING-M-A** — rien d'essentiel ne vit uniquement sur l'ancienne machine.
> Rédigé le 9 octobre 2026. Lis les sections 1 et 2, le reste se consulte au besoin.

---

## 1. LE PROJET EN UNE PAGE

**Objectif** : racheter une **agence d'intérim indépendante à Paris (75) ou dans les Hauts-de-Seine (92)**,
à titre personnel. Le dépôt contient l'outil de sourcing, la liste des cibles, leur qualification,
le suivi de prospection et le benchmark du marché.

**Point de départ historique** : le dépôt s'appelait `CHECKING-M-A` et servait à chercher des PME
françaises de luxe/artisanat à reprendre. Le 17 septembre 2026, il a été **entièrement recentré sur l'intérim**.
Tout ce qui parlait de boulangerie, cristallerie ou maroquinerie a été retiré (consultable dans l'historique git).

**Ce qui est acquis aujourd'hui :**

| Élément | État |
|---|---|
| Vivier de cibles | **524 sociétés** (NAF 78.10Z / 78.20Z / 78.30Z, siège 75 ou 92, dirigeant 50 ans et +) |
| Cœur de cible | **46 vraies ETT** (78.20Z) avec CA publié entre 1 et 15 M€, dont **21 dirigeants de 60 ans et +** |
| Sites web | **162 trouvés et vérifiés** : 42 « Sûr » (SIREN sur le site), 48 « Probable », 72 « Douteux » |
| Pré-qualification | **26 cibles ★ intéressantes**, 27 filiales de groupe écartées |
| Benchmark marché | Page complète : économie du métier, valorisation, BFR, contraintes légales, point secteur |
| Coût | **0 €** — tout vient d'API publiques gratuites. Aucune clé Pappers ni Anthropic utilisée. |

**Les trois pages de l'outil** (en ligne, publiques) :

- **Liste et planning** → https://bencode92.github.io/CHECKING-M-A/cibles.html
- **Benchmark marché** → https://bencode92.github.io/CHECKING-M-A/marche.html
- **Sources brutes** (Pappers / Actify / BODACC) → https://bencode92.github.io/CHECKING-M-A/dashboard.html

---

## 2. INSTALLATION SUR LE NOUVEL ORDI — DANS L'ORDRE

```bash
# 1. Outils de base (si Homebrew n'est pas là : https://brew.sh)
brew install git gh node python@3.12

# 2. Connexion GitHub
gh auth login          # compte Bencode92, protocole HTTPS

# 3. Récupérer le projet
cd ~
gh repo clone Bencode92/CHECKING-M-A
cd CHECKING-M-A

# 4. Dépendances
pip3 install -r requirements.txt     # requests, beautifulsoup4, pyyaml, playwright
npm install                          # @anthropic-ai/sdk, zod (pour l'enrichissement IA optionnel)

# 5. Ouvrir l'outil
chmod +x "Ouvrir Cibles.command"
open "Ouvrir Cibles.command"         # lance un serveur local et ouvre la page
```

**Vérification que tout est bon** : la page doit afficher **524 sociétés** en haut à droite,
**26 ★ Marquées** et **27 Écartées** dans les compteurs.

### ⚠️ LA PREMIÈRE CHOSE À FAIRE DANS LA PAGE

En haut à droite il y a un bouton. **S'il est rouge (« ⚠ SAUVEGARDE NON BRANCHÉE »), clique dessus**
et choisis le fichier `data/annotations.json` dans le dossier CHECKING-M-A.

Le bouton passe au vert et **chaque clic est désormais écrit sur le disque**.

> **Pourquoi c'est critique** : le 23 septembre, deux heures de travail de tri ont été perdues parce que
> le suivi ne vivait que dans la mémoire du navigateur. Rien n'a pu être récupéré (vérifié : tous les
> profils Chrome, Brave, Opera, Safari, corbeille — aucune trace). La version actuelle écrit dans un
> vrai fichier, mais **il faut le brancher une fois par navigateur**.
> Procédure de secours détaillée dans `RECUPERER_SUIVI.md`.

---

## 3. CE QUE CONTIENT LE DÉPÔT

```
CHECKING-M-A/
├── cibles.html                 ★ L'OUTIL PRINCIPAL : liste, fiches, statuts, planning, kanban
├── marche.html                 Benchmark du marché de l'intérim + point secteur par secteur
├── dashboard.html              Sources brutes : Pappers / Actify / BODACC (3 onglets)
├── email_expert.md             E-mail type pour contacter un expert (3 variantes)
├── RECUPERER_SUIVI.md          Que faire si un suivi semble perdu
├── Ouvrir Cibles.command       Double-clic → serveur local + ouverture de la page
├── config.yaml                 Cible : codes NAF, mots-clés, départements, filtres Pappers
├── deploy.sh                   Déploiement Cloudflare Pages privé (jamais exécuté, voir §8)
├── functions/_middleware.js    Mot de passe pour le déploiement privé
├── scrapers/
│   ├── sirene_hunter.py        ★ Construit la liste des cibles (API SIRENE gratuite)
│   ├── find_sites.mjs          ★ Trouve les sites web (Bing + vérification, sans IA)
│   ├── reverify.mjs            Re-vérifie les sites déjà trouvés avec des règles plus strictes
│   ├── enrich_ai.mjs           Qualification par Claude + recherche web (OPTIONNEL, payant)
│   ├── bodacc_monitor.py       Procédures collectives d'agences d'intérim (API BODACC)
│   ├── actify_scraper.py       Reprises à la barre du tribunal (scraping Actify)
│   └── pappers_hunter.py       Variante payante du sourcing (nécessite un token Pappers)
└── data/
    ├── pappers_results.json    ★ LES 524 CIBLES (nom, dirigeant, âge, CA, effectif, adresse)
    ├── enrichment.json         ★ Sites web + secteurs + signaux, 524 fiches
    ├── annotations.json        ★ TON SUIVI : statuts, notes, relances — le fichier à brancher
    ├── cibles_interim_idf.csv  La même liste en CSV, triée, pour Excel
    ├── Liste_...50plus.xlsx    La même liste en Excel mis en forme (filtres, liens cliquables)
    ├── bodacc_alerts.json      73 annonces de procédures collectives d'agences d'intérim
    └── actify_listings.json    Annonces de reprise à la barre (peu d'intérim dedans)
```

---

## 4. LES DONNÉES — D'OÙ ELLES VIENNENT, COMMENT LES REFAIRE

### 4.1 La liste des cibles (le socle)

**Source** : `recherche-entreprises.api.gouv.fr` — l'API publique SIRENE, celle qui alimente
annuaire-entreprises.data.gouv.fr. **Gratuite, sans compte, sans token**, limitée à ~7 requêtes/seconde.
Elle donne légalement : dirigeants avec année de naissance, comptes déposés non confidentiels,
effectif, date de création, adresse du siège.

```bash
cd ~/CHECKING-M-A/scrapers
python3 sirene_hunter.py --dept 75 92 --age-min 50
# → data/pappers_results.json + data/cibles_interim_idf.csv
```

Options utiles : `--naf 78.20Z` (ETT seulement), `--ca-min 1000000`, `--all` (sans filtre d'âge),
`--keep-grands` (garder Adecco, Manpower… normalement exclus).

**Ce que le script filtre tout seul** : sièges hors 75/92, NAF du siège hors 78.xx,
dirigeants personnes morales (commissaires aux comptes exclus), dirigeants de plus de 85 ans
(registre non mis à jour), grandes entreprises et ETI.

> Le fichier s'appelle `pappers_results.json` pour des raisons historiques (le dashboard le lit sous ce nom),
> **mais il ne contient aucune donnée Pappers** — tout vient de SIRENE.

### 4.2 Les sites web (sans IA, sans clé)

```bash
node scrapers/find_sites.mjs            # toutes les sociétés pas encore traitées
node scrapers/find_sites.mjs --limit 20 # test
node scrapers/reverify.mjs --max 69     # re-vérifier seulement les sites douteux
```

**Comment ça marche** : pour chaque société, 3 recherches Bing (nom + ville + intérim, **le SIREN**
— les mentions légales sont indexées —, puis nom + code postal), puis le script ouvre les sites candidats
(accueil + pages mentions légales / contact) et cherche des **preuves** :

| Preuve trouvée | Confiance | Label |
|---|---|---|
| SIREN ou SIRET sur le site (+ nom) | 92–98 % | **✓ Sûr** |
| Adresse du siège + nom | 85 % | Probable |
| Code postal + nom, ou nom du dirigeant | 70–78 % | Probable |
| Nom seul **et** vocabulaire intérim/RH présent | 50–60 % | Douteux |
| Rien ne colle | 15 % | rejeté |

Deux garde-fous ont été ajoutés après des faux positifs réels :
un mot court (« cap », « sbc ») ne compte que s'il forme un segment entier du domaine
(`sbc-interim.fr` oui, `capcut.com` non), et un site qui ne parle ni d'intérim ni de recrutement
ne peut pas être validé sur le seul nom (ça a éliminé decathlon.fr, rightmove.co.uk, abcbourse.com…).

Durée : ~25 secondes par société, soit **~3 h pour les 524**. Le script reprend où il s'est arrêté.

### 4.3 Les procédures collectives (agences en difficulté)

```bash
python3 scrapers/bodacc_monitor.py --days 90
```
API BODACC publique, filtrée sur les mots-clés intérim, puis **enrichie par SIRENE** pour récupérer
le code NAF (le BODACC n'en a pas). Dernier passage : 73 annonces, dont 38 confirmées NAF 78.xx.
Un workflow GitHub Actions le relance automatiquement **du lundi au vendredi à 8h15**.

### 4.4 L'enrichissement par IA — OPTIONNEL, PAYANT, PAS UTILISÉ

`scrapers/enrich_ai.mjs` interroge Claude avec recherche web pour produire une description,
une spécialité et une note d'intérêt sur 5. **Jamais lancé** : il faut `ANTHROPIC_API_KEY`,
et ça coûte ~5 ¢ par société, soit ~25 € pour les 524. La version sans IA fait déjà 90 % du travail.

---

## 5. COMMENT ON SE SERT DE L'OUTIL

### Page « Cibles » (`cibles.html`)

**Colonnes** : ★ · Statut · Société · NAF · Dirigeant + âge · CA et résultat · Effectif et année ·
Secteurs · IA · Site (avec label de fiabilité) · **Avis** · Relance · Notes.

**Les trois boutons de la colonne Avis**, sans ouvrir la fiche :
- 👍 = Intéressant (re-clic pour annuler)
- 👎 = Pas intéressant (ligne barrée, filtrable)
- 🏷 = ajouter un secteur (liste numérotée ou texte libre)

**« + site »** apparaît quand aucun site n'a été trouvé : tu colles l'adresse, elle est marquée `✎ manuel`
et prend le pas sur celle trouvée automatiquement. Si le site trouvé est faux, tu le remplaces dans la fiche.

**La fiche latérale** (clic sur une ligne) : liens Pappers / Annuaire / Google / site,
détail des preuves de vérification, 18 pastilles de secteur cliquables, contact (nom, tél, mail),
notes, **date de relance**, et un journal horodaté de toutes tes actions.

**Filtres** : recherche libre, département, NAF, secteur, intérêt, âge minimum, CA minimum,
« avec site », « site sûr ≥ 70 % », « ★ seulement », « masquer écartés ». Tri par colonne.

**Onglet Planning** : les relances groupées par semaine (« En retard », « Cette semaine », …),
les « à contacter » sans date, et un kanban par statut.

**Export** : bouton Excel/CSV — exporte la liste filtrée avec tout le suivi.

### Les filtres qui servent vraiment

| Objectif | Filtres à poser |
|---|---|
| Le cœur de cible | NAF `78.20Z` + CA ≥ 1000 k€ + âge ≥ 58 |
| Priorité santé | NAF `78.20Z` + Secteur `Santé` → **13 sociétés** |
| Priorité hôtellerie | NAF `78.20Z` + Secteur `Hôtellerie-restauration` |
| Celles qui ont un vrai site | chip « site sûr ≥ 70 % » → **90 sociétés** |

---

## 6. LA STRATÉGIE — CE QUI A ÉTÉ DÉCIDÉ ET POURQUOI

### 6.1 Les repères du métier

Le chiffre d'affaires d'une agence d'intérim, ce sont des heures refacturées : **il est gros mais creux**.

```
Salaire brut intérimaire        100 €
× coefficient de facturation    1,6 à 2,2   (généraliste ≈ 1,75–1,95)
= facturé au client             ~180 €
− coût réel (charges, IFM 10 %, ICCP 10 %, mutuelle)
= MARGE BRUTE                   15 à 20 % du CA  (12–15 % logistique · 25–35 % IT)
− coûts fixes (permanents, loyer, logiciel, garantie, assurance)
= RÉSULTAT NET                  3 à 8 % du CA quand ça va bien
```

Autres repères : CA par permanent 600 k€ – 1,2 M€ · point mort ≈ 880 k€ de CA pour 150 k€ de frais fixes ·
un consultant rentable tient 20 à 30 missions actives en même temps.

### 6.2 Le BFR — le point qui tue les agences

Les intérimaires sont payés **tous les mois sans délai** (~70 % du CA), les clients règlent
à 30/45/60 jours — **moyenne sectorielle réelle autour de 80 jours**. Plus l'agence croît,
plus sa trésorerie se creuse ; les impayés clients sont la première cause de défaillance du secteur.

**Compter 15 à 20 % du CA de BFR à financer EN PLUS du prix d'achat** — soit 600 à 800 k€
sur une agence à 4 M€. À auditer dès le premier rendez-vous : DSO réel, contrat d'affacturage
(et sa clause de changement de contrôle), assurance-crédit, balance âgée, créances sur clients
eux-mêmes en difficulté.

### 6.3 La valorisation

**Il n'existe aucune base publique de transactions d'ETT françaises.** Méfiance envers tout chiffre
présenté comme « le multiple du secteur ». Repères utilisables :

- **4 à 6 × l'EBITDA** (moyenne toutes PME ≈ 5,5 ×)
- ou **0,6 à 1,2 × la marge brute annuelle** — le vrai repère de métier
- **jamais en % du CA** : trompeur, le CA est gonflé par les salaires refacturés

Fait monter : spécialité résiliente, aucun client > 15–20 % du CA, marge brute > 20 % stable sur 3 ans,
process digitalisé, équipe permanente qui reste, plusieurs agences.
Fait chuter : client unique, BTP pur en IDF, DSO > 80 j, contentieux de requalification,
dirigeant seul commercial, marge brute < 14 %.

### 6.4 Les deux contraintes légales qui décident du deal

**La garantie financière** — obligatoire pour toute ETT, auprès d'une banque, d'un assureur ou
d'une société de caution. Minimum **8 % du CA annuel**, plancher légal **151 445 € en 2026**
(décret n° 2025-1350 du 26 décembre 2025). Coût courant 0,5 à 1 % du CA.
⚠️ **Le garant réapprécie le dossier au changement d'actionnaire.** Sans reconduction, l'agence perd
le droit d'exercer. → **Obtenir un accord de principe AVANT de signer, et en faire une condition suspensive.**

**La requalification** — une mission qui couvre durablement un emploi permanent du client peut être
requalifiée en CDI (rappels de salaires + redressement URSSAF). Ce passif suit la société si on
rachète les titres. À auditer : ancienneté des missions en cours (24 % des intérimaires font
désormais des missions de plus de 9 mois), motifs de recours écrits, prud'hommes en cours, dernier contrôle URSSAF.

> Rachat de **titres** = on prend le passé → garantie d'actif et de passif indispensable.
> Rachat de **fonds de commerce** = on laisse le passé, mais on perd l'agrément, la garantie financière
> et l'antériorité des contrats clients. À arbitrer avec un avocat.

### 6.5 Le marché, et le piège parisien

Chiffres Prism'emploi, janvier–mars 2026 :

| | Part des intérimaires | Évolution sur un an |
|---|---|---|
| Industrie | 39,0 % | **+ 4,9 %** |
| Services / tertiaire | 23,6 % | − 3,1 % |
| BTP | 16,1 % | − 5,3 % |
| Transports / logistique | 12,2 % | + 0,2 % |
| Commerce | 9,0 % | − 3,3 % |

Par qualification : ouvriers qualifiés 41,3 % (+0,8 %), ouvriers non qualifiés 35,9 % (+1,3 %),
employés 12,0 % (**−4,8 %**), cadres et professions intermédiaires 10,8 % (−2,6 %).

**Le décalage central** : le seul secteur qui monte (l'industrie) n'a presque **pas de donneur d'ordre
dans Paris intra-muros**, et les secteurs surreprésentés à Paris sont ceux qui reculent le plus.
L'Île-de-France est à **− 3,9 %** (pire région avec le Grand Est), tirée par le Commerce et le BTP.
C'est aussi la région où le CDI intérimaire est le moins développé (3,3 % contre 6,8 % au national) :
marché de missions courtes, peu fidélisé.

Marché en bas de cycle = **mauvaise nouvelle pour un vendeur, bonne fenêtre pour un acheteur**.
Mais il faut valoriser sur un exercice normalisé, pas sur le point bas, et ne pas payer le rebond promis.

### 6.6 Le classement des secteurs pour Paris

| Secteur | Marge brute | Verdict |
|---|---|---|
| **Santé / médico-social** | 20–28 % | **PRIORITÉ 1** — AP-HP, cliniques, ~700 EHPAD. Demande permanente, non cyclique, pénurie de soignants. ⚠️ **Loi Rist** : plafond 2 681 € HT / 24 h pour l'intérim **médical** dans le public — **le paramédical (IDE, AS, ASH) n'est PAS concerné**. Vérifier la part des médecins dans le CA. |
| **Hôtellerie-restauration-événementiel** | 18–25 % | **PRIORITÉ 2** — palaces, traiteurs, salons Viparis. Carnet d'adresses non réplicable. Saisonnalité brutale, missions très courtes. |
| Industrie / aéronautique | 16–22 % | Excellent secteur, mauvaise géographie. Vérifier **où sont les clients**, pas où est le siège. |
| Tertiaire qualifié | 18–25 % | Le gros du gisement parisien, mais le segment qui recule le plus vite. |
| IT / ingénierie | 25–35 % | Meilleures marges, mais **piège de classification** : souvent 78.30Z (portage/ESN), donc pas une ETT, pas de garantie financière, autre valorisation. |
| Logistique / transport | 12–15 % | Volume sans marge. Rien dans Paris intra-muros. |
| BTP | 15–20 % | **À décoter fort ou éviter** — −5,3 %, risque d'impayés client majeur. |
| Commerce / retail | 13–17 % | En repli. Chez nous, souvent du mannequinat (78.10Z). |
| Propreté / sécurité | 12–16 % | Peu d'intérêt : marges basses, main-d'œuvre volatile. |

**La règle de décision** : à Paris, ne pas chercher le secteur qui monte au national — chercher
**la spécialité défendable**. Une agence qui vit de trois grands comptes en tertiaire est fragile ;
une agence paramédicale avec 40 établissements clients a quelque chose qui ne se reconstruit pas
en deux ans. **C'est ça qui se paie, pas le CA.**

### 6.7 La grille de recherche

| Critère | Cible | Pourquoi |
|---|---|---|
| NAF | **78.20Z** en priorité | Vraie ETT. 78.10Z = recrutement/mannequinat, 78.30Z = portage. |
| CA | 1,5 – 10 M€ | En dessous, sous le point mort. Au-dessus, autre ligue de prix et de concurrents. |
| Marge brute | ≥ 18 % | Jamais publiée — **la question à poser au premier appel**. |
| Âge du dirigeant | 60 – 72 ans | Fenêtre de succession réelle. |
| Ancienneté | > 12 ans | Portefeuille installé, agrément en place. |
| Concentration client | aucun > 20 % | Premier facteur de destruction de valeur après reprise. |
| Secteurs | industrie, santé, logistique, tertiaire qualifié | Résilients sur ce cycle. |
| Indépendance | pas de groupe derrière | Une filiale ne se vend pas à un repreneur individuel. |

---

## 7. LES CIBLES CONCRÈTES À ATTAQUER

Les 26 déjà marquées ★ dans l'outil sont celles qui cochent : ETT 78.20Z + CA 1–15 M€ + dirigeant 58 ans et +.
Les plus nettes :

| Société | Dép | Âge | CA | Pourquoi |
|---|---|---|---|---|
| **3S SANTE** | 75 | 76 ans | 5,5 M€ | **Santé + mention de cession sur le site**, SIREN vérifié à 98 % — le profil le plus net de la liste |
| LCI SAINT MAUR | 75 | 66 ans | 12,7 M€ | Gros, groupe Sultan |
| SALES FACTORY INTERIM | 92 | 58 ans | 6,8 M€ | |
| REGITRA | 75 | 78 ans | 6,1 M€ | Créée en 1979, site vérifié, industrie/BTP |
| SBC ELEC / TERTIAIRE / RHÔNE-ALPES | 75 | 66 ans | ~19 M€ cumulés | Groupe familial Albert Sultan, 3 sociétés |
| QUALI INTERIM | 92 | 59 ans | 6,2 M€ | |
| HACOR SERVICES | 75 | 72 ans | 4,7 M€ | |

Les **13 ETT santé** (le filtre prioritaire) : 3S SANTE, MEDIC INTERIM (92), SCIENTECH INTERIM,
CAPITALE TT, INTERIM CO, COMPAS SERVICES, IPF JOBS, TEMYS, DIRECT INTERIM, PRESS INTER,
PLANET-INTER IDF, ALTI SERVICES, MEDICOOP FRANCE (celle-ci trop grosse : 57 M€, coopérative nationale).

> Trois de ces sites sont manifestement faux (direct-assurance.fr, planet.fr, alti-trading.fr).
> C'est à ça que sert le bouton **« + site »** : corriger à la main.

**Les 9 agences dont le site mentionne une cession** sont repérables par le signal
« mention cession » dans leur fiche.

---

## 8. LES PIÈGES ET LES CHOSES À SAVOIR

### 8.1 Sauvegarde du suivi — LE point critique
Déjà dit en §2, mais c'est le plus important : **bouton vert = écrit sur le disque, rouge = perdu à la fermeture.**
Le fichier à brancher est `data/annotations.json`. S'il contient déjà quelque chose, la page **fusionne**,
elle n'écrase jamais. Fonctionne sur Chrome et Edge ; sur Safari et Firefox il faut cliquer « ⤓ Copie » régulièrement.

### 8.2 Le dépôt est PUBLIC et contient des données nominatives
`pappers_results.json` et `enrichment.json` contiennent les noms et âges de 524 dirigeants.
Ils sont en ligne parce que c'était nécessaire pour que la page fonctionne sur GitHub Pages.
**Conséquences** :
- Ne **jamais** envoyer ces fichiers par mail à un tiers (un expert, un intermédiaire) — version anonymisée seulement.
- Si tu veux les retirer un jour, `git rm` ne suffit pas : l'historique les garde. Il faudrait passer le dépôt en privé.
- Une alternative privée est prête mais **jamais déployée** : `deploy.sh` + `functions/_middleware.js`
  publient sur Cloudflare Pages derrière un mot de passe. Pour l'activer :
  ```bash
  wrangler pages project create checking-ma --production-branch main
  wrangler pages secret put SITE_PASSWORD --project-name checking-ma
  ./deploy.sh
  ```

### 8.3 Python 3.9 sur l'ancienne machine
Les scrapers `actify_scraper.py` et `pappers_hunter.py` utilisent la syntaxe `int | None`
qui demande **Python 3.10 minimum**. L'ancien Mac était en 3.9, donc ils ne tournaient qu'en CI.
Sur le nouveau, installe Python 3.12 (cf. §2) et tout fonctionne en local.

### 8.4 Les scripts qui peuvent se bloquer
`find_sites.mjs` s'est figé deux fois sur des sites qui répondent au compte-gouttes.
Corrigé : délai d'expiration de 15 s qui couvre aussi la lecture du corps de page.
Si un passage semble bloqué, `pkill -f find_sites` puis relance — **il reprend où il s'est arrêté** (cache par SIREN).

### 8.5 L'API Pappers — pas nécessaire
Une clé Pappers a été envisagée puis abandonnée : SIRENE donne gratuitement l'essentiel.
Pour mémoire, si tu y reviens un jour : v2 et v3 coexistent, 0,1 jeton par résultat de recherche,
1 jeton par fiche entreprise, 3 par document, 100 jetons offerts à la création du compte.
La stratégie économe serait : SIRENE pour le gros, `/recherche` filtrée (~15 jetons),
puis `/entreprise` sur une shortlist de 30–40 (~40 jetons).
Le script `pappers_hunter.py` est prêt, il lit `PAPPERS_API_TOKEN` dans l'environnement.

---

## 9. LE WORKFLOW DE PROSPECTION

1. **Filtrer** : NAF `78.20Z` + CA ≥ 1 M€ + âge ≥ 60 → une vingtaine de lignes. Les marquer ★.
2. **Qualifier en 5 minutes** par le site : secteurs servis, nombre d'agences, indépendance.
   👎 sur les filiales de groupe, 👍 sur les indépendantes spécialisées.
3. **Appeler le dirigeant directement** — pas de mail type. Trois questions suffisent à trier :
   **quelle marge brute · quel poids du premier client · quel délai de paiement moyen**.
4. **Dater la relance** dans la fiche : le planning se construit tout seul.
   Un « rappelez-moi dans 6 mois » est la réponse la plus fréquente et la plus prometteuse dans ce métier.
5. **Avant toute offre** : accord de principe du garant financier, balance âgée,
   top 10 clients avec % du CA, état des contentieux prud'homaux.

**Pour contacter un expert** : `email_expert.md` contient un e-mail prêt à envoyer avec trois jeux
de questions selon l'interlocuteur (conseil M&A, expert-comptable, avocat en droit social),
et la liste des endroits où le trouver (Prism'emploi, CRA, CNCFA, cabinets comptables spécialisés ETT,
sociétés de caution, LinkedIn).
Une version interactive existe aussi comme artefact Claude : https://claude.ai/artifact/RxksUiCZK43bGWmTKFg4Ur

---

## 10. CE QUI RESTE À FAIRE

- [ ] **Brancher le fichier de sauvegarde** dès la première ouverture sur le nouvel ordi (§2)
- [ ] Corriger à la main les sites faux des 13 ETT santé (bouton « + site »)
- [ ] Appeler les 26 cibles ★, en commençant par 3S SANTE
- [ ] Trouver et contacter un expert du secteur (§9)
- [ ] Décider : dépôt public ou bascule vers Cloudflare privé (§8.2)
- [ ] Optionnel : relancer `find_sites.mjs` dans quelques mois (les sites changent)
- [ ] Optionnel : élargir à la grande couronne si le gisement 75/92 s'épuise
      (`python3 sirene_hunter.py --dept 93 94 78 91 95 77 --age-min 50`)

---

## 11. HISTORIQUE DE CE QUI A ÉTÉ FAIT

| Date | Étape |
|---|---|
| 17 sept. | Recentrage complet du dépôt sur l'intérim (NAF 78.xx), abandon de l'ancienne cible luxe/artisanat. Dashboard en thème clair. |
| 17 sept. | `sirene_hunter.py` : découverte que l'API SIRENE donne gratuitement dirigeants + âge + CA → **524 cibles**, zéro euro dépensé. |
| 18 sept. | Page `cibles.html` : tableau CRM, fiches, statuts, planning, kanban, export. |
| 21 sept. | Vérification mécanique des sites (preuves SIREN/adresse/nom → % de confiance). Passage complet sur les 524. |
| 22 sept. | Deux passes de nettoyage : 140 faux positifs rejetés. Résultat final 42 Sûr / 48 Probable / 72 Douteux. |
| 22 sept. | Site modifiable à la main, avis 👍/👎, secteurs en un clic. |
| 23 sept. | Benchmark marché (`marche.html`) + point secteur par secteur appliqué à Paris. |
| 23 sept. | **Incident : perte de 2 h de travail de tri** (suivi uniquement en mémoire navigateur, rien de récupérable). → Refonte de la sauvegarde : écriture directe dans un fichier, alerte permanente. |
| 23 sept. | Pré-qualification automatique de 26 cibles ★ et 27 filiales écartées, pour repartir d'une base. |
| 24 sept. | E-mail type pour contacter un expert, en trois variantes. |
| 9 oct. | Ce document de passation. |

---

## 12. COMMANDES UTILES, EN VRAC

```bash
# Ouvrir l'outil
open ~/CHECKING-M-A/"Ouvrir Cibles.command"

# Servir à la main si le .command ne marche pas
cd ~/CHECKING-M-A && python3 -m http.server 8765
# puis http://localhost:8765/cibles.html

# Régénérer la liste des cibles
python3 scrapers/sirene_hunter.py --dept 75 92 --age-min 50

# Chercher les sites web manquants
node scrapers/find_sites.mjs

# Re-vérifier les sites douteux
node scrapers/reverify.mjs --max 69

# Surveiller les procédures collectives
python3 scrapers/bodacc_monitor.py --days 90

# Pousser des modifications (le dépôt est public : pas de données sensibles nouvelles)
git add -A && git commit -m "message" && git push origin main

# Voir l'état
git log --oneline | head -10
```

**Compte GitHub** : Bencode92 · **dépôt** : CHECKING-M-A (public) ·
**pages** : bencode92.github.io/CHECKING-M-A/
