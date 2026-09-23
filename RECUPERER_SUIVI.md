# Récupérer un suivi perdu (console du navigateur)

## 1. Un onglet de la page est-il encore ouvert ?
Cherche dans toutes les fenêtres Chrome un onglet `cibles.html`
(bencode92.github.io/CHECKING-M-A/cibles.html ou localhost:8765/cibles.html).
**Ne le ferme pas.**

## 2. Dans cet onglet : Cmd + Option + J (console), puis colle :

```js
copy(localStorage.getItem('cma_ann'))
```

Le contenu est copié dans le presse-papier. Colle-le dans un fichier
`~/CHECKING-M-A/data/annotations.json` et enregistre.

Si la console répond `null`, il n'y a rien à récupérer dans cet onglet.

## 3. Vérifier ce qu'il y a dedans, sans risque :

```js
JSON.parse(localStorage.getItem('cma_ann') || '{}')      // l'objet complet
Object.keys(JSON.parse(localStorage.getItem('cma_ann') || '{}')).length  // nb de fiches
```

## 4. Recharger la page : elle lit `data/annotations.json` au démarrage
quand le stockage du navigateur est vide.

---

## Pourquoi ça peut être définitivement perdu
- **Fenêtre de navigation privée** : le stockage vit en mémoire et disparaît à la fermeture.
- **Page ouverte en `file://`** (double-clic sur le fichier au lieu du serveur local).
- **Autre profil Chrome** que celui où tu cherches.

## Pour que ça n'arrive plus
Clique sur le bouton d'état en haut à droite de la page et choisis un fichier
(`annotations.json` dans ce dossier) : **chaque modification y est écrite immédiatement**.
Le bouton est vert quand c'est branché, rouge sinon.
