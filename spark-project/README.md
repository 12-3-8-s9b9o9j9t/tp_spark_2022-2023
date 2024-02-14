# Projet DMB

Pour le projet, j'ai décidé d'utiliser le dataset de kills de PUBG,
ou plutôt un sous ensemble de ce dataset (les 1000000 premières lignes).

Les questions auxquelles j'ai répondu sont les suivantes :
- Quelles armes utilisent les meilleurs joueurs ? les pires joueurs ?
- Quelles sont les zones les plus meurtrières pour chaque carte ?
- Quelles sont les distances de effetives de chaque arme ?

Le dataset me semblait être compatible avec la gestion des données sous forme de graphe,
j'ai donc créé des `case class` pour représenter les `vertices` et les `edges` du graphe.

Pour les `vertices`, j'ai les joueurs et les parties, et pour les `edges`, j'ai les relations
« a tué » et « s'est placé ».

J'ai eu besoin de réaliser pas mal de traitement des données:
- filtrer les joueurs non spécifiés ou explicitement indiqués comme inconnus pour la relation « s'est placé »
- filtrer les armes non spécifiées, ou n'en étant pas vraiment pour la relation « a tué » (j'ai concervé les véhicules et les dégats de zone)
- filtrer les kills ayant une position bugguée pour la victime, ou ayent eu lieu sur une carte non spécifiée pour la question 2
- filtrer les kills ayant une position bugguée pour le tueur ou la victime, ou ayant eu lieu sur une carte non spécifiée pour la question 3

De plus il a fallu que je formatte les positions pour les faire correspondre aux tailles des cartes (le facteur se trouvant être différent pour les deux cartes connues)