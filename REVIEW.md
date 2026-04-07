# Code Review — ETL (état du code au 07/04/2026)

## Vue d'ensemble

Le projet est un pipeline ETL Python qui lit des fichiers JSON/CSV/XML depuis un
dossier `ToImport`, les convertit en `pandas.DataFrame`, puis les insère en base
PostgreSQL via SQLAlchemy. Quatre entités sont gérées : `User`, `Exercise`, `Food`,
`Health_metric`.

Depuis la première review, les points positifs ajoutés sont :
- `WriteLog` utilise désormais `with open(...)` ✅
- `sendToTable` centralise le routage par nom de fichier ✅
- `sendFoodToDb` et `sendHealthMetricToDb` sont implémentées ✅
- `csvHandler.py` et `xmlHandler.py` existent (fichiers vides pour l'instant)

Plusieurs bugs et problèmes de qualité subsistent, et de nouveaux ont été introduits
avec les nouvelles fonctions.

---

## 🔴 Bugs / Problèmes critiques

### 1. `gender.lower()` appelé avant le guard — `AttributeError` garanti

**Fichier :** `handlers/dbHandler.py`, lignes 73–74

```python
gender = row.get("gender").lower()          # exécuté en premier
if "gender" in row and gender in [...]:     # la vérification arrive trop tard
```

Après `fillna(0)`, si la colonne `gender` contient `NaN`, la valeur devient `0`
(entier). Appeler `.lower()` sur `0` lève `AttributeError`. Il faut vérifier
`"gender" in row` **avant** d'appeler `.get()`.

---

### 2. `equipment` toujours écrasé par la valeur brute

**Fichier :** `handlers/dbHandler.py`, lignes 192–196

```python
if (isinstance(equipment, list)):
    exercise.equipment = ", ".join(row.get("equipment") or [])  # jointure liste
exercise.equipment = equipment   # écrase systématiquement avec la valeur brute
```

La ligne 196 est en dehors du `if`, donc le résultat du `join` est immédiatement
perdu. La ligne 196 doit être dans un bloc `else`.

---

### 3. `exercise.secondary_muscle` peut rester non affecté

**Fichier :** `handlers/dbHandler.py`, lignes 186–190

```python
secondaryMuscle = row.get("secondary_muscle") or "No Secondary Muscle"
if (isinstance(secondaryMuscle, list)):
    exercise.secondary_muscle = ", ".join(...)
elif ("secondary_muscle" in row):
    exercise.secondary_muscle = secondaryMuscle
# Si "secondary_muscle" n'est PAS dans row : aucune branche prise
# exercise.secondary_muscle n'est jamais affecte → IntegrityError (nullable=False)
```

Il manque un `else: exercise.secondary_muscle = "No Secondary Muscle"`.

---

### 4. `NameError` sur `data` pour les fichiers XML et CSV

**Fichier :** `main.py`, lignes 26–37

```python
data : pandas.DataFrame        # annotation de type seulement — pas une assignation
match GetFileType(...):
    case "xml": print()        # data jamais assigné
    case "csv": print()        # data jamais assigné
    case "json": data = ...
if data is not None:           # NameError si xml ou csv
    sendToTable(data, ...)
```

`data : pandas.DataFrame` n'est **pas** une initialisation en Python. Il faut écrire
`data: pandas.DataFrame = None` avant le `match`.

---

### 5. Crash potentiel dans `sendFoodToDb` si colonne absente

**Fichier :** `handlers/dbHandler.py`, lignes 245–259

```python
sodium = row.get("sodium")
if sodium > 32767:             # TypeError si sodium est None
```

Si la colonne `sodium` est absente du fichier source, `row.get("sodium")` retourne
`None` (même après `fillna(0)` qui ne crée pas de colonnes), et la comparaison
`None > 32767` lève `TypeError`. Même problème pour `cholestorol`.

---

### 6. Mauvais message de log pour le sodium

**Fichier :** `handlers/dbHandler.py`, ligne 247

```python
WriteLog(file, food.name + " cholesterol is above smallint limit")
```

Ce message est déclenché par la vérification du champ **sodium**, pas du cholestérol.

---

### 7. Calcul de l'IMC (BMI) probablement faux

**Fichier :** `handlers/dbHandler.py`, ligne 95

```python
user.bmi = round(user.weight / (user.height**2), 2)
```

La colonne `height` est de type `Integer` (généralement stockée en cm). La formule
IMC est `poids(kg) / taille(m)²`. Si la taille est en cm, le résultat est ~10 000
fois trop petit. Il faut : `round(user.weight / (user.height / 100) ** 2, 2)`.

---

## 🟠 Sécurité

### 8. Credentials de base de données en clair dans le code

**Fichier :** `main.py`, ligne 14

```python
engine = create_engine('postgresql+psycopg2://postgres:azerty@localhost:5434/mspr')
```

Le mot de passe `azerty` est commité en clair. Utiliser une variable d'environnement
ou un fichier `.env` (exclu via `.gitignore`) :

```python
import os
engine = create_engine(os.environ["DATABASE_URL"])
```

---

## 🟡 Qualité du code

### 9. `difficulty_level` assigné deux fois dans `sendExerciseToDb`

**Fichier :** `handlers/dbHandler.py`, lignes 173 et 198

La même ligne apparaît deux fois avec 25 lignes d'écart. La première (ligne 173)
est inutile, à supprimer.

---

### 10. Message de log trompeur dans `sendExerciseToDb`

**Fichier :** `handlers/dbHandler.py`, ligne 183

```python
WriteLog(file, "file does not contain name_exercice attribute or name_exercice is misspelled.")
```

Ce message est déclenché par l'absence de `target_muscle`, pas de `name_exercice`.

---

### 11. `return ex` incohérent dans `sendExerciseToDb`

**Fichier :** `handlers/dbHandler.py`, ligne 216

```python
except Exception as ex:
    ...
    return ex   # les 3 autres fonctions ne retournent rien
```

La valeur retournée est ignorée par l'appelant. À supprimer pour cohérence.

---

### 12. Variable `index` jamais utilisée dans tous les `iterrows()`

**Fichier :** `handlers/dbHandler.py`, lignes 34, 163, 226, 289

```python
for index, row in data.iterrows():   # index non utilisé
```

Utiliser `for _, row in data.iterrows():` partout.

---

### 13. Fichier ouvert sans `with` dans `jsonHandler.py`

**Fichier :** `handlers/jsonHandler.py`, ligne 9

```python
data = json.load(open(os.path.join(TO_IMPORT_PATH, file)))  # jamais fermé
```

Utiliser un gestionnaire de contexte :
```python
with open(os.path.join(TO_IMPORT_PATH, file)) as f:
    data = json.load(f)
```

---

### 14. Grand bloc de code commenté dans `jsonHandler.py`

**Fichier :** `handlers/jsonHandler.py`, lignes 20–55

L'ancienne fonction `SendJsonToDb` est toujours présente sous forme de commentaire.
Ce code mort doit être supprimé (il est tracé par git si besoin).

---

### 15. `Main()` appelé sans garde `__main__`

**Fichier :** `main.py`, ligne 44

```python
Main()   # s'exécute à l'import du module
```

Remplacer par :
```python
if __name__ == "__main__":
    Main()
```

---

### 16. `if session:` toujours vrai

**Fichier :** `main.py`, ligne 40

`session` est toujours un objet SQLAlchemy initialisé à ce stade. Le `if` est inutile.

---

### 17. Chargement de tous les utilisateurs en mémoire

**Fichier :** `handlers/dbHandler.py`, lignes 281–286

```python
users = session.query(User.user_id, User.email).all()
```

Pour un grand volume d'utilisateurs, ce chargement complet en mémoire peut poser
des problèmes de performance. Préférer une sous-requête ou une recherche par email.

---

## 🔵 Style / Nommage

### 18. Fautes de frappe persistantes

| Présent dans le code | Correction     | Emplacement                                 |
|----------------------|----------------|---------------------------------------------|
| `succesful`          | `successful`   | 4 fonctions dans `dbHandler.py`             |
| `cholestorol`        | `cholesterol`  | `config.py` (colonne ORM) + `dbHandler.py` |
| `exersice_id`        | `exercise_id`  | `config.py` (clé primaire `Exercise`)       |

---

### 19. Nommage de classe non conforme PEP 8

**Fichier :** `config.py`, ligne 66

```python
class Health_metric(Base):   # snake_case
```

Utiliser `HealthMetric` (CamelCase comme les autres classes du fichier).

---

### 20. Indentation excessive dans `WriteLog`

**Fichier :** `fileManager.py`, lignes 33–37

Le corps de la fonction est indenté de 4 niveaux supplémentaires sans raison. Doit
être aligné au niveau normal d'une fonction.

---

## ⚪ Architecture / Structure

### 21. Imports dupliqués et inconsistants dans `config.py`

**Fichier :** `config.py`, ligne 2

Les deux variantes de plusieurs types sont importées mais utilisées en mélange :

| Importé deux fois | Versions                       |
|-------------------|--------------------------------|
| Numeric           | `NUMERIC` et `Numeric`         |
| Text              | `TEXT` et `Text`               |
| SmallInteger      | `SMALLINT` et `SmallInteger`   |

Choisir une seule forme par type et s'y tenir.

---

### 22. `csvHandler.py` et `xmlHandler.py` sont vides

Les fichiers existent mais ne contiennent aucun code. Le `match` dans `main.py`
appelle juste `print()` pour ces formats. À implémenter.

---

### 23. Pas de `requirements.txt` / `pyproject.toml`

Les dépendances (`sqlalchemy`, `pandas`, `python-magic`) ne sont pas déclarées.
Impossible de reproduire l'environnement.

---

### 24. `__pycache__/` présent dans le dépôt

Les fichiers `.pyc` compilés ne doivent pas être versionnés. Ajouter dans
`.gitignore` :
```
__pycache__/
*.pyc
```

---

## Tableau récapitulatif

| Sévérité        | Nb | Exemples principaux                                                                             |
|-----------------|----|-------------------------------------------------------------------------------------------------|
| 🔴 Bug          | 7  | `gender.lower()` crash, `equipment` écrasé, `secondary_muscle` non affecté, `NameError` `data`, crash `None > 32767`, mauvais log sodium, BMI en cm |
| 🟠 Sécurité     | 1  | Credentials en clair dans `main.py`                                                             |
| 🟡 Qualité      | 9  | Double assignation, mauvais message, `return ex`, `index` inutilisé, fichier non fermé, code mort, pas de garde `__main__`, `if session` inutile, users en RAM |
| 🔵 Style        | 3  | Fautes de frappe, nommage PEP 8, indentation excessive                                          |
| ⚪ Architecture  | 4  | Imports dupliqués, handlers vides, pas de `requirements.txt`, `.pyc` commités                   |
