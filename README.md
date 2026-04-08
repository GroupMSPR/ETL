# 🧠 HealthAiCoach ETL

Pipeline ETL pour l’import et le traitement automatisé des données de santé dans le système HealthAiCoach.

---

## 📦 Formats de fichiers supportés

Déposez vos fichiers dans les dossiers appropriés en respectant l’un des formats suivants :

- `CSV`
- `XLSX`
- `JSON`
  - Doit être :
    - une liste  
    - ou un objet contenant une liste appelée `data`

---

## 🗂️ Organisation des dossiers

Chaque dossier correspond à une table en base de données et **doit être préfixé par un chiffre** :

| Préfixe | Nom de la table           |
|--------|----------------------------|
| 1      | User                       |
| 2      | Exercise                   |
| 3      | Food                       |
| 4      | Health Metric              |
| 5      | Consume (`user_food`)      |
| 6      | Practice (`user_exercise`) |

---

## ⚙️ Logique de traitement

### ✅ Import réussi
- Le fichier est déplacé dans le dossier `Archive/`  
- Le nom du fichier est enrichi avec la **date et l’heure d’import**

### ❌ Erreur lors de l’import
- Le fichier est déplacé dans le dossier `Error/`  
- Un fichier de log est généré dans le dossier `Log/` avec le détail de l’erreur  

---

## 📝 Remarques

- Vérifiez le format des fichiers avant import  
- Les fichiers JSON doivent respecter strictement la structure attendue  
- Consultez les logs pour analyser les erreurs  

---

## 🚀 Résumé rapide

1. Déposer les fichiers dans le bon dossier  
2. Le ETL les traite automatiquement  
3. Vérifier :
   - `Archive/` → imports réussis  
   - `Error/` → fichiers en erreur  
   - `Log/` → détails des erreurs  

---
