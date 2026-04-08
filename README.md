# 🧠 HealthAiCoach ETL – Déploiement et Configuration

Cette version du pipeline ETL permet l’import et le traitement automatisé des données de santé, avec intégration Google Drive pour la récupération automatique des fichiers.

---

## 📥 Déploiement

1. **Cloner le projet** :
   git clone https://github.com/GroupMSPR/ETL.git
   cd HealthAiCoach

2. **Installer les dépendances** :
   pip install -r requirements.txt

3. **Créer l’arborescence des dossiers dans Google Drive** :
   ETL/
     ├─ Log/
     ├─ Archive/
     ├─ ToImport/
     └─ Error/

4. **Mettre en place le fichier `.env`**
   - Copier `.env.example` en `.env`
   - Remplir les variables avec vos informations (voir .env.exemple)

> Optionnel : vous pouvez convertir le token en Base64 et le mettre dans `GOOGLE_TOKEN_PICKLE`.

---

## 🔑 Configuration Google Drive

1. Aller sur https://console.cloud.google.com/apis/credentials
2. Créer un **OAuth 2.0 Client ID**
3. Ajouter un **Client Secret** et télécharger le fichier JSON
4. Renommer ce fichier en `credentials.json` et le placer à la racine du projet

---

## ⚙️ Initialisation du service Google Drive

1. Décommenter le code dans la fonction `get_drive_service` de `driveHelper.py`
2. Lancer le projet. Une fenêtre Google s’ouvrira pour connecter votre compte
   - Cela générera automatiquement un fichier `token.pickle` pour authentifier les futures connexions.
3. Vous pouvez laisser le code décommenté ou le remettre commenté selon que vous souhaitez réautoriser l’accès si le token expire.

---

## 📂 Logique ETL

- Les fichiers à traiter doivent être déposés dans `ETL/ToImport/`
- Après traitement :
  - ✅ `ETL/Archive/` → fichiers importés avec succès (nom enrichi avec date/heure)
  - ❌ `ETL/Error/` → fichiers ayant rencontré une erreur
  - `ETL/Log/` → détails des erreurs
