# 🧠 HealthAiCoach ETL

ETL pipeline for importing and processing health-related data files into the HealthAiCoach system.

---

## 📦 Supported File Formats

Place your files in the appropriate folders using one of the following formats:

- `CSV`
- `XLSX`
- `JSON`
  - Must be either:
    - a list  
    - or an object containing a list named `data`

---

## 🗂️ Folder Structure

Each folder represents a database table and **must be prefixed with a number**:

| Prefix | Table Name                |
|--------|--------------------------|
| 1      | User                     |
| 2      | Exercise                 |
| 3      | Food                     |
| 4      | Health Metric            |
| 5      | Consume (`user_food`)    |
| 6      | Practice (`user_exercise`)|

---

## ⚙️ Processing Logic

### ✅ Successful Import
- File is moved to the `Archive/` folder  
- Filename is updated with the **import date and time**

### ❌ Import Error
- File is moved to the `Error/` folder  
- A log file is generated in the `Log/` folder with error details  

---

## 📝 Notes

- Ensure files are correctly formatted before import  
- JSON files must strictly follow the expected structure  
- Logs can be used for debugging failed imports  

---

## 🚀 Quick Summary

1. Drop files into the correct folder  
2. ETL processes them automatically  
3. Check:
   - `Archive/` → success  
   - `Error/` → failed files  
   - `Log/` → error details  

---
