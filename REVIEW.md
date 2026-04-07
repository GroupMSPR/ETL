# Code Review — ETL Pull Request #1

## Overview

The PR introduces the main ETL architecture: database connection, file routing,
JSON-to-DataFrame conversion, and four DB insert functions (User, Food, Exercise,
HealthMetric). The structure is reasonable but has several bugs, security issues,
code quality problems, and incomplete pieces.

---

## 🔴 Bugs / Critical Issues

### `handlers/dbHandler.py`

### 1. Crash on `gender` access before guard check

```python
gender = row.get("gender").lower()        # ← called BEFORE checking "gender" in row
if "gender" in row and gender in [...]:
```

If `"gender"` is missing or `fillna(0)` replaced it with `0`, calling `.lower()` on
`0` raises `AttributeError`. The same pattern occurs with `physicalActivityLevel` and
`subscription`. The attribute existence check must come **before** the `.get()` call.

### 2. `equipment` always overwritten

```python
if (isinstance(equipment, list)):
    exercise.equipment = ", ".join(...)   # ← set correctly for list
exercise.equipment = equipment            # ← unconditionally overwrites with raw value
```

The list-join result is immediately overwritten. The last line should be in an `else`
branch.

### 3. `exercise.secondary_muscle` may remain unset

The column is `nullable=False` in the ORM model, but there is no `else` branch
assigning a fallback — the DB will reject the insert.

### 4. `NameError` risk in `main.py`

```python
data: pandas.DataFrame          # declaration only — no assignment
match GetFileType(...):
    case "xml": print()         # data is never assigned
    case "csv": print()         # data is never assigned
    case "json": data = ...
if data is not None:            # ← NameError on first xml/csv file
    sendToTable(data, ...)
```

Assign `data = None` before the `match` block.

---

## 🟠 Security Issues

### `main.py` — Hardcoded credentials

```python
engine = create_engine("postgresql+psycopg2://user:password@localhost:5434/mspr")
```

The database password is hardcoded in source code. Use environment variables
(e.g., `os.environ.get("DB_URL")`) and a `.env` file excluded from version control.

---

## 🟡 Code Quality / Logic Issues

### 5. Duplicate assignment in `sendExerciseToDb`

`exercise.difficulty_level` is assigned twice. The first assignment is redundant.

### 6. Wrong error message

The log message says `"name_exercice"` (misspelled) but the validation was for
`target_muscle`.

### 7. Inconsistent return in `sendExerciseToDb`

`return ex` in the `except` block is inconsistent — other functions do not return
the exception.

### 8. `index` variable unused in all `iterrows()` loops

Use `for _, row in data.iterrows():` throughout.

### 9. File handle never closed in `jsonHandler.py`

```python
data = json.load(open(os.path.join(TO_IMPORT_PATH, file)))  # never closed
```

Use a `with` statement.

### 10. Large commented-out code block in `jsonHandler.py`

Dead code should be removed.

### 11. `Main()` called at module level without guard

```python
Main()  # should be: if __name__ == "__main__": Main()
```

### 12. Unnecessary `if session:` guard in `main.py`

`session` is always initialized at that point.

---

## 🔵 Style / Naming Issues

### 13. Typos throughout

- `succesful` → `successful` (all 4 functions)
- `cholestorol` → `cholesterol` (`config.py` column, `dbHandler.py`)
- `exersice_id` → `exercise_id` (`config.py`)

### 14. Class naming

`Health_metric` should follow PEP 8 → `HealthMetric`.

### 15. Excessive indentation in `WriteLog` (`fileManager.py`)

The function body is indented with 4 extra levels for no reason.

### 16. Missing newline at end of files

Missing in `config.py`, `fileManager.py`, `handlers/dbHandler.py`, and `main.py`.

---

## ⚪ Structural / Architecture Notes

### 17. `__pycache__/` committed

Binary `.pyc` files should not be committed. Add `__pycache__/` and `*.pyc` to
`.gitignore`.

### 18. CSV and XML handlers are stubs

`case "csv": print()`, `case "xml": print()` — fine for WIP but should be tracked.

### 19. `config.py` duplicate imports

Both `NUMERIC`/`Numeric`, `TEXT`/`Text`, `SMALLINT`/`SmallInteger` are imported but
mixed inconsistently. Standardize to one style.

### 20. No `requirements.txt` / `pyproject.toml`

Dependencies (`sqlalchemy`, `pandas`, `python-magic`) are not declared.

---

## Summary Table

| Severity       | Count | Examples                                                                     |
|----------------|-------|------------------------------------------------------------------------------|
| 🔴 Bug         | 4     | `gender.lower()` crash, `equipment` overwrite, unset `secondary_muscle`, `data` NameError |
| 🟠 Security    | 1     | Hardcoded DB credentials                                                     |
| 🟡 Quality     | 8     | Unclosed file handle, dead code, unused vars, wrong log message              |
| 🔵 Style       | 4     | Typos, class naming, indentation                                             |
| ⚪ Architecture | 4     | `.pyc` in repo, no deps file, stub handlers, mixed imports                   |
