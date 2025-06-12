# TUI‐Display Test Matrix
“**Direct vs Agent**” contract coverage for every current command
Last updated: 2025-06-12

---

## 0 How to read this table

| Col | Meaning |
|-----|---------|
| Command(s) | Slash aliases (`/foo`) **and** agent tool name (`foo`) |
| Display method | Private function in `ChuckTUI` that renders the full view |
| Key list | Dict key that holds the row list passed to `display_table` |
| Highlight field | Name used to highlight the “current/active” row (blank = none) |
| Custom agent handler? | Registered in `agent_full_display_handlers`? |
| Tests required | Which of the **four canonical tests** (see §1) should exist |
| ✅ | Test already in repository |
| ❌ | Missing test – please add |

---

## 1 Canonical tests (per command)

| # | Purpose | Function Skeleton |
|---|---------|-------------------|
| T1 | Data-contract (columns + headers + title + data) | `test_<obj>_display_data_contract` |
| T2 | Slash path ➜ full display | `test_slash_<obj>_calls_full` |
| T3 | Agent path ➜ condensed (no `display=True`) | `test_agent_<obj>_condensed_default` |
| T4 | Agent path ➜ full when `display=True` | `test_agent_<obj>_full_when_requested` |
| T5 | Highlight logic (optional) | `test_<obj>_highlighting` |

---

## 2 Current command coverage matrix

| Command(s) | Display method | Key list | Highlight field | Custom agent handler? | T1 | T2 | T3 | T4 | T5 |
|------------|----------------|----------|-----------------|-----------------------|----|----|----|----|----|
| list-catalogs /catalogs | `_display_catalogs` | `catalogs` | `current_catalog` | No | ✅ `test_catalog_display_data_contract` | ✅ | ✅ | ✅ | ✅ |
| list-schemas /schemas | `_display_schemas` | `schemas` | `current_schema` | No | ✅ | ✅ | (N/A, not used by agent yet) | (N/A) | ✅ |
| list-tables /tables | `_display_tables` | `tables` | — | No | ✅ `test_tables_display_data_contract` | ✅ | ✅ | ✅ | ❌ |
| list-models /models | `_display_models_consolidated` | `models` | `active_model` | No | ✅ `test_models_display_data_contract` | ✅ | ✅ | ✅ | ✅ |
| list-warehouses /warehouses | `_display_warehouses` | `warehouses` | `current_warehouse_id` | No | ✅ `test_warehouses_display_data_contract` | ✅ | ✅ | ✅ | ✅ (row colour for status) |
| list-volumes /volumes | `_display_volumes` | `volumes` | — | No | ✅ `test_volumes_display_data_contract` | ✅ | ✅ | ✅ | ❌ |
| status | `_display_status` (slash) / `_display_status_for_agent` (agent) | permissions / status_items | — | **Yes** (`status`) | ✅ | ✅ | ✅ | N/A (agent always custom) | N/A |

Legend:
• “N/A” = behaviour not applicable (e.g., command never used by agent).
• Tests marked ❌ are still missing.

---

## 3 Step-by-step instructions **for each missing ❌ cell**

1. Clone or open `tests/unit/ui/test_tui_<object>_display.py` (create if absent).
2. Copy the template from the *Playbook* (previous message). Fill in:
   * `<object>` → tables, models, warehouses, volumes…
   * `<tool-name>` → list-tables, list-models, etc.
   * `<list-key>` → see Key list column.
   * Columns / headers / title → read `_display_<object>` implementation.
3. Ensure fixtures & helper (`register_temp_cmd`) are present (only once per file).
4. For highlight test (T5) identify the `style_map` key (often `"name"`).
5. Run only your tests:

```bash
pytest tests/unit/ui/test_tui_<object>_display.py
```

6. All green?  Add/commit:

```
git add tests/unit/ui/test_tui_<object>_display.py
git commit -m "test: full display and agent routing for <object> command"
```

7. Update the matrix above (README or this doc) replacing ❌ → ✅.

---

## 4 Global checklist before PR

- [ ] Every **Key list** in table has at least T1 & T2 tests.
- [ ] Commands exposed to agents have T3 & T4.
- [ ] Highlight-row logic verified where applicable (T5).
- [ ] No `print()` left in tests; use mocks.
- [ ] New tests pass with `pytest -q`.
- [ ] CI green.

---

## 5 Handy snippets

### 5.1 Capture `display_table` arguments

```python
captured = []
def spy(**kw):
    captured.append(kw)
    raise PaginationCancelled()
with patch("chuck_data.ui.table_formatter.display_table", side_effect=spy):
    ...
```

### 5.2 Patch condensed method

```python
orig = tui._display_condensed_tool_output
tui._display_condensed_tool_output = lambda n,d: called.append((n,d))
...
tui._display_condensed_tool_output = orig
```

---

Keeping this table current ensures we never regress the TUI experience while we add features or refactor the UI layer.

---

## 6 Command inventory & display classification (auto-generated guide)

| Slash / Tool | Visual output in TUI | How rendered | Canonical table tests? |
|--------------|---------------------|--------------|------------------------|
| /agent, /ask | Agent conversation  | Rich Panel (response) + condensed lines | N/A |
| /auth | Success/error Panel | Panel | N/A |
| /bug | Text panel | Panel | N/A |
| /bulk-tag-pii | Progress + condensed line | Console line | N/A (no full view) |
| /catalog (tool `catalog`) | Catalog detail | `_display_catalog_details` | T1 only |
| /catalogs (`list-catalogs`) | Catalog table | `_display_catalogs` | T1-T4 (+T5) |
| /catalog-selection (`set-catalog`) | Interactive wizard | Prompts | N/A |
| /create-volume | Text | Message | N/A |
| /create-warehouse | Text | Message | N/A |
| /discord | Invitation Panel | Panel | N/A |
| /examples, /getting-started, /tips | Help Panel | Panel | N/A |
| /help, /support, /help-me | Help/support Panel | Panel | N/A |
| /job-status (`job_status`) | Single-line status | Console line | N/A |
| /jobs (launch/status) | Progress lines | Console lines | N/A |
| /list-models | Models table | `_display_models_consolidated` | T1-T4 (+T5) |
| /list-schemas | Schemas table | `_display_schemas` | T1-T2 (+T5) |
| /list-tables | Tables table | `_display_tables` | T1-T4 |
| /list-volumes | Volumes table | `_display_volumes` | T1-T4 |
| /list-warehouses | Warehouses table | `_display_warehouses` | T1-T4 (+T5) |
| /model-selection (`set-model`) | Interactive | Wizard | N/A |
| /pii-scan (`scan-pii`) | PII results | `_display_pii_scan_results` | T1 only |
| /run-sql, /sql | Result table / pagination | `_display_sql_results` | T1 only |
| /schema (`schema`) | Schema detail | `_display_schema_details` | T1 only |
| /schemas (`list-schemas`) | Schemas table | `_display_schemas` | see above |
| /schema-selection (`set-schema`) | Interactive | Wizard | N/A |
| /setup (wizard) | Interactive screens | Wizard | N/A |
| /setup-stitch (`add-stitch-report`) | Text panel | Panel | N/A |
| /status | Status table | `_display_status` | T1-T3 |
| /table (`show_table`) | Table detail | `_display_table_details` | T1 only |
| /tag-pii | Progress + condensed line | Console line | N/A |
| /upload-file | Condensed line + success Panel | Mixed | N/A |
| /warehouse (`warehouse`) | Text | Message | N/A |
| /warehouse-selection (`set-warehouse`) | Interactive | Wizard | N/A |

This appendix lists **every** command registered in `chuck_data/commands`. Only
those whose output is a multi-row table (or highlight logic) require the 4
canonical tests.

