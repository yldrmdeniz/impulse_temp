# Migration Guide: Unit Conversion & Stats Validation → impulse_temp

> **Source branch**: `feature/unit_conversion_update` in `edp-products`  
> **Target repo**: `impulse_temp` (fork of `mda_framework` with renamed packages)  
> **Base commit**: `cf63a837` (fix filesize read out)

---

## Summary of Changes

This branch implements:
1. **Unit conversion factor fix** — reversed formula from `src/tgt` to `tgt/src`
2. **Case 6 exclusion** — channels with `target_unit` set but NO `source_unit` anywhere are excluded from processing and persistence
3. **Effective unit enrichment** — resolved unit is written into fact tables (stats, histogram, histogram2d)
4. **Skipped channels tracking** — excluded channels are persisted as metadata in the dimension table
5. **StatsAggregator validation** — raises `ValueError` on unsupported statistic types at init time
6. **Schema additions** — `unit` (StringType, nullable) column added to all 3 fact schemas

---

## Files Changed (12 total)

| # | File (relative to `shared_components/src/mda_framework/`) | Change Type |
|---|---|---|
| 1 | `setup.py` | NEW — minimal setuptools setup |
| 2 | `src/mda_query_engine/analyze/query/aggregations/stats_aggregator.py` | MODIFIED — validation |
| 3 | `src/mda_query_engine/analyze/query/solvers/basic_narrow_solver.py` | MODIFIED — core logic |
| 4 | `src/mda_query_engine/unit_conv.md` | NEW — documentation |
| 5 | `src/mda_reporting/aggregations/histogram.py` | MODIFIED — add unit column |
| 6 | `src/mda_reporting/aggregations/histogram2d.py` | MODIFIED — add unit column |
| 7 | `src/mda_reporting/aggregations/stats_aggregator.py` | MODIFIED — add unit column |
| 8 | `src/mda_reporting/core/report.py` | MODIFIED — enrichment + skipped channels |
| 9 | `src/mda_reporting/persist/fact_schema.py` | MODIFIED — schema addition |
| 10 | `src/mda_reporting/persist/report_storage.py` | MODIFIED — formatting only |
| 11 | `tests/integration/unit_conversion_e2e_test.ipynb` | NEW — integration notebook |
| 12 | `tests/unit/.../solvers/key_value_store_alias_test.py` | MODIFIED — 3 new tests |

---

## Detailed Change Descriptions

### 1. `stats_aggregator.py` (Query Engine — NOT Reporting)

**Path**: `src/mda_query_engine/analyze/query/aggregations/stats_aggregator.py`

**What**: Added validation in `__init__` after `self.statistics = statistics`:

```python
# Validate statistics
supported = NUMERIC_STATISTICS | set(STRING_STATISTICS) | {"start", "end"}
invalid = [s for s in statistics if s not in supported]
if invalid:
    raise ValueError(
        f"Unsupported statistic type(s): {invalid}. "
        f"Available options are: {sorted(supported)}."
    )
```

**⚠️ Watch out**: In `impulse_temp`, check if `NUMERIC_STATISTICS` and `STRING_STATISTICS` constants exist with the same names. They should be defined at module level.

---

### 2. `basic_narrow_solver.py` (Core Solver Logic)

**Path**: `src/mda_query_engine/analyze/query/solvers/basic_narrow_solver.py`

#### 2a. New instance attributes in `__init__`:
```python
self.skipped_channels_df: DataFrame | None = None
self.channel_units: dict[int, str | None] = {}
```

#### 2b. Conversion factor formula FIX (critical):
```python
# OLD (WRONG):
F.col("_src_factor") / F.col("_tgt_factor")

# NEW (CORRECT):
F.col("_tgt_factor") / F.col("_src_factor")
```

**Reasoning**: Factors in the unit_conversion table are relative to the base unit (where `is_base=True`, `factor=1`). To convert source→target: multiply by `target_factor / source_factor`.

#### 2c. Case 6 exclusion in `solve()` method (before `_compute_conversion_factors`):
```python
case6_mask = F.col(source_unit_col).isNull() & F.col(target_unit_col).isNotNull()
skipped_df = channels_df.where(case6_mask).select(
    self.config.container_id_col,
    self.config.channel_id_col,
    target_unit_col,
)
skipped_count = skipped_df.count()
if skipped_count > 0:
    self.skipped_channels_df = skipped_df
channels_df = channels_df.where(~case6_mask)
```

#### 2d. Effective unit computation (after `_compute_conversion_factors`, before dropping unit cols):
```python
_eff_unit_expr = (
    F.when(F.col(target_unit_col).isNotNull(), F.col(target_unit_col))
    .when(
        F.col(source_unit_col).isNotNull() & (F.col(source_unit_col) != F.lit("-")),
        F.col(source_unit_col),
    )
    .otherwise(F.lit(None))
)

if "selector_ids" in channels_df.columns:
    unit_rows = (
        channels_df.withColumn("_eff_unit", _eff_unit_expr)
        .select(F.explode("selector_ids").alias("_sid"), "_eff_unit")
        .groupBy("_sid")
        .agg(F.first("_eff_unit", ignorenulls=True).alias("_unit"))
        .collect()
    )
    self.channel_units = {row["_sid"]: row["_unit"] for row in unit_rows}
```

**⚠️ Watch out**: 
- `source_unit_col` and `target_unit_col` come from `self.config` (SolverConfig) — verify these property names in impulse_temp
- The `selector_ids` column is an array column created during aliased channel resolution
- `self.config.container_id_col` / `channel_id_col` / `target_unit_col` / `source_unit_col` — verify naming in impulse_temp's SolverConfig

---

### 3. `fact_schema.py` (Schema Changes)

**Path**: `src/mda_reporting/persist/fact_schema.py`

Added `StructField("unit", StringType(), True)` as the **last field** in ALL THREE schemas:
- `HISTOGRAM_FACT_SCHEMA`
- `HISTOGRAM2D_FACT_SCHEMA`
- `STATS_AGGREGATOR_FACT_SCHEMA`

**⚠️ Critical**: Must be added BEFORE the closing `]` of each schema. Position matters because `.select(SCHEMA.fieldNames())` is used downstream.

---

### 4. Aggregation Classes (Add placeholder unit column)

Each aggregation's `determine_aggregations()` must add `.withColumn("unit", f.lit(None).cast("string"))` BEFORE the final `.select(SCHEMA.fieldNames())`:

- **histogram.py**: After `.withColumn("visual_id", ...)`, add `.withColumn("unit", f.lit(None).cast("string"))`
- **histogram2d.py**: After `.transform(Histogram2D._add_visual_id_column(...))`, add `.withColumn("unit", f.lit(None).cast("string"))`
- **stats_aggregator.py**: After `.transform(StatsAggregator._add_visual_id_column(...))`, add `.withColumn("unit", f.lit(None).cast(StringType()))`

**Note**: StatsAggregator uses `StringType()` explicitly (imported from pyspark.sql.types), histogram/histogram2d use `"string"` literal. Both work, but keep consistent with impulse_temp's style.

---

### 5. `report.py` (Enrichment Logic — LARGEST CHANGE)

**Path**: `src/mda_reporting/core/report.py`

#### 5a. New import at top:
```python
import pyspark.sql.functions as F
```

#### 5b. New attribute in `__init__`:
```python
self.skipped_channels_df = None
```

#### 5c. In `persist_results()` — both full and incremental paths:
Replace direct `writer.write(self.container_dimension_df, ...)` with:
```python
dim_df = self._enrich_dimension_with_skipped_channels(self.container_dimension_df)
writer.write(dim_df, uri=uri)
```

For incremental path (where `transformer.add_meta_information` is called):
```python
dim_df = self._enrich_dimension_with_skipped_channels(self.container_dimension_df)
df_enriched = dim_df.transform(transformer.add_meta_information)
self.sink.upsert(df_enriched, uri, ["container_id"])
```

#### 5d. New method `_enrich_dimension_with_skipped_channels`:
Adds a `skipped_channels` column (JSON string) to the dimension DataFrame by joining the solver's excluded channels info. Returns dimension_df with the new column (null if no channels were skipped).

#### 5e. New method `_enrich_facts_with_unit`:
Iterates over pages → aggregations, builds `channel_name → unit` and `visual_id → unit` mappings from `self.solver.channel_units`, then overwrites the placeholder `unit` column in `self.aggregation_dfs[...]`.

Handles both plain DataFrames and `dict` format (`{"changed": df, "unchanged": df}`) for incremental mode.

#### 5f. Call sites in `determine_report()`:
After the solve + aggregation pipeline completes, add:
```python
# Collect skipped channels from solver (Case 6)
self.skipped_channels_df = getattr(self.solver, "skipped_channels_df", None)

# Enrich fact DataFrames with the effective unit column
self._enrich_facts_with_unit()
```

**⚠️ Watch out**: 
- The enrichment uses `agg.channel_names`, `agg.input_expressions`, `agg.base_expr`, `agg.get_id()` — verify these attribute/method names exist in impulse_temp's aggregation classes
- `expr.get_selectors()` returns list of `TimeSeriesSelector` objects with `.selector_id` attribute
- `AggregationType.STATS_AGGREGATOR.name`, `AggregationType.HISTOGRAM.name`, `AggregationType.HISTOGRAM2D.name` — verify enum exists

---

### 6. `report_storage.py` (Minor)

Only a formatting change (wrapping a long f-string line). No functional change. Skip in migration.

---

## Key Naming Differences to Watch (mda_framework → impulse_temp)

| Concept | mda_framework name | Likely impulse_temp name | Verify |
|---|---|---|---|
| Package root | `mda_query_engine` | Check if renamed (e.g., `impulse_query_engine`) | ✅ |
| Package root | `mda_reporting` | Check if renamed (e.g., `impulse_reporting`) | ✅ |
| Solver config class | `SolverConfig` | Same? | ✅ |
| Config attributes | `source_unit_col`, `target_unit_col`, `container_id_col`, `channel_id_col` | Same? | ✅ |
| BasicNarrowSolver | `BasicNarrowSolver` | Same? | ✅ |
| KeyValueStoreSolver | `KeyValueStoreSolver` | Same? | ✅ |
| Report class | `Report` | Same? | ✅ |
| AggregationType enum | `AggregationType` | Same? | ✅ |
| Fact schema constants | `STATS_AGGREGATOR_FACT_SCHEMA`, `HISTOGRAM_FACT_SCHEMA`, `HISTOGRAM2D_FACT_SCHEMA` | Same? | ✅ |

---

## Migration Checklist

- [ ] **Verify package names**: Check all `from mda_query_engine...` and `from mda_reporting...` import paths
- [ ] **Check SolverConfig properties**: Confirm `source_unit_col`, `target_unit_col`, `container_id_col`, `channel_id_col` exist
- [ ] **Apply conversion factor fix** in `basic_narrow_solver.py` → `_compute_conversion_factors()`
- [ ] **Add instance attributes** to `BasicNarrowSolver.__init__`: `skipped_channels_df`, `channel_units`
- [ ] **Add Case 6 exclusion** in `BasicNarrowSolver.solve()` before `_compute_conversion_factors()`
- [ ] **Add effective unit computation** in `BasicNarrowSolver.solve()` after `_compute_conversion_factors()`
- [ ] **Add `unit` field** to all 3 fact schemas in `fact_schema.py`
- [ ] **Add `.withColumn("unit", ...)`** in histogram.py, histogram2d.py, stats_aggregator.py
- [ ] **Add validation** in `StatsAggregator.__init__` (query engine side)
- [ ] **Add `import pyspark.sql.functions as F`** to report.py
- [ ] **Add `self.skipped_channels_df = None`** to Report.__init__
- [ ] **Add `_enrich_dimension_with_skipped_channels()`** method to Report
- [ ] **Add `_enrich_facts_with_unit()`** method to Report
- [ ] **Add `_apply_unit_by_channel_name()`** helper method to Report
- [ ] **Add `_apply_unit_by_visual_id()`** helper method to Report
- [ ] **Update both persist paths** (full + incremental) to use enriched dimension df
- [ ] **Add call sites** in `determine_report()` for skipped channels + enrichment
- [ ] **Port unit tests** (3 new tests in `key_value_store_alias_test.py`)
- [ ] **Port integration notebook** if applicable
- [ ] **Run existing tests** to ensure nothing breaks

---

## Order of Implementation (Recommended)

1. **Schema first** — Add `unit` field to fact schemas (breaks nothing, just adds a nullable column)
2. **Aggregation classes** — Add `.withColumn("unit", ...)` placeholder (ensures schema alignment)
3. **Solver fixes** — Conversion factor formula + new attributes + Case 6 + effective unit
4. **Report enrichment** — All the Report class changes (depends on solver attributes)
5. **Validation** — StatsAggregator init validation (independent, can be done anytime)
6. **Tests** — Port and adapt tests last

---

## Potential Pitfalls

1. **Import paths**: Every `from mda_query_engine...` and `from mda_reporting...` must use impulse_temp's renamed package
2. **The conversion factor formula**: Double-check by looking at the unit_conversion table — `is_base=True` row has `factor=1.0`. The formula is `target_factor / source_factor`
3. **`selector_ids` column**: Only exists when channel aliasing is used. The `if "selector_ids" in channels_df.columns` guard handles this
4. **Incremental mode dict format**: `aggregation_dfs[key]` can be either a DataFrame OR a dict `{"changed": df, "unchanged": df}`. Both cases must be handled in `_apply_unit_by_*` methods
5. **StringType import**: The stats_aggregator (reporting side) uses `from pyspark.sql.types import StringType` for `.cast(StringType())` — make sure this import exists
6. **`F.broadcast`**: Used in `_enrich_dimension_with_skipped_channels` for the small skipped_agg DataFrame join — ensure `pyspark.sql.functions` is imported as `F` in report.py
7. **Test fixtures**: The unit tests rely on `setup_key_value_store_alias_db` fixture and CSV test data with `unit_conversion` and `channel_mapping` tables — ensure these exist in impulse_temp's test data

