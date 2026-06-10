---
sidebar_position: 2
sidebar_label: KeyValueStore Solver
title: KeyValueStore Solver walkthrough
---

# KeyValueStore Solver walkthrough

The [Getting Started](../getting_started.md) demo and the
[Reporting walkthrough](demo.md) both use the `DeltaSolver` against the full
silver-layer model — five tables including the narrow EAV `container_tags` and
`channel_tags`. This walkthrough instead drives the **`KeyValueStoreSolver`** on a
**reduced data model** that has *neither* tag table, and adds two metadata tables
the `DeltaSolver` never uses: a **`channel_mapping`** alias table and a
**`unit_conversion`** table.

Open `demos/key_value_store.ipynb`, fill in the three widgets, and run it top to
bottom. For background on how the two solvers differ, see the
[Query Engine reference](../references/query_engine.md).

---

## Prerequisites

- A **Databricks workspace** with **serverless compute** enabled.
- Permission to create a schema in a catalog you can write to.
- The Impulse repository cloned as a Git folder (see
  [Getting Started, Step 1](../getting_started.md)). Attach the notebook to a
  **Serverless** cluster running **Environment Version 2 or higher** (Python 3.12+).

This demo reuses the same pre-shaped Seat Leon data as the other demos
(`demos/data/reporting/`); no ingestion work is required.

---

## The reduced data model

The `KeyValueStoreSolver` does not read `channel_tags` at all, and treats
`container_tags` as optional. This demo loads only five tables and **never
configures `container_tags` or `channel_tags`**:

| Silver table       | Role in this demo                                                        |
|--------------------|--------------------------------------------------------------------------|
| `container_metrics`| wide container attributes (`vehicle_key`, timestamps, …)                 |
| `channel_metrics`  | wide channel metadata — **carries `channel_name` and `unit` as columns** |
| `channels`         | raw `(timestamp, value)` samples                                          |
| `channel_mapping`  | logical channel **aliases** → physical channels                          |
| `unit_conversion`  | per-unit scaling factors for automatic conversion                        |

Two consequences follow from dropping the EAV tag tables:

1. **Channel identity lives on `channel_metrics`.** Because there is no
   `channel_tags` table, the `channel_name` and `unit` columns are added directly
   to `channel_metrics`, keyed per `(container_id, channel_id)`. (Note that the
   same `channel_id` integer can refer to different signals in different
   containers, so these must be looked up per row — the demo data does exactly
   that.)
2. **Container filtering uses `MetricSelector`, not `TagSelector`.** With no
   `container_tags` table, filter on wide columns of `container_metrics`, e.g.
   `db.query.where(MetricSelector("vehicle_key") == "Seat_Leon")`. A `TagSelector`
   would need the EAV tag table this model omits.

---

## Channel aliasing and unit conversion

The `channel_mapping` table maps physical channels to logical **aliases** that you
query with `channel_with_alias(...)`. The demo defines:

| channel_alias  | source_channel       | priority | source_unit | target_unit |
|----------------|----------------------|----------|-------------|-------------|
| vehicle_speed  | Vehicle Speed Sensor | 1        | km/h        | m/s         |
| vehicle_speed  | veh_spd_ms           | 2        | m/s         | m/s         |
| engine_speed   | Engine RPM           | 1        | RPM         | RPM         |

The `unit_conversion` table supplies the scaling factors (base unit per family):

| group_id | unit | conversion_factor | is_base |
|----------|------|-------------------|---------|
| speed    | m/s  | 1.0               | true    |
| speed    | km/h | 0.277778          | false   |
| rotation | RPM  | 1.0               | true    |

When you select `channel_with_alias(channel_alias="vehicle_speed")`, the solver
resolves the alias, reads the matching physical channel, and rescales its values
to the `target_unit` using `source_factor / target_factor`. For
`Vehicle Speed Sensor` that is `0.277778 / 1.0` (km/h → m/s); for `engine_speed`
it is `1.0 / 1.0` (an identity mapping that changes nothing). **Conversion only
happens on aliased selectors** — a direct `db.query.channel(channel_name=...)`
read returns the raw stored values.

The solver matches the alias join on `source_channel ↔ channel_name`. The demo
configures this explicitly so no extra join column is required:

```python
"solver_config": {
    "channel_mapping": {
        "join_keys": [{"mapping_col": "source_channel", "metrics_col": "channel_name"}]
    }
}
```

The effective source unit is `COALESCE(channel_metrics.unit, channel_mapping.source_unit)`,
so the `unit` column on `channel_metrics` is authoritative and must agree with the
mapping's `source_unit`.

---

## Priority: resolving duplicate channels

Container 1 carries the vehicle-speed signal **twice** — as `Vehicle Speed Sensor`
(km/h) and as `veh_spd_ms` (the same signal already in m/s). Both physical channels
map to the `vehicle_speed` alias, so the solver must pick one. It keeps the row with
the **lowest `priority` number** (ties broken with nulls last), so container 1
resolves to `Vehicle Speed Sensor` (priority 1) and converts km/h → m/s. Containers
2 and 3 only have `Vehicle Speed Sensor`, so no tie-break is needed.

Because `veh_spd_ms` is the same physical signal already expressed in m/s, the alias
yields one consistent m/s result regardless of which channel priority selects — the
mapping + conversion together give you a single canonical logical channel.

---

## Reading the output

The notebook builds three duration histograms on one page:

- `speed_distribution_ms` — `vehicle_speed` alias, binned in **m/s**.
- `speed_distribution_kmh` — the same physical channel read directly in raw **km/h**.
- `engine_speed_when_fast` — `engine_speed` alias (RPM identity), gated by an event
  defined in converted units (`vehicle_speed > 13.9 m/s`, i.e. ~50 km/h).

The final cell joins the gold-layer `histogram_fact` table to `histogram_dimension`
on `visual_id` to label each histogram by name. The `speed_distribution_ms`
histogram occupies roughly **0.278×** the axis range of `speed_distribution_kmh` —
direct visual proof that the alias applied the km/h → m/s conversion while the direct
read did not.

---

## Where to next

- **[Query Engine reference](../references/query_engine.md)** — when to choose each
  solver and the exact table requirements.
- **[Reporting walkthrough](demo.md)** — multiple events, 2D histograms, and
  statistics aggregators with the `DeltaSolver`.
- **[Configuration reference](../config/configuration.md)** — every config field,
  including `solver_config` column mappings and alias join keys.
