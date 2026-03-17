# Flink Processing Pipeline (Phase 2)

This folder contains the Flink SQL implementation for:
1. Cleaning + deduplication
2. Windowed feature generation
3. Rule-based anomaly detection

## Files
- `sql/00_tables.sql` — source/sink table definitions
- `sql/10_cleaning.sql` — data cleaning and dedup
- `sql/20_window_features.sql` — 1-minute aggregate features
- `sql/30_anomaly_detection.sql` — anomaly/risk event generation

## Execution Order
Run scripts in this order from your Flink SQL workspace:
1. `00_tables.sql`
2. `10_cleaning.sql`
3. `20_window_features.sql`
4. `30_anomaly_detection.sql`

## Before Running
Update placeholders in `sql/00_tables.sql`:
- `<BOOTSTRAP_SERVERS>`
- `<KAFKA_API_KEY>`
- `<KAFKA_API_SECRET>`
- `<SCHEMA_REGISTRY_URL>`
- `<SR_API_KEY>`
- `<SR_API_SECRET>`

## Notes
- Cleaning stage enforces valid ranges and deduplicates by `event_id`.
- Windowing stage computes per-user 1-minute metrics for downstream summary logic.
- Anomaly stage emits `normal`, `tachycardia`, `bradycardia`, `high_variability`, and `sustained_elevation` labels with `stable/watch/critical` risk.
