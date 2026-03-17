-- Stage 3: Rule-based anomaly and risk detection
-- Event-level append-safe detection from cleaned stream.

INSERT INTO `heart-rate-risk-events`
SELECT
  user_id,
  event_id,
  start_time AS event_time,
  heart_rate,
  CASE
    WHEN heart_rate >= 130 THEN 'tachycardia'
    WHEN heart_rate <= 45 THEN 'bradycardia'
    ELSE 'normal'
  END AS anomaly_type,
  CASE
    WHEN heart_rate >= 145 OR heart_rate <= 40 THEN 'critical'
    WHEN heart_rate >= 130 OR heart_rate <= 45 THEN 'watch'
    ELSE 'stable'
  END AS risk_level,
  CAST(NULL AS TIMESTAMP_LTZ(3)) AS window_start,
  CAST(NULL AS TIMESTAMP_LTZ(3)) AS window_end,
  CURRENT_TIMESTAMP AS detected_at
FROM `heart-rate-cleaned`;
