-- Stage 2: Windowed feature generation (1-minute tumbling windows)

INSERT INTO `heart-rate-features`
SELECT
  user_id,
  window_start,
  window_end,
  COUNT(*) AS sample_count,
  AVG(CAST(heart_rate AS DOUBLE)) AS avg_hr,
  MIN(CAST(heart_rate AS DOUBLE)) AS min_hr,
  MAX(CAST(heart_rate AS DOUBLE)) AS max_hr,
  STDDEV_POP(CAST(heart_rate AS DOUBLE)) AS hr_stddev,
  MAX(CAST(heart_rate AS DOUBLE)) - MIN(CAST(heart_rate AS DOUBLE)) AS hr_range,
  SUM(CASE WHEN heart_rate >= 100 THEN 1 ELSE 0 END) AS elevated_count
FROM TABLE(
  TUMBLE(TABLE `heart-rate-cleaned`, DESCRIPTOR(start_time), INTERVAL '1' MINUTE)
)
GROUP BY user_id, window_start, window_end;
