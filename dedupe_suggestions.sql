CREATE OR REPLACE TABLE `dnd-trends-index.dnd_trends_raw.ai_suggestions` AS
SELECT concept_name, suggested_category, reason, status
FROM (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY concept_name) as row_num
  FROM `dnd-trends-index.dnd_trends_raw.ai_suggestions`
)
WHERE row_num = 1;
