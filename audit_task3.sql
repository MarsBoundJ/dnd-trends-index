SELECT 
    COUNT(*) as gothic_count
FROM `silver_data.norm_catalog_supply`
WHERE 
    REGEXP_CONTAINS(LOWER(title), 'ravenloft|strahd|gothic')
    OR EXISTS (SELECT 1 FROM UNNEST(tags) t WHERE REGEXP_CONTAINS(LOWER(t), 'ravenloft|strahd|gothic'));
