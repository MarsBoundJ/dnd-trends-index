SELECT tag, COUNT(*) as tag_count 
FROM `silver_data.norm_catalog_supply`, UNNEST(tags) tag 
GROUP BY 1 
ORDER BY 2 DESC 
LIMIT 20;
