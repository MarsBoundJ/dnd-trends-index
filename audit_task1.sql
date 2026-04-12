SELECT 
    source, 
    COUNT(*) as total_rows, 
    COUNTIF(ARRAY_LENGTH(tags) > 0) as tagged_rows,
    COUNTIF(seller_tier IS NOT NULL AND seller_tier != 'Normal') as high_tier_rows
FROM `silver_data.norm_catalog_supply`
GROUP BY 1;
