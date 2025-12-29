CREATE OR REPLACE TABLE `dnd-trends-index.commercial_data.backerkit_hype_analysis` AS
SELECT
    p.project_id,
    p.title,
    p.funding_usd,
    p.backers_count,
    p.system_tag,
    k.concept_name AS match_keyword,
    k.category AS match_category,
    -- Hype Signal: Funding per backer
    (p.funding_usd / NULLIF(p.backers_count, 0)) AS funding_per_backer
FROM
    `dnd-trends-index.commercial_data.backerkit_projects` p
LEFT JOIN
    `dnd-trends-index.dnd_trends_categorized.concept_library` k
ON
    LOWER(p.title) LIKE CONCAT('%', LOWER(k.concept_name), '%')
WHERE
    p.system_tag = '5e Compatible'
    AND k.concept_name IS NOT NULL
ORDER BY
    p.funding_usd DESC;
