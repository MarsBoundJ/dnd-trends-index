-- check for classes
SELECT search_term, interest
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
WHERE search_term IN ('Barbarian', 'Bard', 'Cleric', 'Druid', 'Fighter', 'Monk', 'Paladin', 'Ranger', 'Rogue', 'Sorcerer', 'Warlock', 'Wizard', 'Barbarian 5e', 'Bard 5e', 'Cleric 5e', 'Druid 5e', 'Fighter 5e', 'Monk 5e', 'Paladin 5e', 'Ranger 5e', 'Rogue 5e', 'Sorcerer 5e', 'Warlock 5e', 'Wizard 5e');

-- check for mechanics
SELECT search_term, interest
FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot`
WHERE LOWER(search_term) LIKE '%stat block%' OR LOWER(search_term) LIKE '%initiative%' OR LOWER(search_term) LIKE '%feat%';

-- get all distinct terms in pilot
SELECT DISTINCT search_term FROM `dnd-trends-index.dnd_trends_categorized.trend_data_pilot` LIMIT 100;
