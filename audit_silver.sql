SELECT keyword, interest, date
FROM `dnd-trends-index.silver_data.google_trends_raw`
WHERE keyword IN ('Barbarian', 'Bard', 'Cleric', 'Druid', 'Fighter', 'Monk', 'Paladin', 'Ranger', 'Rogue', 'Sorcerer', 'Warlock', 'Wizard')
LIMIT 20;
