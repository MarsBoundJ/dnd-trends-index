from google.cloud import bigquery
client = bigquery.Client(project='dnd-trends-index')
sql = """
INSERT INTO `dnd-trends-index.dnd_trends_raw.ai_suggestions` (concept_name, suggested_category, reason, status)
VALUES 
('Caleb Widogast', 'NPC', 'High-hype Critical Role character', 'PENDING'),
('Mollymauk Tealeaf', 'NPC', 'High-hype Critical Role character', 'PENDING'),
('Ireena Kolyana', 'NPC', 'Core Curse of Strahd character', 'PENDING'),
('Kas', 'Villain', 'Legendary D&D Villain', 'PENDING'),
('Raistlin Majere', 'NPC', 'Iconic Dragonlance character', 'PENDING'),
('Fizban the Fabulous', 'NPC', 'Iconic Dragonlance character', 'PENDING'),
('Lady of Pain', 'NPC', 'Planescape Ruler', 'PENDING'),
('Iuz', 'Villain', 'Classic Greyhawk Villain', 'PENDING'),
('Planescape', 'Setting', 'Major D&D Setting', 'PENDING'),
('Abyss', 'Location', 'Iconic Lore Location', 'PENDING'),
('Mighty Nein', 'Actual Play', 'Critical Role Party', 'PENDING'),
('Space clown', 'Monster', 'Spelljammer Creature', 'PENDING'),
('Borys', 'Villain', 'Dark Sun Sorcerer-King', 'PENDING'),
('Sorcerer-Kings', 'Faction', 'Dark Sun Rulers', 'PENDING'),
('Natural 20', 'Mechanic', 'Core Game Term', 'PENDING'),
('Creature type', 'Mechanic', 'Core Game Term', 'PENDING');
"""
client.query(sql).result()
print("Inserted 16 Fandom ghosts into ai_suggestions.")
