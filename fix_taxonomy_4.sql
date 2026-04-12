UPDATE `dnd-trends-index.dnd_trends_categorized.concept_library`
SET category = CASE concept_name
  WHEN "Acid Seal" THEN "Other"
  WHEN "Celestial Codex" THEN "MagicItem"
  WHEN "Conch" THEN "MagicItem"
  WHEN "Cooperative Blade" THEN "MagicItem"
  WHEN "Elucidarium" THEN "MagicItem"
  WHEN "Folding Drums" THEN "MagicItem"
  WHEN "Fulcrum Lattice" THEN "MagicItem"
  WHEN "Hellfire Blade" THEN "MagicItem"
  WHEN "Jakad's Heart" THEN "MagicItem"
  WHEN "Lightning Rods" THEN "MagicItem"
  WHEN "Mythos Tomes" THEN "MagicItem"
  WHEN "Serpent's Crown" THEN "MagicItem"
  WHEN "Soul Coin" THEN "MagicItem"
  WHEN "Spelljamming Helm" THEN "MagicItem"
END
WHERE concept_name IN (
  "Acid Seal","Celestial Codex","Conch","Cooperative Blade","Elucidarium",
  "Folding Drums","Fulcrum Lattice","Hellfire Blade","Jakad's Heart",
  "Lightning Rods","Mythos Tomes","Serpent's Crown","Soul Coin","Spelljamming Helm"
);
