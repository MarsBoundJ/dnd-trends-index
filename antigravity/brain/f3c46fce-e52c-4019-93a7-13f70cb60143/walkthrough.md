# D&D Partnered Content Keyword Extraction Walkthrough

## Completed Work
We have successfully extracted and compiled keywords from the following Partnered Content and Third-Party sources:

1.  **Flee, Mortals!** (MCDM) - Extracted Monsters and Villain Parties.
2.  **Tome of Beasts 1** (Kobold Press) - Extracted 486 Monsters.
3.  **Monsters of Drakkenheim** (Ghostfire Gaming) - Extracted Monsters, Factions, and Villains.
4.  **Tal’Dorei Campaign Setting Reborn** (Darrington Press) - Extracted Factions, Locations, and Monsters.
5.  **Explorer's Guide to Wildemount** (Wizards of the Coast / Critical Role) - Extracted Factions, Locations, Subclasses, and Monsters.

## Verification
The final CSV file `dnd_keywords.csv` now contains keywords from all these sources, categorized by `Keyword`, `Category`, `World`, `Source`, and `Ruleset` (2014 or 2024).

### Sample Entries
```csv
Angulotls,Monster,Generic,"Flee, Mortals!",2014
Zmey,Monster,Generic,Tome of Beasts 1,2014
Delerium Dreg,Monster,Drakkenheim,Monsters of Drakkenheim,2024
Ashari,Faction,Exandria,Tal’Dorei Campaign Setting Reborn,2014
Echo Knight,Subclass,Exandria,Explorer's Guide to Wildemount,2014
```

## Next Steps
- The CSV is ready for import into BigQuery.
- You can continue to add more books using the same method (providing TOCs or lists).
