# Implementation Plan - D&D Keyword Extraction

## Goal Description
Extract proper noun keywords (NPCs, Locations, Factions, Deities, Villains) from D&D 5e sourcebooks/adventures, starting with "Divine Contention", and compile them into a CSV format suitable for import into Google Cloud BigQuery.

## User Review Required
> [!IMPORTANT]
> **Access Limitations**: I will attempt to access D&D Beyond, but content is often paywalled. If direct access fails, I will use web searches (wikis, guides) to identify keywords.
> **CSV Format**: I will use the requested format: `Keyword, Category, World, Source`.

## Proposed Changes

### Data Extraction
I will process books sequentially. For each book:
1.  Identify the book title.
2.  Search for/Extract lists of:
    *   Major NPCs / Villains
    *   Locations (Cities, Dungeons, Landmarks)
    *   Factions / Organizations
    *   Deities (if specific to the adventure)
3.  Filter out generic terms (already done by user) and focus on proper nouns.

### Data Storage
#### [NEW] [dnd_keywords.csv](file:///C:/Users/Yorri/.gemini/antigravity/scratch/dnd_keywords.csv)
A CSV file containing the extracted data with columns: `Keyword, Category, World, Source`.

## Verification Plan
### Manual Verification
- Check a sample of extracted keywords against the source material (via web search) to ensure accuracy.
- I will include "Core Rules" keywords if they appear relevant to the adventure (e.g., specific monsters that are key villains), as the user's importer handles deduplication.
