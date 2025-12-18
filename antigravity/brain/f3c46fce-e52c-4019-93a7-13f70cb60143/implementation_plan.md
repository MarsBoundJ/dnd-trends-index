# Partnered Content Keyword Extraction Plan

## Goal Description
Extract keywords (Monsters, Subclasses, NPCs, Locations, etc.) from D&D Beyond Partnered Content books and append them to `dnd_keywords.csv`.

## User Review Required
- **Source of Data**: We need to determine if web searching is sufficient or if the user needs to provide TOCs for all books.
- **Categorization**: Confirm how to categorize "Partnered Content" (e.g., Source column format).

## Proposed Changes
### Data Extraction
- **Workflow**: User provides Table of Contents (TOC) in batches of 5.
- **Parsing**: Create/Update parsing scripts for each batch to extract Keywords, Categories, Worlds, and Sources.
- **Ruleset**: Default to "2014" unless specified otherwise (e.g., Drakkenheim).

### CSV Updates
- Append new rows to `c:\Users\Yorri\.gemini\antigravity\scratch\dnd_keywords.csv`.
- Columns: `Keyword`, `Category`, `World`, `Source`, `Ruleset`.
    - `Ruleset`: "2014" or "2024".

## Verification Plan
### Automated Tests
- None (Data entry task).

### Manual Verification
- Check `dnd_keywords.csv` for correct formatting and new entries.
- Verify a sample of extracted keywords against the source list.
