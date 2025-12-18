# Disambiguation & Cleaning Plan for UA Importer

## Goal
Ensure valid, distinct keywords for D&D Unearthed Arcana concepts, specifically addressing naming collisions for classes (e.g., "Ranger", "Artificer") that appear in multiple UA columns, while also fixing "garbage" keywords derived from long description text.

## Problem Analysis
1.  **Messy Keywords:** Currently, the script splits the "Contents" column by semicolon. Many rows contain full sentences (e.g., "Full revised Ranger class with new conclaves"). This results in long, practically useless "keywords" like `"Full revised Ranger class with new conclaves (UA)"`.
2.  **Disambiguation Risk:** If we clean these keywords to their core concepts (e.g., "Ranger"), we inevitably create collisions because "Ranger" appears in multiple UAs (2015, 2016, One D&D).
3.  **User Request:** The user specifically asked to handle cases where classes like Warlock appear in multiple UAs.

## Proposed Strategy: "Clean & Disambiguate"

I propose modifying `importer_ua_expanded.py` to:

1.  **Normalize Class Names:** Use regex to identify major class updates within the text.
    - Map "Revised Ranger", "Ranger class", "The Ranger" -> `Ranger`.
    - Map "Artificer class" -> `Artificer`.
2.  **Append Year Disambiguator:** Automatically append the year to the concept name for these normalized class concepts.
    - `Ranger` (from 2015 UA) -> **"Ranger (UA 2015)"**
    - `Ranger` (from 2016 UA) -> **"Ranger (UA 2016)"**
    - `Warlock` (from Hexblade UA) -> **"Warlock (Hexblade) (UA)"** (Subclasses usually stay distinct enough, but we can standardise this too if desired).
3.  **Fallback:** For items that don't match a clear "Class" pattern, keep the existing behavior or try to extract the first few words, but ensure the `(UA)` suffix is present.

## Proposed Changes

### [Dnd_UA Importer](file:///c%3A/Users/Yorri/.gemini/antigravity/scratch/dnd_ua/importer_ua_expanded.py)

#### [MODIFY] `parse_ua_row` function
-   Implement a cleaning step before assigning `concept_name`.
-   Use a dictionary or regex to detect distinct Base Classes (Artificer, Barbarian, etc.).
-   If a Base Class update is detected (without a subclass), fetch the `Year` from the `Date` column.
-   Format concept as: `{Class Name} (UA {Year})`.

## Verification Plan

### Automated Verification
I will update and run `verify_parsing.py` to check the new output format.
-   **Check:** Ensure "Revised Ranger" becomes "Ranger (UA 2015)" (or similar).
-   **Check:** Ensure "The Ranger, Revised" becomes "Ranger (UA 2016)".
-   **Check:** Ensure "Warlock (Hexblade)" remains "Warlock (Hexblade) (UA)" or becomes "Warlock (Hexblade) (UA 2017)".

### User Review
-   Review a sample of the generated keywords to ensure they match expectations for the "Trends Index".
