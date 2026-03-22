# Antigravity Commit Instructions — Context Documents
# Run this once to establish the context_docs folder in the GitHub repo.

# 1. Create the context_docs directory and add all four files
#    Copy the four files Claude generated into context_docs/ in the repo root

# 2. Commit with this message:
git add context_docs/
git commit -m "feat: add AI context documents for session continuity

Adds four reference documents maintained by Claude and committed by Antigravity:
- CONTEXT.md: master onboarding doc, project status, key principles
- ARCHITECTURE.md: GCP infrastructure, BigQuery schema, source files
- CONCEPT_LIBRARY.md: keyword database reference, known messy areas
- SESSIONS.md: running session log, decisions made, next steps

These documents are updated by Claude at the end of each session.
Antigravity commits the updates. Do not edit manually."

git push

# 3. Verify the files are present:
git ls-files context_docs/

# Expected output:
# context_docs/CONTEXT.md
# context_docs/ARCHITECTURE.md
# context_docs/CONCEPT_LIBRARY.md
# context_docs/SESSIONS.md
