# Claude Code Guardrails — dnd-trends

Durable instructions for any Claude session in this repository. These apply regardless of permission mode (Accept Edits, Bypass Permissions, etc.) and regardless of how much autonomy a given session feels like it has.

---

## Bypass Permissions — pause and ask first for these actions

Yorri runs Claude Code in Bypass Permissions mode for flow. That skips the per-action permission prompt, but it does **not** remove the judgment layer. Before taking any of the following actions, I must stop, describe what I'm about to do in one sentence, and wait for explicit confirmation in the same turn — even though no permission prompt will fire:

- **Merging a PR** — `gh pr merge` in any form
- **Pushing to `main`** — direct pushes or any operation that lands commits on `main` (feature branches are fine to push freely)
- **Force-pushing anywhere** — `git push --force`, `git push --force-with-lease`
- **Deploying to Cloud Run** — `gcloud run deploy`, `gcloud builds submit`, any deploy script
- **Writing to Firestore** — any Firestore admin SDK mutation, any `firebase firestore:*` write
- **Writing or altering BigQuery data** — `bq query --destination_table`, `bq load`, `bq rm`, `DELETE FROM`, `DROP TABLE`, `CREATE OR REPLACE`, `TRUNCATE` (read queries are fine)
- **`gcloud` commands that change GCP state** — IAM, bucket writes, service config, scheduler job edits (read commands like `list`/`describe` are fine)
- **Recursive deletes** — `rm -rf`, `git clean -fdx`, `git worktree remove --force`

**What "pause and ask" looks like:** I output something like *"About to `gh pr merge 42 --squash` — this will squash-merge PR #42 into main and delete the branch. Confirm?"* and then stop. I do not call the tool until Yorri replies with confirmation in the same turn. A plain "yes" or "ok" is enough.

**Why:** Bypass Permissions is a flow optimization for the ~95% of actions that are reversible local edits. The categories above are where a mistake is expensive — visible to others, hard to undo, or touches durable shared state (production services, databases, git history others have pulled). The prompt-level approval is gone; the judgment-level approval should not be.

**Why this list and not more:** Everything here either affects shared infra, rewrites public history, or destroys data at scale. Routine file edits, branch pushes, PR *creation* (not merging), `git commit`, `git checkout`, package installs, and dev-server runs all stay fast — that's the whole point of bypass mode.

---

## Commit cadence — after every logical chunk

When working on a multi-step task, commit after every logical unit of work, not just at the end of the session. A "logical chunk" means one of:

- A discrete sub-feature that works on its own (e.g. "shadcn primitives added" and "CardChrome component built" are two commits, not one)
- A file-group refactor that compiles and makes sense standalone
- A vocabulary-boundary rename across N files (like Briefcase → Bag of Holding)
- A bug fix plus its test

**Not** every single file edit, and **not** "everything I touched this session."

**Why:** In Bypass Permissions mode, recovery from a bad edit is bounded by the last commit. Long uncommitted working trees mean `git restore` risks losing an hour of good work alongside one bad change. Small frequent commits mean every rollback is ~10 minutes of loss, not ~90. The cost of an extra `git commit` is ~2 seconds; the cost of losing good work while recovering from a bad edit is much higher.

**How to apply:** If I notice three unrelated changes that all work and are all wanted, that's a signal I'm past a commit boundary — commit the coherent ones now and keep going. Conversely, don't split a 30-line component implementation across six commits just to hit a cadence. Cadence scales with task; the rule is "don't let uncommitted state accumulate past the point where a rollback would hurt."

---

## How to extend this file

New guardrails go in their own `## ` section with a short rule, a **Why:** paragraph, and a **How to apply:** paragraph (same pattern as the memory files). Keep the rule scannable on the first line so future sessions can absorb it at a glance.
