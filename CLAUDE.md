# Claude Code Guardrails — dnd-trends

Durable instructions for any Claude session in this repository. These apply regardless of permission mode (Accept Edits, Bypass Permissions, etc.) and regardless of how much autonomy a given session feels like it has.

---

## Bypass Permissions — pause and ask first for these actions

Yorri runs Claude Code in Bypass Permissions mode for flow. That skips the per-action permission prompt, but it does **not** remove the judgment layer. Before taking any of the following actions, I must stop, describe what I'm about to do in one sentence, and wait for explicit confirmation in the same turn — even though no permission prompt will fire:

- **Merging a PR I did not open** — someone else's PR, or one opened in an earlier session (see "Merging my own PRs" below for the pre-authorized case)
- **Pushing to `main`** — direct pushes (feature branches are fine to push freely; landing commits on `main` *via merging my own PR* is pre-authorized below)
- **Force-pushing anywhere** — `git push --force`, `git push --force-with-lease`
- **Deploying to Cloud Run** — `gcloud run deploy`, `gcloud builds submit`, any deploy script
- **Writing to Firestore** — any Firestore admin SDK mutation, any `firebase firestore:*` write
- **Writing or altering BigQuery data** — `bq query --destination_table`, `bq load`, `bq rm`, `DELETE FROM`, `DROP TABLE`, `CREATE OR REPLACE`, `TRUNCATE` (read queries are fine)
- **`gcloud` commands that change GCP state** — IAM, bucket writes, service config, scheduler job edits (read commands like `list`/`describe` are fine)
- **Recursive deletes** — `rm -rf`, `git clean -fdx`, `git worktree remove --force`

**What "pause and ask" looks like:** I output something like *"About to `gh pr merge 42 --squash` — this will squash-merge PR #42 into main and delete the branch. Confirm?"* and then stop. I do not call the tool until Yorri replies with confirmation in the same turn. A plain "yes" or "ok" is enough.

**Why:** Bypass Permissions is a flow optimization for the ~95% of actions that are reversible local edits. The categories above are where a mistake is expensive — visible to others, hard to undo, or touches durable shared state (production services, databases, git history others have pulled). The prompt-level approval is gone; the judgment-level approval should not be.

**Why this list and not more:** Everything here either affects shared infra, rewrites public history, or destroys data at scale. Routine file edits, branch pushes, `git commit`, `git checkout`, package installs, and dev-server runs all stay fast — that's the whole point of bypass mode.

---

## Merging my own PRs — pre-authorized (granted Sep 1, 2026)

Yorri granted standing permission to merge a PR **I opened during the current chat session**, without pausing to confirm. He should not have to go into GitHub to click the button.

**Scope — what this covers:**

- A PR I created in this repo, in this session, whose contents I authored and can vouch for
- Merging it into `main`, and deleting the branch afterward

**Scope — what this does NOT cover** (still pause and ask):

- PRs opened by anyone else, or by a previous session — I have not reviewed that diff end to end
- Direct pushes to `main`, force-pushes, or anything that rewrites published history
- Every other category in the pause-and-ask list above. Merging a PR whose diff performs a guarded action (a deploy, a BigQuery migration) is still that action — the merge wrapper does not launder it

**Before merging, I still:**

1. Confirm required checks pass (`gh pr checks <n>`) — a red check is a stop, not a speed bump
2. Match the repo's merge convention (squash; recent history shows `(#NN)` suffixes)
3. Report the resulting `main` SHA so the change is traceable

**Why:** The pause existed to guarantee a human reviewed an irreversible, outward-facing change. For a PR I just authored in front of Yorri, that review already happened *in the conversation* — he watched it get built, saw the verification, and approved it by asking for the PR. Re-confirming at merge time was asking the same question twice. The guarantee still matters for diffs he has not seen, which is why third-party PRs stay gated.

**How to apply:** If I opened the PR this session and checks are green, merge it and report the SHA. If there is any doubt about whether Yorri has actually seen the diff — a PR from an earlier session, a rebase that pulled in unrelated commits — treat it as a third-party PR and ask.

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
