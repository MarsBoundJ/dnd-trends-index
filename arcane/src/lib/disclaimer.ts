/*
 * Trusight — AI-content disclaimers.
 *
 * Shared source of truth for all disclaimers that travel with
 * AI-generated outputs. Having these co-located means:
 *   1. One place to edit the wording.
 *   2. Consistent phrasing/tone across article footers, the Sage chat
 *      footer, and Council-member bio popovers.
 *   3. PDF export (Step 15 planned) reads from the same constants,
 *      so leaked-screenshot / leaked-PDF / leaked-chat-snippet all
 *      carry the same non-affiliation language.
 *
 * Why this exists: Trusight articles — especially Track D
 * (Hasbro-frame) corporate-strategy pieces like the Dean's
 * "D&D Book Isn't Dying" — can be structurally indistinguishable
 * from internal-consulting analysis if screenshot out of context.
 * The disclaimer is the anti-misattribution insurance. Ships inside
 * the card, not outside, so it travels with every screenshot.
 */

/**
 * Full legal disclaimer for AI-generated article content. Rendered
 * in the article-card footer. Travels with every screenshot, every
 * future PDF export. Wording is deliberately defensive because
 * Track D corporate-strategy articles are the specific leakage
 * risk — confident analytical tone + Hasbro frame citations + named
 * benchmarks = "reads like internal strategy deck."
 */
export const ARTICLE_DISCLAIMER =
  "Generated with AI for educational analysis. Not affiliated with or " +
  "endorsed by Hasbro, Wizards of the Coast, or any named entity. " +
  "DUNGEONS & DRAGONS, D&D, and related marks are property of " +
  "Wizards of the Coast LLC."

/**
 * Shorter variant for the Sage chat footer (fast-follow). Sage is
 * unambiguously a chat UI so the full footer is overkill, but a
 * disclosure note keeps Sage's responses framed correctly if they're
 * copy-pasted elsewhere. Planned for a post-pivot commit — exported
 * now so consumers can start referencing it without another import
 * churn.
 */
export const SAGE_DISCLAIMER =
  "Sage is AI-generated. Trusight is not affiliated with " +
  "or endorsed by Hasbro or Wizards of the Coast."

/**
 * For Council member bio popovers. Clarifies synthetic-persona status
 * without naming any real individuals. The 9.10.1 tuning pass moved
 * this away from the earlier "built from real industry luminaries"
 * phrasing to avoid even oblique attribution — pairs with the
 * luminary-anonymity rule in COUNCIL_HOUSE_RULES (council.py) that
 * keeps named individuals out of article output entirely.
 */
export const COUNCIL_PERSONA_DISCLAIMER =
  "Synthetic persona. Composite analytical voice grounded in " +
  "published strategic canon. Not a real analyst."
