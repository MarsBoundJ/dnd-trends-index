/**
 * Build editable DOCX companions for both Trusight one-pagers.
 * These are for Phil's editing — clean text + structure, not trying
 * to recreate the visual polish of the PDFs.
 */

const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageOrientation,
} = require("docx");

// ── Shared styles & helpers ───────────────────────────────────────────
const FONT = "Calibri";
const HEAD_FONT = "Trebuchet MS";

function brandHeader() {
  return [
    new Paragraph({
      children: [new TextRun({
        text: "TRUSIGHT", bold: true, size: 40, color: "E87722",
        font: HEAD_FONT, characterSpacing: 60,
      })],
    }),
    new Paragraph({
      children: [new TextRun({
        text: "a D&D Trendwatch", italics: true, size: 22,
        color: "555555", font: HEAD_FONT,
      })],
      spacing: { after: 240 },
    }),
  ];
}

function footer() {
  return [
    new Paragraph({
      border: { top: { style: BorderStyle.SINGLE, size: 12, color: "E87722", space: 8 } },
      spacing: { before: 480, after: 60 },
      children: [],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [
        new TextRun({ text: "Phil Jones  ·  ", bold: true }),
        new TextRun({ text: "phil@trusightdata.ai" }),
        new TextRun({ text: "  ·  trusightdata.ai" }),
      ],
    }),
  ];
}

const sharedStyles = {
  default: { document: { run: { font: FONT, size: 22 } } }, // 11pt default
  paragraphStyles: [
    { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
      run: { size: 44, bold: true, font: HEAD_FONT, color: "111111" },
      paragraph: { spacing: { before: 120, after: 120 }, outlineLevel: 0 } },
    { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
      run: { size: 28, bold: true, font: HEAD_FONT, color: "E87722" },
      paragraph: { spacing: { before: 240, after: 120 }, outlineLevel: 1 } },
    { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
      run: { size: 24, bold: true, font: HEAD_FONT, color: "111111" },
      paragraph: { spacing: { before: 180, after: 60 }, outlineLevel: 2 } },
  ],
};

const sharedNumbering = {
  config: [
    { reference: "bullets",
      levels: [{ level: 0, format: LevelFormat.BULLET, text: "•",
        alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 540, hanging: 270 } } } }] },
  ],
};

function bullet(text, opts = {}) {
  return new Paragraph({
    numbering: { reference: "bullets", level: 0 },
    children: [new TextRun({ text, ...opts })],
    spacing: { after: 60 },
  });
}

function p(text, opts = {}) {
  return new Paragraph({
    children: [new TextRun({ text, ...opts })],
    spacing: { after: opts.spaceAfter || 120 },
  });
}

const pageSetup = {
  size: { width: 12240, height: 15840 }, // US Letter
  margin: { top: 1080, right: 1080, bottom: 1080, left: 1080 }, // 0.75"
};

// ═════════════════════════════════════════════════════════════════════
// ONE-PAGER #1 — The IP Decision Engine
// ═════════════════════════════════════════════════════════════════════

const doc1 = new Document({
  styles: sharedStyles,
  numbering: sharedNumbering,
  sections: [{
    properties: { page: pageSetup },
    children: [
      ...brandHeader(),

      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun({ text: "The IP Decision Engine", bold: true, font: HEAD_FONT, size: 44 })],
      }),
      new Paragraph({
        children: [new TextRun({
          text: "12 candidate IPs. 8-source signal stack. Sleepers rise; obvious picks fall.",
          italics: true, size: 26, color: "555555",
        })],
        spacing: { after: 360 },
      }),

      // ── Intro premise ──
      p("We carefully selected these 12 IPs to hit potential strategic blind spots. The Left Side (Force-Fit Risk and License Landmine) features brands WotC may covet based on vanity metrics. The Right Side (Greenlight and Sleeper) features data-backed, high-ROI IPs that bypass traditional licensing intuition. Every data claim below is cross-referenced against Trusight's gold-tier views; nothing here is hand-waved."),

      // ═══ GREENLIGHT ═══
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "GREENLIGHT — high fit + high reception (6 IPs)", bold: true, font: HEAD_FONT, size: 28, color: "2A8B4D" })],
      }),
      p("Surprising, data-backed greenlights ready to ship. Low CAC, high margin.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "1. Hollow Knight", bold: true, font: HEAD_FONT, size: 22, color: "2A8B4D" })] }),
      p("Indie sleeper most licensing teams overlook. Both communities pre-converting; indie pricing protects margin. Ship as a digital-first D&D Beyond supplement."),
      p("Reddit Reception 1.0 · Reverse-funnel Acquisition 0.85 · 2 of 2 sources positive (consilience) · Auto-flagged gold_mine in our matrix.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "2. Bloodborne", bold: true, font: HEAD_FONT, size: 22, color: "2A8B4D" })] }),
      p("FromSoft pattern proven. Dark fantasy + tactical combat = the Aging-Up demographic Hasbro is targeting. Premium hardcover that commands premium pricing."),
      p("5 sources measured (deepest data in the deck) · Forum 0.80 (HIGH confidence) · BGG 0.81 · Homebrew 0.98 · 5 of 6 sources positive (consilience).", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "3. Dungeon Crawler Carl", bold: true, font: HEAD_FONT, size: 22, color: "2A8B4D" })] }),
      p("Top-tier matrix score (composite fit 0.97). LitRPG audience pre-converted via reverse-funnel signal — fans are already begging for D&D conversion. Near-zero CAC."),
      p("Composite fit 0.97 · Reddit acquisition 0.71 · 2 of 2 sources positive (consilience) · Auto-flagged gold_mine.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "4. Stranger Things", bold: true, font: HEAD_FONT, size: 22, color: "2A8B4D" })] }),
      p("License for RETENTION, not acquisition. The asymmetry no other tool surfaces — D&D players love it (Reddit recep 0.85), but Stranger Things fans aren't asking for D&D back (acq 0.54)."),
      p("6 sources (most-measured IP in the deck) · 4 of 5 sources positive (consilience) · 3 forum backlash narratives flagged on Stage 7 — sell to existing DMs, not ad-blast Netflix fans.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "5. Avatar: The Last Airbender", bold: true, font: HEAD_FONT, size: 22, color: "2A8B4D" })] }),
      p("Highest mechanical-fit score in the deck (Gemini fit 0.92). WoTC's existing MTG license precedent supports natural D&D extension."),
      p("Gemini fit 0.92 · BGG proxy 0.78 · 3 of 4 sources positive (consilience) · 3 sources measured.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "6. Berserk", bold: true, font: HEAD_FONT, size: 22, color: "2A8B4D" })] }),
      p("Iconic dark fantasy with deep D&D Beyond homebrew. Premium hardcover that commands premium pricing without brand dilution."),
      p("Composite fit 0.94 · Homebrew 0.94 · 5 of 5 sources positive (unanimous consilience) · 38 DDB Berserker subclasses visible / 9 confirmed via AI Bouncer.", { italics: true, color: "555555" }),

      // ═══ SLEEPER ═══
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "SLEEPER — high fit + low/untested reception (4 IPs)", bold: true, font: HEAD_FONT, size: 28, color: "5078B8" })],
      }),
      p("Invisible to mainstream metrics, but high mechanical fit suggests real opportunity to investigate before commitment.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "7. Frieren: Beyond Journey's End", bold: true, font: HEAD_FONT, size: 22, color: "5078B8" })] }),
      p("Anime structured exactly like a D&D adventuring party (Mage / Warrior / Priest, post-Demon-Lord setting). Captures the Gen-Z anime audience without requiring mechanical rewrites of 5e core rules."),
      p("Gemini fit 0.86 · Forum 0.69 (HIGH confidence, 3 constructive narratives) · 1 of 1 sources positive (thin evidence — investigate further before commitment).", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "8. Demon Slayer", bold: true, font: HEAD_FONT, size: 22, color: "5078B8" })] }),
      p("Gemini rates the mechanics translatable, but cross-source signal is thin. AO3 zero D&D crossover is the yellow flag — investigate further before committing the licensing fee."),
      p("Composite fit 0.86 · Reception 0.48 · 2 of 3 sources positive (consilience — split signal) · AO3 zero crossover works.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "9. Made in Abyss", bold: true, font: HEAD_FONT, size: 22, color: "5078B8" })] }),
      p("Literally about descending layer-by-layer into a terrifying dungeon. Translates 1:1 to Virtual Tabletop and digital map packs. Operational efficiency dream for Project Sigil."),
      p("Composite fit 0.78 · Homebrew 0.67 · 1 of 1 sources positive (thin evidence — reception untested).", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "10. Dwarf Fortress", bold: true, font: HEAD_FONT, size: 22, color: "5078B8" })] }),
      p("Emergent-storytelling DNA. Surfaced organically in our v5 GitP forum body scrape. The most D&D game that isn't a TTRPG."),
      p("Composite fit 0.91 · Forum 0.50 (HIGH confidence, 10 confirmed threads via GitP scrape) · 1 of 1 sources positive (thin evidence).", { italics: true, color: "555555" }),

      // ═══ FORCE-FIT RISK ═══
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "FORCE-FIT RISK — low fit + high reception (1 IP)", bold: true, font: HEAD_FONT, size: 28, color: "C49530" })],
      }),
      p("Community shows interest, but the IP doesn't translate. Backlash if forced.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "11. Discworld", bold: true, font: HEAD_FONT, size: 22, color: "C49530" })] }),
      p("Beloved fantasy, but Pratchett's whole project subverts the D&D conventions WoTC would charge for adapting. Officially licensing it would feel like 'milking' to the diehards."),
      p("Gemini fit 0.40 (only IP in the deck under 0.50) · Forum 0.72 · 3 of 4 sources positive (consilience).", { italics: true, color: "555555" }),

      // ═══ LICENSE LANDMINE ═══
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "LICENSE LANDMINE — low fit + low reception (1 IP)", bold: true, font: HEAD_FONT, size: 28, color: "C44949" })],
      }),
      p("Looks viable from outside; the data says it bites you.", { italics: true, color: "555555" }),

      new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text: "12. Spy × Family", bold: true, font: HEAD_FONT, size: 22, color: "C44949" })] }),
      p("Top global anime, but a triple-source negative on D&D-context fit. The 'looks like a deal, will fail at the table' case. Trusight saves the licensing fee."),
      p("Composite fit 0.66 · Reception 0.22 (lowest in the deck — severe failure, below 0.30 threshold) · 0 of 2 sources positive (consilience — unanimous negative) · BGG 'Old Maid: Spy × Family' branded variant flopped commercially · AO3 zero D&D crossover fanfic.", { italics: true, color: "555555" }),

      // ── Process footer ──
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "The 8-source signal stack", bold: true, font: HEAD_FONT, size: 28, color: "E87722" })],
      }),
      p("Each stage = independent measurement. Composite scores triangulate across stages. Per-IP renormalization handles missing sources without penalty. Abstention by default when fewer than two sources measure an IP."),
      bullet("Stage 1 — Gemini-anchored baseline (142 IPs scored on a 6-dimension rubric)"),
      bullet("Stage 2 — Precedent databases (MTG Universes Beyond + D&D crossover history, 33 entries)"),
      bullet("Stage 3 — BGG licensed-game proxy (tabletop buyers' prior reception of the IP)"),
      bullet("Stage 4 — AO3 fanfic crossover (organic revealed preference)"),
      bullet("Stage 5 — Reddit sentiment + reverse-funnel acquisition (25 D&D-adjacent subreddits)"),
      bullet("Stage 6 — Homebrew creation (UA + GMBinder + Homebrewery + D&D Beyond)"),
      bullet("Stage 7 — TTRPG forums × 3 with cross-forum triangulation (EN World + RPG.net + GitP)"),
      bullet("Stage 8 — Commercial revealed preference (DriveThruRPG + DMs Guild bestseller-tier)"),
      p("Cumulative build cost across all 8 stages: ~$2.25. Production-deployed.", { italics: true, color: "555555" }),

      ...footer(),
    ],
  }],
});

Packer.toBuffer(doc1).then(buf => {
  fs.writeFileSync("trusight_onepager_ip.docx", buf);
  console.log("Wrote: trusight_onepager_ip.docx");
});

// ═════════════════════════════════════════════════════════════════════
// ONE-PAGER #2 — What else Trusight does
// ═════════════════════════════════════════════════════════════════════

const doc2 = new Document({
  styles: sharedStyles,
  numbering: sharedNumbering,
  sections: [{
    properties: { page: pageSetup },
    children: [
      ...brandHeader(),

      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun({ text: "What else Trusight does", bold: true, font: HEAD_FONT, size: 44 })],
      }),
      new Paragraph({
        children: [new TextRun({
          text: "What Palantir is to defense, Trusight is to fantasy IP — the decision layer behind Designers, Marketers, and Licensing.",
          italics: true, size: 26, color: "555555",
        })],
        spacing: { after: 360 },
      }),

      // ── Designers ──
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "DESIGNERS  —  what should we build next?", bold: true, font: HEAD_FONT, size: 28, color: "7AA3FF" })],
      }),
      p("Killer feature: Top 5 unmet player fantasies in the last 12 months.", { bold: true, italics: true }),
      bullet("UA Sentiment Auditor — live Reddit + forum classification of Unearthed Arcana playtest reaction. AI Bouncer tags 'math broken' / 'flavor bad' / 'fun to play'. 48-hour signal vs 4-week survey lag."),
      bullet("Homebrew Gap Identifier — when the community is patching what you're not shipping. UA + GMBinder + DDB Homebrew velocity by mechanical category gives quantitative proof of a rules-gap to fill."),
      bullet("Mechanic Demand Detection — cross-stream rising signal for class types, settings, mechanics. Surface 'witch class', 'cozy fantasy', 'tactical horror' demand 12-18 months before mainstream peak."),

      // ── Marketers ──
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "MARKETERS  —  what sells, and how do we say it?", bold: true, font: HEAD_FONT, size: 28, color: "B8862C" })],
      }),
      p("Killer feature: Backlash early warning  ★", { bold: true, italics: true }),
      bullet("OGL-tier Backlash Detector ★ — daily-cadence community sentiment. If WotC pre-hints a move (Asmodee printing partnership, OGL revision, AI policy change), Trusight surfaces the reaction within 24 hours, before commitment. Not a minute-level crisis monitor — a trial-balloon detector. The OGL fiasco surfaced in horizontal social-listening tools only AFTER the community had already organized boycotts. Trusight catches the response wave inside 24 hours."),
      bullet("Creator ROI Attribution — did that Critical Role / Dimension 20 sponsorship actually move product? Track Google Trends + Fandom + DDB lookup velocity around the specific monsters and modules used in the episode for the 14 days after."),
      bullet("Audience Segmentation — AO3-heavy vs Reddit-heavy vs BGG-heavy IPs map cleanly to narrative-fan / mechanics-fan / buyer segments. Tailor channel mix and creative direction by where each IP's fans actually live."),

      // ── IP & Licensing ──
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "IP & LICENSING  —  what should we acquire?", bold: true, font: HEAD_FONT, size: 28, color: "2A8B4D" })],
      }),
      p("Killer feature: 12-18 month forecasting radar.", { bold: true, italics: true }),
      bullet("UB Matrix (the IP Decision Engine) — 142 candidate IPs scored on Fit / Reception / Acquisition / Commercial. See the companion one-pager for the Hollow Knight (rises) vs Spy × Family (falls) case studies. Cross-source triangulation rules out single-source noise."),
      bullet("Buyers-Remorse Landmine Detector — BGG board-game adaptation reception + AO3 zero-crossover signal flag IPs that look great on paper but the tabletop community will reject. Saves 7-figure licensing fees on Walking-Dead-tier risks."),
      bullet("Reverse-Funnel Acquirer — scans non-D&D IP subreddits for 'D&D' / '5e' / 'campaign' mentions. Surfaces communities pre-converted to D&D — near-zero CAC targets like Hollow Knight or Dungeon Crawler Carl that no Hollywood-IP-list approach would surface."),

      // ── Maps to Hasbro Playing-to-Win ──
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "Maps to Hasbro's Playing-to-Win strategy", bold: true, font: HEAD_FONT, size: 28, color: "E87722" })],
      }),
      bullet("Profitable Franchises — high-resolution D&D-segment health by region, demographic, content category."),
      bullet("Aging Up — kidult-tier engagement signal, including high-value collector + adult-fan dynamics."),
      bullet("Digital & Direct — D&D Beyond / VTT signal layer with REST + GraphQL APIs for personalization."),
      bullet("Partner Scaled — UB Matrix de-risks every inbound license; cross-source scoring forces accountability into the partner-selection process."),

      // ── Tech integration ──
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun({ text: "Drops into your existing stack", bold: true, font: HEAD_FONT, size: 28, color: "E87722" })],
      }),
      p("Trusight outputs land as Snowflake Secure Share into your Looker dashboards. No new BI tool. Your analytics engineers materialize Trusight signals into existing dbt models in an afternoon. 12+ live signal streams. 8-stage BigQuery pipeline. AI Bouncer disambiguation."),

      ...footer(),
    ],
  }],
});

Packer.toBuffer(doc2).then(buf => {
  fs.writeFileSync("trusight_onepager_capabilities.docx", buf);
  console.log("Wrote: trusight_onepager_capabilities.docx");
});
