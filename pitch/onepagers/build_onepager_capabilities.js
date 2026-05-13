/**
 * Trusight one-pager #2 — What Else Trusight Does
 * Custom Letter portrait (8.5" × 11"), single page.
 * Three persona columns: Designers / Marketers / IP & Licensing.
 * Plus Playing-to-Win mapping + the OGL early-warning capability.
 */

const pptxgen = require("pptxgenjs");
const path = require("path");
const pres = new pptxgen();

const LOGO_DARK_PATH = path.join(__dirname, "..", "assets", "logos", "trusight_logo_4k_dark.png");

pres.defineLayout({ name: "LETTER_P", width: 8.5, height: 11 });
pres.layout = "LETTER_P";
pres.author = "Phil Trubbel (Trusight)";
pres.title = "Trusight — Capability Sweep";

const C = {
  obsidian:    "0a0a18",
  obsidian2:   "1a1a2e",
  obsidianBd:  "2a2a4a",
  ember:       "e87722",
  cream:       "e0e0ff",
  white:       "ffffff",
  green:       "5fdc7c",
  amber:       "d9a64a",
  red:         "ff8888",
  blue:        "7aa3ff",
  mutedGray:   "888888",
  mutedGray2:  "555555",
};
const FONT_HEAD = "Trebuchet MS";
const FONT_BODY = "Calibri";

const slide = pres.addSlide();
slide.background = { color: C.obsidian };

// Left ember rail
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0, y: 0, w: 0.12, h: 11,
  fill: { color: C.ember }, line: { type: "none" },
});

// ─── HEADER ───────────────────────────────────────────────────────────
// Trusight beta logo (replaces wordmark text). Aspect ~6.22:1.
slide.addImage({
  path: LOGO_DARK_PATH,
  x: 0.5, y: 0.4, w: 3.11, h: 0.5,
});
// (Tagline "a D&D Trendwatch" is part of the logo image — no separate subtitle needed.)

// Thin ember accent
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0.5, y: 1.30, w: 0.8, h: 0.04,
  fill: { color: C.ember }, line: { type: "none" },
});

// ─── TITLE BLOCK ──────────────────────────────────────────────────────
slide.addText("What else Trusight does", {
  x: 0.5, y: 1.45, w: 7.5, h: 0.55,
  fontSize: 30, fontFace: FONT_HEAD, bold: true,
  color: C.white, align: "left", margin: 0,
});
slide.addText("What Palantir is to defense, Trusight is to fantasy IP — the decision layer behind Designers, Marketers, and Licensing.", {
  x: 0.5, y: 2.05, w: 7.5, h: 0.45,
  fontSize: 13, fontFace: FONT_BODY, italic: true,
  color: C.cream, align: "left", margin: 0,
});

// ─── THREE PERSONA COLUMNS ────────────────────────────────────────────
const colY = 2.75;
const colH = 5.65;
const colW = 2.45;
const colGap = 0.10;
const colStart = 0.5;

const personas = [
  {
    label: "DESIGNERS",
    color: C.blue,
    sub: "What should we build next?",
    headline: "Top 5 unmet player fantasies",
    cases: [
      {
        head: "UA Sentiment Auditor",
        body: "Live Reddit + forum classification of Unearthed Arcana playtest reaction. AI Bouncer tags 'math broken' / 'flavor bad' / 'fun to play'. 48-hour signal vs 4-week survey lag.",
      },
      {
        head: "Homebrew Gap Identifier",
        body: "When the community is patching what you're not shipping. UA + GMBinder + DDB Homebrew velocity by mechanical category — quantitative proof of a rules-gap to fill.",
      },
      {
        head: "Mechanic Demand Detection",
        body: "Cross-stream rising signal for class types, settings, mechanics. Surface 'witch class', 'cozy fantasy', 'tactical horror' demand 12-18 months before mainstream peak.",
      },
    ],
  },
  {
    label: "MARKETERS",
    color: C.amber,
    sub: "What sells, and how do we say it?",
    headline: "Backlash early warning",
    cases: [
      {
        head: "OGL-Tier Backlash Detector  ★",
        body: "Daily-cadence community sentiment. If WotC pre-hints a move (Asmodee printing partnership, OGL revision, AI policy change), Trusight surfaces the reaction within 24 hours — before commitment. Not a minute-level crisis monitor; a trial-balloon detector.",
      },
      {
        head: "Creator ROI Attribution",
        body: "Did that Critical Role / Dimension 20 sponsorship actually move product? Track Google Trends + Fandom + DDB lookup velocity around the specific monsters / modules used in the episode.",
      },
      {
        head: "Audience Segmentation",
        body: "AO3-heavy vs Reddit-heavy vs BGG-heavy IPs map to narrative-fan / mechanics-fan / buyer segments. Tailor channel mix by where each IP's fans actually live.",
      },
    ],
  },
  {
    label: "IP & LICENSING",
    color: C.green,
    sub: "What should we acquire?",
    headline: "12-18 month forecasting radar",
    cases: [
      {
        head: "UB Matrix (the IP Decision Engine)",
        body: "142 candidate IPs scored on Fit / Reception / Acquisition / Commercial. See companion one-pager — Hollow Knight rises, Spy × Family falls. Cross-source triangulation rules out single-source noise.",
      },
      {
        head: "Buyers-Remorse Landmine Detector",
        body: "BGG board-game adaptation reception + AO3 zero-crossover signal flag IPs that look great on paper but the tabletop community will reject. Saves 7-figure licensing fees on Walking-Dead-tier risks.",
      },
      {
        head: "Reverse-Funnel Acquirer",
        body: "Scans non-D&D IP subreddits for 'D&D' / '5e' / 'campaign' mentions. Surfaces communities pre-converted to D&D — near-zero CAC targets like Hollow Knight or Dungeon Crawler Carl.",
      },
    ],
  },
];

personas.forEach((p, i) => {
  const x = colStart + i * (colW + colGap);

  // Column card
  slide.addShape(pres.shapes.RECTANGLE, {
    x, y: colY, w: colW, h: colH,
    fill: { color: C.obsidian2 }, line: { color: p.color, width: 1.5 },
  });

  // Persona label (big, color-coded)
  slide.addText(p.label, {
    x: x + 0.15, y: colY + 0.18, w: colW - 0.30, h: 0.32,
    fontSize: 14, fontFace: FONT_HEAD, bold: true,
    color: p.color, align: "left", charSpacing: 2, margin: 0,
  });
  // Sub-question
  slide.addText(p.sub, {
    x: x + 0.15, y: colY + 0.50, w: colW - 0.30, h: 0.28,
    fontSize: 10, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "left", margin: 0,
  });
  // "Killer feature" headline
  slide.addText(p.headline, {
    x: x + 0.15, y: colY + 0.82, w: colW - 0.30, h: 0.30,
    fontSize: 11, fontFace: FONT_HEAD, bold: true,
    color: C.white, align: "left", margin: 0,
  });

  // Three use-case rows
  p.cases.forEach((c, j) => {
    const rowY = colY + 1.30 + j * 1.40;
    // Case head
    slide.addText(c.head, {
      x: x + 0.15, y: rowY, w: colW - 0.30, h: 0.28,
      fontSize: 10.5, fontFace: FONT_HEAD, bold: true,
      color: p.color, align: "left", margin: 0,
    });
    // Case body
    slide.addText(c.body, {
      x: x + 0.15, y: rowY + 0.30, w: colW - 0.30, h: 1.05,
      fontSize: 8.5, fontFace: FONT_BODY,
      color: C.cream, align: "left", margin: 0,
    });
  });
});

// ─── PLAYING TO WIN PILLAR MAPPING ────────────────────────────────────
const pwY = 8.55;
const pwH = 1.10;
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0.5, y: pwY, w: 7.5, h: pwH,
  fill: { color: C.obsidian2 }, line: { color: C.ember, width: 1 },
});
slide.addText("MAPS TO HASBRO'S PLAYING-TO-WIN STRATEGY", {
  x: 0.65, y: pwY + 0.10, w: 7.2, h: 0.25,
  fontSize: 10, fontFace: FONT_HEAD, bold: true,
  color: C.ember, charSpacing: 3, margin: 0,
});
// Four pillars in a row
const pillars = [
  { name: "Profitable Franchises", how: "high-resolution D&D-segment health" },
  { name: "Aging Up",              how: "kidult-tier engagement signal" },
  { name: "Digital & Direct",       how: "DDB / VTT signal layer + APIs" },
  { name: "Partner Scaled",         how: "UB Matrix de-risks every license" },
];
const pillarW = 1.78;
const pillarStartX = 0.65;
pillars.forEach((pp, i) => {
  const x = pillarStartX + i * (pillarW + 0.05);
  slide.addText(pp.name, {
    x, y: pwY + 0.42, w: pillarW, h: 0.22,
    fontSize: 10, fontFace: FONT_HEAD, bold: true,
    color: C.white, align: "left", margin: 0,
  });
  slide.addText(pp.how, {
    x, y: pwY + 0.65, w: pillarW, h: 0.40,
    fontSize: 9, fontFace: FONT_BODY, italic: true,
    color: C.cream, align: "left", margin: 0,
  });
});

// ─── FOOTER ───────────────────────────────────────────────────────────
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0.5, y: 9.85, w: 7.5, h: 0.04,
  fill: { color: C.ember }, line: { type: "none" },
});
slide.addText([
  { text: "12+ LIVE SIGNAL STREAMS",   options: { color: C.ember, bold: true, fontFace: FONT_HEAD, charSpacing: 2 } },
  { text: "  ·  ",   options: { color: C.mutedGray } },
  { text: "8-STAGE BIGQUERY PIPELINE",  options: { color: C.ember, bold: true, fontFace: FONT_HEAD, charSpacing: 2 } },
  { text: "  ·  ",   options: { color: C.mutedGray } },
  { text: "AI BOUNCER",                 options: { color: C.ember, bold: true, fontFace: FONT_HEAD, charSpacing: 2 } },
], {
  x: 0.5, y: 10.00, w: 7.5, h: 0.28,
  fontSize: 10, align: "center", margin: 0,
});
slide.addText([
  { text: "Lands as Snowflake Secure Share into your Looker dashboards. No new BI tool.",
    options: { color: C.cream, italic: true, fontFace: FONT_BODY } },
], {
  x: 0.5, y: 10.30, w: 7.5, h: 0.22,
  fontSize: 9, align: "center", margin: 0,
});

// Contact line
slide.addText([
  { text: "Phil Trubbel  ·  ", options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "phil@trusightdata.ai",  options: { color: C.cream, fontFace: FONT_BODY } },
  { text: "  ·  trusightdata.ai",  options: { color: C.cream, fontFace: FONT_BODY } },
], {
  x: 0.5, y: 10.62, w: 7.5, h: 0.28,
  fontSize: 11, align: "center", margin: 0,
});

// ─── Write file ───────────────────────────────────────────────────────
pres.writeFile({ fileName: "trusight_onepager_capabilities.pptx" })
  .then((fileName) => console.log(`Wrote: ${fileName}`));
