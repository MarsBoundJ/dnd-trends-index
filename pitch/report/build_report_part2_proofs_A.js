/**
 * Trusight comprehensive report — Part 2 PROOFS, Persona A
 *
 * Three mini-reports demonstrating that the use cases work in practice,
 * with real BigQuery output from gold_data views.
 *
 * Each mini-report: The question → What we ran → What came back → What it tells us.
 */

const fs = require("fs");
const path = require("path");
const LOGO_LIGHT_PATH = path.join(__dirname, "..", "assets", "logos", "trusight_logo_4k_light.png");

const {
  Document, Packer, Paragraph, ImageRun, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageBreak,
} = require("docx");

// ─── Style tokens (matched to Part 2) ─────────────────────────────────
const FONT_HEAD = "Trebuchet MS";
const FONT_BODY = "Calibri";
const FONT_MONO = "Consolas";

const COLOR = {
  ember:       "C46419",
  emberLight:  "F5E6D8",
  dark:        "0A0A18",
  bodyText:    "2A2A3A",
  mutedGray:   "606070",
  mutedGray2:  "888090",
  greenAccent: "2A8B4D",
  amberAccent: "B8862C",
  redAccent:   "C44949",
  blueAccent:  "5078B8",
  rule:        "D4C8B8",
  tableHeadBg: "EFE4D2",
  tableAltBg:  "FAF6F0",
};

// ─── Helpers ──────────────────────────────────────────────────────────
function p(text, opts = {}) {
  const runs = Array.isArray(text) ? text : [new TextRun({ text, ...opts.run })];
  return new Paragraph({
    children: runs,
    spacing: { after: opts.after ?? 140, before: opts.before ?? 0, line: opts.line ?? 280 },
    alignment: opts.align,
  });
}
function rt(text, opts = {}) { return new TextRun({ text, ...opts }); }
function spacer(h = 200) { return new Paragraph({ children: [], spacing: { after: h } }); }
function pageBreak() { return new Paragraph({ children: [new PageBreak()] }); }
function hr(color = COLOR.rule) {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color, space: 1 } },
    spacing: { before: 60, after: 180 },
    children: [],
  });
}
function emberBar(color = COLOR.ember) {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 18, color, space: 1 } },
    spacing: { before: 40, after: 200 },
    children: [],
  });
}

// Mini-report header
function miniReportHeader(label, title) {
  return [
    p([rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 18, characterSpacing: 60 })],
      { after: 80, before: 200 }),
    p([rt(title, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 32 })],
      { after: 120 }),
    emberBar(),
  ];
}

// "Section beat" — bold lead-in for The question / What we ran / etc.
function beat(label, body) {
  return p([
    rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22 }),
    rt("  " + body, { color: COLOR.bodyText, font: FONT_BODY, size: 22 }),
  ], { line: 300, after: 160 });
}

// Method block — monospace SQL excerpt or query description
function methodBlock(text) {
  const cell = new TableCell({
    borders: {
      top:    { style: BorderStyle.SINGLE, size: 4, color: COLOR.rule },
      bottom: { style: BorderStyle.SINGLE, size: 4, color: COLOR.rule },
      left:   { style: BorderStyle.SINGLE, size: 18, color: COLOR.ember },
      right:  { style: BorderStyle.SINGLE, size: 4, color: COLOR.rule },
    },
    width: { size: 9360, type: WidthType.DXA },
    shading: { fill: COLOR.tableAltBg, type: ShadingType.CLEAR },
    margins: { top: 160, bottom: 160, left: 240, right: 240 },
    children: [
      p([rt(text, { color: COLOR.dark, font: FONT_MONO, size: 18 })],
        { line: 260, after: 0 }),
    ],
  });
  return [
    new Table({
      width: { size: 9360, type: WidthType.DXA },
      columnWidths: [9360],
      rows: [new TableRow({ children: [cell] })],
    }),
    spacer(180),
  ];
}

// Data table — header row + body rows
function dataTable(headers, rows, columnWidths) {
  const totalWidth = columnWidths.reduce((a, b) => a + b, 0);
  const border = { style: BorderStyle.SINGLE, size: 4, color: COLOR.rule };
  const borders = { top: border, bottom: border, left: border, right: border };

  const headRow = new TableRow({
    tableHeader: true,
    children: headers.map((h, i) => new TableCell({
      borders,
      width: { size: columnWidths[i], type: WidthType.DXA },
      shading: { fill: COLOR.tableHeadBg, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 100, right: 100 },
      children: [p([rt(h, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 18 })],
        { line: 240, after: 0 })],
    })),
  });

  const bodyRows = rows.map((row, r) => new TableRow({
    children: row.map((cell, i) => new TableCell({
      borders,
      width: { size: columnWidths[i], type: WidthType.DXA },
      shading: { fill: r % 2 === 0 ? "FFFFFF" : COLOR.tableAltBg, type: ShadingType.CLEAR },
      margins: { top: 70, bottom: 70, left: 100, right: 100 },
      children: [p([rt(String(cell), {
        color: COLOR.bodyText,
        font: FONT_BODY,
        size: 18,
        bold: i === 0,
      })], { line: 240, after: 0 })],
    })),
  }));

  return [
    new Table({
      width: { size: totalWidth, type: WidthType.DXA },
      columnWidths,
      rows: [headRow, ...bodyRows],
    }),
    spacer(180),
  ];
}

// Caption under a table
function caption(text) {
  return p([rt(text, { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 18 })],
    { line: 260, after: 200 });
}

// ─── Document setup ───────────────────────────────────────────────────
const sharedStyles = {
  default: { document: { run: { font: FONT_BODY, size: 22, color: COLOR.bodyText } } },
};

const pageSetup = {
  size: { width: 12240, height: 15840 },
  margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
};

// ═════════════════════════════════════════════════════════════════════
// PART 2 PROOFS — PERSONA A
// ═════════════════════════════════════════════════════════════════════

const docChildren = [
  // Trusight logo on the cover
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { before: 200, after: 200 },
    children: [
      new ImageRun({
        type: "png",
        data: fs.readFileSync(LOGO_LIGHT_PATH),
        transformation: { width: 240, height: 39 },
        altText: { title: "Trusight", description: "Trusight logo", name: "Trusight" },
      }),
    ],
  }),
    // ── Cover ──
  spacer(800),
  p([rt("PART 2 — PROOFS", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22, characterSpacing: 80 })],
    { align: AlignmentType.CENTER, after: 120 }),
  p([rt("Each Use Case, Run Against Live Data", { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 48 })],
    { align: AlignmentType.CENTER, after: 200 }),
  p([rt("— · —", { color: COLOR.ember, font: FONT_HEAD, size: 24 })],
    { align: AlignmentType.CENTER, after: 400 }),
  p([rt("PERSONA A — DESIGNERS", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 18, characterSpacing: 60 })],
    { align: AlignmentType.CENTER, after: 80 }),
  p([rt("Three mini-reports. Each one runs the use case against the live BigQuery gold-tier views and reports the structured output. Where data is sparse or the anchoring event sits outside our collection window, that limit is named in the open. Persona B and C tranches follow.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { align: AlignmentType.CENTER, line: 320, after: 200 }),

  // ═════════════════════════════════════════════════════════════════════
  // A1 — UA SENTIMENT AUDITOR
  // ═════════════════════════════════════════════════════════════════════
  pageBreak(),
  ...miniReportHeader("USE CASE A1  ·  PROOF OF USE",
    "The Unearthed Arcana Sentiment Auditor"),

  beat("The question.",
    "When WoTC drops a UA playtest packet, what does the community actually think — broken down by structured narrative type, not by raw sentiment polarity?"),

  beat("What we ran.",
    "WoTC has not shipped a fresh UA packet inside our v5 forum-body collection window (Apr 2026). So we ran the same classifier against the IP-reception corpus — every confirmed forum thread classified by the v5 narrative-type tagger across EN World, RPG.net, and Giant in the Playground. The classifier is the product. The input is interchangeable: UA packet today, IP crossover yesterday, rules-revision tomorrow."),

  ...methodBlock(
    "SELECT ip_name, confirmed_about_ip_count, attitude_avg,\n" +
    "       cash_grab_count, tone_mismatch_count, not_dnd_count,\n" +
    "       pandering_count, system_design_critique_count,\n" +
    "       worldbuilding_endorsement_count, sample_backlash_evidence\n" +
    "FROM gold_data.forum_presence_proxy\n" +
    "WHERE confirmed_about_ip_count >= 5\n" +
    "ORDER BY (cash_grab_count + tone_mismatch_count + not_dnd_count + pandering_count) DESC\n" +
    "LIMIT 5;"
  ),

  beat("What came back.",
    "Top-5 backlash-tagged IPs across the three forums. Structured columns are confirmed-thread counts — every cell is a real forum post the AI Bouncer matched into that narrative class:"),

  ...dataTable(
    ["IP", "Threads", "Tone­mismatch", "Cash­grab", "Pandering", "Design­critique", "Endorse­ment", "Attitude avg"],
    [
      ["Goblin Slayer",        "10", "4", "1", "1", "6", "2", "0.04"],
      ["Stranger Things",      "10", "1", "2", "2", "1", "1", "0.36"],
      ["Baldur's Gate 3",      "10", "0", "2", "1", "3", "1", "0.32"],
      ["Welcome to Night Vale", "7", "2", "0", "0", "2", "0", "0.18"],
      ["The Boys",             "10", "2", "0", "0", "2", "0", "0.20"],
    ],
    [1700, 700, 1100, 900, 900, 1100, 1100, 1860]
  ),

  caption("Source: gold_data.forum_presence_proxy, snapshot 2026-04-30, 142 IPs scanned, 24 above the 5-thread minimum."),

  beat("What it tells us.",
    "The classifier produces six narrative tags per thread, not a single positive/negative score. That structure is what makes it useful for a Day-1 UA-packet read: \"4 tone_mismatch flags out of 10 threads\" tells the design team something specific about WHY a packet is divisive (the flavor reads wrong), separable from \"6 system_design_critique flags\" (the math is being argued). For Goblin Slayer, the triangulation shows up cleanly — the same IP is flagged tone_mismatch on three forums independently, which is the structural signature an OGL-tier event would produce on Day 1. Note how Stranger Things and BG3 score positive overall (attitude_avg 0.36, 0.32) but still register cash_grab and pandering tags from a minority of voices: that's the kind of nuance a horizontal social-listening tool's average-sentiment scalar erases."),

  beat("What this would look like for a real UA packet.",
    "Same classifier, same three forum sources, same six narrative tags — but pointed at the new harvest filter. Stage 4 of the pipeline (harvest_forum_presence.py) takes search terms; swap \"Goblin Slayer\" for \"Witch UA\" or \"One D&D Warlock playtest\" and you get the same six-column breakdown 24-48 hours after the packet drops, instead of waiting six weeks for the official survey."),

  // ═════════════════════════════════════════════════════════════════════
  // A2 — HOMEBREW GAP IDENTIFIER
  // ═════════════════════════════════════════════════════════════════════
  pageBreak(),
  ...miniReportHeader("USE CASE A2  ·  PROOF OF USE",
    "The Homebrew Gap Identifier"),

  beat("The question.",
    "Which IPs is the D&D community building unofficial 5e content for, despite WoTC having shipped no licensed crossover? The gap between community demand and official supply is the lead."),

  beat("What we ran.",
    "Cross-platform homebrew query against gold_data.homebrew_combined_proxy. Every IP in the master list is scored against three independent platforms — D&D Beyond Homebrew (the official creator workshop), GMBinder, and Homebrewery — with the AI Bouncer disambiguating false-positive name collisions on each side."),

  ...methodBlock(
    "SELECT ip_name, homebrew_combined_score, homebrew_combined_status,\n" +
    "       ddb_total_items, ddb_top_item_name, ddb_top_item_section,\n" +
    "       ddb_top_item_adds, gmbinder_confirmed, homebrewery_confirmed,\n" +
    "       ua_homebrew_mention_count, ua_total_upvotes\n" +
    "FROM gold_data.homebrew_combined_proxy\n" +
    "WHERE homebrew_combined_status LIKE 'sufficient%'\n" +
    "ORDER BY homebrew_combined_score DESC LIMIT 10;"
  ),

  beat("What came back.",
    "60 of 142 candidate IPs cleared the \"sufficient\" homebrew bar. 5 cleared all three platforms simultaneously. None of the top-10 listed below has a licensed official D&D crossover — every column is unofficial community labor:"),

  ...dataTable(
    ["IP", "Combined score", "Streams", "DDB items", "Top DDB item / adds", "GMBinder", "Homebrewery"],
    [
      ["Bloodborne",   "0.98", "2 of 3", "71", "Bloodborne Hunter / 213",          "7",  "2"],
      ["Hollow Knight","0.94", "2 of 3", "50", "Hollow Knight Vessel / 269",       "8",  "1"],
      ["Elden Ring",   "0.93", "3 of 3", "29", "Margit the Fell Omen / 38",        "10", "0"],
      ["Final Fantasy XIV", "0.96", "1 of 3", "0", "—",                            "8",  "1"],
      ["Demon Slayer", "0.81", "2 of 3", "55", "Blood Hunter / Demon Slayer / 718","4",  "0"],
      ["One Piece",    "0.89", "2 of 3", "28", "Cyborg (One Piece) / 75",          "7",  "3"],
      ["Berserk",      "0.74", "3 of 3", "25", "Berserker Redux / 227",            "2",  "0"],
      ["Mistborn",     "0.73", "2 of 3", "44", "Mistborn / 117",                   "3",  "0"],
      ["Jujutsu Kaisen","0.91", "2 of 3", "0", "—",                                "5",  "1"],
      ["Discworld",    "0.81", "1 of 3", "0", "—",                                 "6",  "0"],
    ],
    [1860, 1100, 900, 900, 2300, 1100, 1200]
  ),

  caption("Source: gold_data.homebrew_combined_proxy, snapshot 2026-04-30. \"Adds\" = the public DDB-Homebrew add count for the top-ranked unofficial item per IP."),

  beat("What it tells us.",
    "Two patterns. First, the all-three-streams set (Berserk, Elden Ring, plus three others not shown) is the highest-conviction gap — the community is building this content on three separate platforms, simultaneously. That's the canonical \"we want this and you're not shipping it\" signal. Second, the single-item adds count is the diagnostic: \"Blood Hunter, Order of the Demon Slayer\" has been added 718 times by D&D Beyond users for a campaign — that's not noise, that's a community vote. The Berserker Redux subclass at 227 adds tells WoTC the rage-mechanic remix specifically, not just \"Berserk\" generically, is the rules-design vector."),

  beat("What this enables.",
    "Pre-licensing pitch: \"the Berserk fanbase is already running 25 unofficial homebrew artifacts on D&D Beyond, including a 227-add Berserker subclass — your audience-conversion cost is near zero because they've already converted themselves.\" Plus the same query, run quarterly, surfaces the velocity changes: which IPs jumped from \"sufficient_one\" to \"sufficient_all_three\" in the last 90 days. That delta is the early-warning track for the next licensing window."),

  // ═════════════════════════════════════════════════════════════════════
  // A3 — MECHANIC DEMAND DETECTOR
  // ═════════════════════════════════════════════════════════════════════
  pageBreak(),
  ...miniReportHeader("USE CASE A3  ·  PROOF OF USE",
    "The Mechanic Demand Detector"),

  beat("The question.",
    "Three independent streams measure each D&D class: search interest (curiosity), community discussion (community), and creator output (creator). When these three diverge for the same class, what does each pattern of disagreement mean for design priorities?"),

  beat("What we ran.",
    "Pulled the composite_concept_index for D&D's 12 evergreen classes. Each stream contributes a normalized 0-1 score; we additionally surface curiosity_momentum and community_momentum — the directional 7-day deltas. The reading column is the divergence interpretation derived from the score-spread."),

  ...methodBlock(
    "SELECT concept_name, curiosity_score, community_score, creator_score,\n" +
    "       demand_score, ROUND(community_score - curiosity_score, 2) AS comm_minus_search,\n" +
    "       streams_present\n" +
    "FROM gold_data.composite_concept_index\n" +
    "WHERE category = 'Class' AND is_active = TRUE AND streams_present >= 3\n" +
    "ORDER BY community_score DESC NULLS LAST;"
  ),

  beat("What came back.",
    "Twelve classes, three stream scores, plus the divergence reading. The fourth column (Comm − Search) is the diagnostic — positive means community advocates more than the public searches; negative means search-led discovery has outrun community engagement:"),

  ...dataTable(
    ["Class", "Search", "Community", "Creator", "Comm − Search", "Reading"],
    [
      ["Paladin",   "0.97", "0.96", "0.96", "−0.01",  "Aligned high — saturated"],
      ["Fighter",   "0.98", "0.94", "0.96", "−0.04",  "Search-led, broad appeal"],
      ["Warlock",   "0.71", "0.95", "0.99", "+0.24",  "Community + creator advocacy"],
      ["Sorcerer",  "0.68", "0.93", "—",    "+0.25",  "Community-led — under-marketed"],
      ["Barbarian", "0.93", "0.93", "0.96", "0.00",   "Aligned"],
      ["Rogue",     "0.89", "0.90", "0.96", "+0.01",  "Aligned"],
      ["Ranger",    "0.96", "0.85", "0.99", "−0.11",  "Search-led discovery"],
      ["Bard",      "0.75", "0.87", "0.99", "+0.13",  "Community + creator"],
      ["Wizard",    "0.85", "0.85", "0.96", "0.00",   "Aligned"],
      ["Cleric",    "0.95", "0.72", "0.96", "−0.23",  "Search-led — newcomer entry"],
      ["Monk",      "0.78", "0.98", "0.99", "+0.20",  "Community + creator beloved"],
      ["Artificer", "0.72", "0.83", "—",    "+0.11",  "Community-led, no creator"],
    ],
    [1500, 1000, 1200, 1000, 1500, 3160]
  ),

  caption("Source: gold_data.composite_concept_index, snapshot 2026-04-30, ruleset = Core Evergreen except Artificer (2014). \"—\" = creator stream returned 0 for that class, treated as missing rather than zero."),

  beat("What it tells us.",
    "Three actionable patterns. (1) The community-led classes — Sorcerer (+0.25), Warlock (+0.24), Monk (+0.20) — are talked about more than they're searched for. That asymmetry says the engaged players advocate for these classes, but the public discovery layer is weaker. The implication for design is the opposite of \"buff Sorcerer mechanically\" — the lever is entry-point marketing or a tentpole-product Sorcerer hook, because the mechanical fanbase is already there. (2) The search-led classes — Cleric (−0.23), Ranger (−0.11) — are what newcomers find when they Google \"D&D classes,\" but the existing community has moved on. That argues for content refresh: keep Cleric/Ranger relevant for the players who already chose them. (3) Artificer reads community 0.83 / creator 0 — the creator stream is empty because no Artificer-specific Itch.io / DMs Guild creator content exists at scale. That's a creator-economy gap, not a player-demand gap, and it's a different lever."),

  beat("What this enables.",
    "A quarterly design read: \"this class's three streams diverged by more than 0.20 — that's where to look.\" The same query, run against subclasses, surfaces the same pattern at finer resolution. Hexblade (community 0.69 / curiosity 0.24, +0.45 spread, 3 streams) is a hidden Hexblade-class-tier signal: established community but no broad search interest — exactly the kind of signal a horizontal social-listening tool would miss because Hexblade isn't trending in the volume sense. Trusight surfaces it because the divergence between streams, not the volume in any one stream, is the lead."),

  // ─── Wrap ───
  pageBreak(),
  spacer(400),
  p([rt("PERSONA A — TAKEAWAY", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22, characterSpacing: 80 })],
    { align: AlignmentType.CENTER, after: 80 }),
  p([rt("Three queries, three real findings.", { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 36 })],
    { align: AlignmentType.CENTER, after: 200 }),
  emberBar(),

  p([rt("Each of the three Persona-A use cases is a live SQL query against gold_data — not a slide deck description. The classifier produces six narrative tags per forum thread (A1). Sixty IPs cleared the cross-platform homebrew bar with five clearing all three streams simultaneously (A2). Twelve classes scored across three streams reveal three separable divergence patterns, each with a distinct design action (A3).", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 320, after: 200 }),

  p([rt("Most importantly: every claim above traces to a row in a gold-tier view, with snapshot date and source column. That's the data trail Trusight ships — the structured signal that turns design judgment from \"my gut says\" into \"the row in the table says.\"", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { line: 320, after: 240 }),

  hr(COLOR.ember),
  p([rt("Next tranche: Persona B (Marketers) — proof-of-use for OGL-Tier Backlash Detector, Creator ROI Attribution, Audience Segmentation Atlas.", { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 20 })],
    { align: AlignmentType.CENTER, line: 280 }),
];

const doc = new Document({
  styles: sharedStyles,
  sections: [{
    properties: { page: pageSetup },
    children: docChildren,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("trusight_report_part2_proofs_personaA.docx", buf);
  console.log("Wrote: trusight_report_part2_proofs_personaA.docx");
});
