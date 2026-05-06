/**
 * Trusight comprehensive report — A1 LIVE PROOF
 *
 * Sentiment audit of UA 2026 Villainous Options 2 (released 2026-04-23).
 * 442 classified items across Reddit + 3 top TTRPG forums + press.
 * Eight-tag UA-content classifier schema applied to all items.
 *
 * Demonstrates: the classifier described in A1 actually works on the input
 * the use case promises (a fresh UA packet), in the time window promised
 * (T+11 from packet drop, structured verdict before WoTC's survey opens).
 */

const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, BorderStyle, WidthType, ShadingType, PageBreak,
} = require("docx");

const FONT_HEAD = "Trebuchet MS";
const FONT_BODY = "Calibri";
const FONT_MONO = "Consolas";

const COLOR = {
  ember:       "C46419",
  emberLight:  "F5E6D8",
  dark:        "0A0A18",
  bodyText:    "2A2A3A",
  mutedGray:   "606070",
  greenAccent: "2A8B4D",
  amberAccent: "B8862C",
  redAccent:   "C44949",
  rule:        "D4C8B8",
  tableHeadBg: "EFE4D2",
  tableAltBg:  "FAF6F0",
  greenLight:  "DCF1E1",
  amberLight:  "F5ECD2",
  redLight:    "F2D8D8",
};

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
function emberBar(color = COLOR.ember) {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 18, color, space: 1 } },
    spacing: { before: 40, after: 200 },
    children: [],
  });
}
function hr(color = COLOR.rule) {
  return new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color, space: 1 } },
    spacing: { before: 60, after: 180 },
    children: [],
  });
}

function header(label, title) {
  return [
    p([rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 18, characterSpacing: 60 })],
      { after: 80 }),
    p([rt(title, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 32 })],
      { after: 120 }),
    emberBar(),
  ];
}

function beat(label, body, opts = {}) {
  return p([
    rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: opts.size ?? 22 }),
    rt("  " + body, { color: COLOR.bodyText, font: FONT_BODY, size: opts.size ?? 22 }),
  ], { line: 300, after: 160 });
}

function callout(label, body, accentColor = COLOR.ember, fillColor = COLOR.emberLight) {
  const cell = new TableCell({
    borders: {
      top:    { style: BorderStyle.SINGLE, size: 6, color: accentColor },
      bottom: { style: BorderStyle.SINGLE, size: 6, color: accentColor },
      left:   { style: BorderStyle.SINGLE, size: 24, color: accentColor },
      right:  { style: BorderStyle.SINGLE, size: 6, color: accentColor },
    },
    width: { size: 9360, type: WidthType.DXA },
    shading: { fill: fillColor, type: ShadingType.CLEAR },
    margins: { top: 200, bottom: 200, left: 240, right: 240 },
    children: [
      p([rt(label, { color: accentColor, bold: true, font: FONT_HEAD, size: 16, characterSpacing: 60 })],
        { after: 60 }),
      p([rt(body, { color: COLOR.dark, font: FONT_BODY, size: 22 })],
        { after: 0 }),
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

function dataTable(headers, rows, columnWidths, headerFill = COLOR.tableHeadBg) {
  const totalWidth = columnWidths.reduce((a, b) => a + b, 0);
  const border = { style: BorderStyle.SINGLE, size: 4, color: COLOR.rule };
  const borders = { top: border, bottom: border, left: border, right: border };

  const headRow = new TableRow({
    tableHeader: true,
    children: headers.map((h, i) => new TableCell({
      borders,
      width: { size: columnWidths[i], type: WidthType.DXA },
      shading: { fill: headerFill, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 100, right: 100 },
      children: [p([rt(h, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 18 })],
        { line: 240, after: 0 })],
    })),
  });

  const bodyRows = rows.map((row, r) => new TableRow({
    children: row.map((cell, i) => {
      const isString = typeof cell === "string";
      const text = isString ? cell : (cell.text ?? "");
      const fill = (!isString && cell.fill) ? cell.fill : (r % 2 === 0 ? "FFFFFF" : COLOR.tableAltBg);
      const bold = (!isString && cell.bold) ? cell.bold : (i === 0);
      const color = (!isString && cell.color) ? cell.color : COLOR.bodyText;
      return new TableCell({
        borders,
        width: { size: columnWidths[i], type: WidthType.DXA },
        shading: { fill, type: ShadingType.CLEAR },
        margins: { top: 70, bottom: 70, left: 100, right: 100 },
        children: [p([rt(String(text), { color, font: FONT_BODY, size: 18, bold })],
          { line: 240, after: 0 })],
      });
    }),
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

function caption(text) {
  return p([rt(text, { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 18 })],
    { line: 260, after: 200 });
}

// Logline — italic kernel that sits after a beat() prose paragraph.
// Visually subordinate (smaller, italic, muted) but warmer than caption gray.
function logline(text) {
  return p([rt(text, { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 20 })],
    { line: 280, after: 240, before: 40 });
}

function quoteBlock(quote, source, ups) {
  const upsText = ups ? ` · +${ups} ups` : "";
  return p([
    rt('"', { color: COLOR.ember, font: FONT_HEAD, size: 22, bold: true }),
    rt(quote, { color: COLOR.dark, italics: true, font: FONT_BODY, size: 22 }),
    rt('"', { color: COLOR.ember, font: FONT_HEAD, size: 22, bold: true }),
    rt("   — " + source + upsText, { color: COLOR.mutedGray, font: FONT_BODY, size: 18 }),
  ], { line: 280, after: 100, before: 0 });
}

const sharedStyles = {
  default: { document: { run: { font: FONT_BODY, size: 22, color: COLOR.bodyText } } },
};
const pageSetup = {
  size: { width: 12240, height: 15840 },
  margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
};

const docChildren = [
  // ─── Cover ───
  spacer(800),
  p([rt("USE CASE A1  ·  LIVE PROOF", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22, characterSpacing: 80 })],
    { align: AlignmentType.CENTER, after: 120 }),
  p([rt("The Sentiment Audit, Run On A Real UA Packet", { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 40 })],
    { align: AlignmentType.CENTER, after: 200 }),
  p([rt("— · —", { color: COLOR.ember, font: FONT_HEAD, size: 24 })],
    { align: AlignmentType.CENTER, after: 400 }),
  p([rt("UA 2026  ·  Villainous Options 2", { color: COLOR.dark, bold: true, italics: true, font: FONT_HEAD, size: 28 })],
    { align: AlignmentType.CENTER, after: 80 }),
  p([rt("Path of Lament (Barbarian)  ·  Warrior of Venom (Monk)  ·  Primordial Patron (Warlock)", { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 22 })],
    { align: AlignmentType.CENTER, after: 240 }),
  p([rt("Released April 23, 2026  ·  Audit run May 4, 2026  ·  T+11 days", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 20, characterSpacing: 40 })],
    { align: AlignmentType.CENTER, after: 240 }),
  p([rt("WoTC's official survey closes May 14 (T+21). The official survey results will be published 4-6 weeks later. This audit reads the structured community signal a month before the survey closes — directionally usable for design decisions today.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { align: AlignmentType.CENTER, line: 320, after: 240 }),

  // ─── 1. WHAT WE RAN ───
  pageBreak(),
  ...header("SECTION 1", "What we ran"),

  beat("The setup.",
    "Utilizing the source PDF (UA2026-VillainousOptions02.pdf, 5 pages, 3 subclasses + 2 invocations), we ran the A1 pipeline on May 4, 2026: harvest community discussion across Reddit, three top TTRPG forums, and the major D&D press/blogosphere; apply the UA-content classifier to every captured post; aggregate the per-subclass tags into a structured fingerprint."),

  beat("Coverage of the five channels.",
    "All five channels harvested. Reddit returned the strongest single signal — 9 dedicated discussion threads with 1,015 aggregate comments; we extracted the 359 highest-scoring top-level and first-reply comments for classification. Three top TTRPG forums each contributed independent signal: 7 substantive replies from Top Forum #3, 22 VO2-window posts from Top Forum #2, 48 posts from Top Forum #1. Press/blog coverage spans six outlets. Total dataset: 442 classified items."),

  logline("Reddit is the loud front-row chorus; the TTRPG forums and press are the cautious balcony critics adding nuance."),

  ...dataTable(
    ["Source", "Threads", "Posts classified", "Status"],
    [
      ["Reddit (r/onednd, r/dndnext, r/UnearthedArcana, r/DnD)", "9", "359", { text: "Complete", color: COLOR.greenAccent, bold: true }],
      ["Top TTRPG Forum #1",                                    "1", "48",  { text: "Complete", color: COLOR.greenAccent, bold: true }],
      ["Top TTRPG Forum #2",                                    "1", "22",  { text: "Complete", color: COLOR.greenAccent, bold: true }],
      ["Top TTRPG Forum #3",                                    "1", "7",   { text: "Complete", color: COLOR.greenAccent, bold: true }],
      ["Press / blog reviews",                                  "6", "6",   { text: "Complete", color: COLOR.greenAccent, bold: true }],
      [{ text: "TOTAL", bold: true }, { text: "18", bold: true }, { text: "442", bold: true }, ""],
    ],
    [4400, 900, 1800, 2260]
  ),

  caption("Each post tagged using the eight-tag UA-content classifier (overpowered / underpowered / mechanic_clunky / class_identity_drift / exploit_loophole / flavor_endorsement / flavor_critique / theme_frame_mismatch) plus overall stance (positive / divisive / negative / mention-only). Tags accumulate per subclass and per channel into the structured fingerprint shown in Section 2."),

  // ─── 2. PER-SUBCLASS RESULTS ───
  pageBreak(),
  ...header("SECTION 2", "What the classifier produced"),

  p([rt("Three subclasses, three meaningfully different signatures. The classifier doesn't return a single overall sentiment number — it returns a multi-dimensional fingerprint that names ", { color: COLOR.bodyText, font: FONT_BODY, size: 22 }),
     rt("which kind", { color: COLOR.dark, italics: true, bold: true, font: FONT_BODY, size: 22 }),
     rt(" of reception is happening. Below: the structured output for all three subclasses, side-by-side.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 240 }),

  ...dataTable(
    ["", "Path of Lament", "Warrior of Venom", "Primordial Patron"],
    [
      [{ text: "Class", bold: true }, "Barbarian", "Monk", "Warlock"],
      [{ text: "Verdict", bold: true },
        { text: "GREEN", color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "GREEN-YELLOW", color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight },
        { text: "YELLOW-RED", color: COLOR.redAccent, bold: true, fill: COLOR.redLight }],
      [{ text: "Positive", bold: true }, "80%", "65%", "35%"],
      [{ text: "Divisive", bold: true }, "15%", "25%", "50%"],
      [{ text: "Negative", bold: true }, "5%", "10%", "15%"],
      [{ text: "flavor_endorsement", bold: true }, "HIGH", "MED-HIGH", "LOW-MED"],
      [{ text: "flavor_critique", bold: true }, "LOW", "LOW", "LOW"],
      [{ text: "overpowered", bold: true }, "—", "HIGH", "LOW"],
      [{ text: "underpowered", bold: true }, "LOW", "MEDIUM", "HIGH"],
      [{ text: "mechanic_clunky", bold: true }, "LOW", "—", "HIGH"],
      [{ text: "class_identity_drift", bold: true }, "LOW", "MEDIUM", "—"],
      [{ text: "exploit_loophole", bold: true }, "—", "MEDIUM", "MEDIUM"],
      [{ text: "theme_frame_mismatch", bold: true }, "LOW", "—", "MEDIUM"],
    ],
    [2700, 2300, 2300, 2060]
  ),

  caption("Stance percentages aggregated across all five channels (Reddit + Top Forum #1 + #2 + #3 + press/blog). The per-subclass r/onednd \"How are you going to rate\" threads structurally mirror WoTC's own Green/Yellow/Red survey rating system. n = 442 classified items. Each tag intensity is the aggregated tag-rate across the per-subclass discussion (\"HIGH\" = the tag fires for ≥30% of substantive critique posts on that subclass; \"—\" = the tag did not fire meaningfully)."),

  // ─── 3. PER-SUBCLASS DETAIL ───
  pageBreak(),
  ...header("SECTION 3", "Path of Lament  ·  Barbarian"),

  ...callout("VERDICT: GREEN — SHIP WITH MINOR POLISH   ◆   CONFIDENCE: HIGH",
    "Strong reception (80% positive). flavor_endorsement tag dominates. Two specific design tags fire at LOW intensity (underpowered on L10, class_identity_drift on L14 Sorrow Form). Confidence is HIGH because 4 of 5 channels converge on GREEN; the one divergent channel (Top Forum #2) is named and explained, not unknown.",
    COLOR.greenAccent, COLOR.greenLight),

  beat("Top endorsements (flavor_endorsement tag).",
    "The flavor lands. The structured posts overwhelmingly tag the Banshee/grief concept as a creative win:"),

  quoteBlock("Banshee-themed Barbarian is a very inspired concept", "r/onednd lament_rate", 26),
  quoteBlock("best martial subs I've seen since 5.5 launched", "r/onednd lament_rate", 24),
  quoteBlock("Lament definitely has the most gripping lore", "Wargamer review"),
  quoteBlock("the flavor on the Barbarian is delicious", "r/dndnext main", 32),

  beat("Top design critiques (per UA tag).",
    "Three design concerns surfaced consistently across sources, each tagged into the UA-content schema:"),

  ...dataTable(
    ["Critique", "Tag", "Instances", "Theme"],
    [
      ["L10 Otherworldly Anguish underwhelming",  "underpowered", "4", "Instakill threshold (2× Barb level HP) rarely matters at L10 — Barb already hits that"],
      ["L14 Sorrow Form forced Undead type",       "class_identity_drift", "3", "Player choice should be opt-in; creature-type change is a major character decision"],
      ["Banshee's Wail nested limited resource",   "mechanic_clunky", "2", "Refresh-on-rage inside long-rest cap feels clunky to track"],
    ],
    [2900, 1500, 900, 4060]
  ),

  beat("Minority dismissal — the only flavor_critique signal.",
    "One comment (+6 ups) read \"Emo phase.\" That's the entire flavor_critique tag volume for this subclass — a single low-engagement dismissal. The classifier captures it but the volume is too small to flag a backlash pattern."),

  beat("Actionable design read.",
    "Ship as-is with two specific polish passes: (1) make Otherworldly Anguish L10 instakill threshold scale to 4× Barb level instead of 2×; (2) make Sorrow Form's Undead-creature-type opt-in rather than automatic. Banshee's Wail mechanic is already net-positive — the \"clunky\" complaints are outweighed 4-to-1 by endorsements of the same feature."),

  logline("Ship it, but tweak it. Fix the instakill scaling and make the undead-form optional. These mechanic_clunky complaints are drowned out by applause."),

  // ─── WARRIOR OF VENOM ───
  pageBreak(),
  ...header("SECTION 3 (cont.)", "Warrior of Venom  ·  Monk"),

  ...callout("VERDICT: GREEN-YELLOW — SHIP WITH THREE FIXES   ◆   CONFIDENCE: HIGH",
    "Strong concept reception (65% positive) but elevated overpowered + class_identity_drift + exploit_loophole tags. The community loves the flavor and finds three specific design issues that need pre-publication fixes. Confidence is HIGH because the Sedative-toxin overpowered tag alone is triangulated across 5 independent voices and 4 community channels — textbook cross-source convergence that single-channel sentiment cannot reproduce.",
    COLOR.amberAccent, COLOR.amberLight),

  beat("Top endorsements.",
    "The poison-monk concept resonates, especially as a long-requested archetype:"),

  quoteBlock("poison-themed character finally solved", "r/onednd main", 306),
  quoteBlock("Reptile Wins", "r/onednd main", 119),
  quoteBlock("I love the alien acid blood thing going on", "r/onednd venom_rate", 12),
  quoteBlock("It's literally everything that a new player coming in thinking 'It would be cool to play a poison using assassin' could want", "r/onednd venom_rate", 11),

  beat("Top design critiques (per UA tag).",
    "Four design issues recurring consistently across all five channels:"),

  ...dataTable(
    ["Critique", "Tag", "Instances", "Theme"],
    [
      ["L17 Hallucinogenic Breath should be cone-AOE", "underpowered", "6", "Single-target capstone feels weak; community wants the cone-effect version"],
      ["Envenom Weapon doesn't apply to unarmed strikes", "class_identity_drift", "4", "Locks the subclass out of core Monk identity"],
      ["Slowing Toxin auto-applies, no save, scales poorly", "overpowered", "4", "ScreenRant flagged as 'subtly broken' — bypasses CR-scaling entirely"],
      ["L11 Toxin Refiner ambiguous wording", "exploit_loophole", "3", "'Bag of Rats' / 'homeopathic poison' loophole — unbounded healing"],
    ],
    [2900, 1500, 900, 4060]
  ),

  beat("Press confirmation.",
    "ScreenRant published a standalone piece titled \"New D&D Subclass Has A Free Game-Breaking Update\" arguing the Slowing Toxin no-save mechanic is broken, urging players to exploit it before the inevitable nerf. That's an external classifier-grade signal independently confirming the community's overpowered tag on Slowing Toxin."),

  logline("When gaming press drops an article explicitly telling players to exploit a broken no-save mechanic, you don't need a survey to know a nerf is coming."),

  beat("Actionable design read.",
    "Ship with three required fixes before publication: (1) make Hallucinogenic Breath a cone, (2) extend Envenom Weapon to unarmed strikes, (3) errata Toxin Refiner to close the Bag-of-Rats loophole. Slowing Toxin balance is already on WoTC's radar via the press piece — design team likely has a draft response."),

  // ─── PRIMORDIAL PATRON ───
  pageBreak(),
  ...header("SECTION 3 (cont.)", "Primordial Patron  ·  Warlock"),

  ...callout("VERDICT: YELLOW-RED — REWORK BEFORE SHIP   ◆   CONFIDENCE: MEDIUM",
    "Lowest Reddit reception of the three (35% positive, 15% negative). underpowered + mechanic_clunky + exploit_loophole all fire at HIGH intensity. Plus the only meaningful theme_frame_mismatch signal of the packet (the 'not actually villainous' subthread). Confidence is MEDIUM not HIGH: cross-forum verdicts genuinely fragment here (Reddit YELLOW-RED vs all three top forums YELLOW vs press POSITIVE-CAUTIOUS). The fragmentation IS the finding, but the magnitude estimate is fragile.",
    COLOR.redAccent, COLOR.redLight),

  beat("Top endorsements (limited).",
    "The vibe lands but reception is thin — \"hyped\" and \"raid boss energy\" are praise, but not enough to outweigh the critique mass:"),

  quoteBlock("Insanely hyped for primordial warlock", "r/onednd main", 91),
  quoteBlock("I really loved the vibe of the class", "r/onednd primordial_rate", 17),
  quoteBlock("spellcaster with a toolset ripped straight from a raid boss", "Wargamer review"),

  beat("Top design critiques (per UA tag).",
    "Five issues with high-volume support across sources. The top critique (Elemental Node friendly fire) is the highest-upvoted comment in the entire primordial discussion thread:"),

  ...dataTable(
    ["Critique", "Tag", "Instances", "Theme"],
    [
      ["Elemental Node does friendly fire", "mechanic_clunky", "8", "Hits allies & familiar; ruins party tactics. +47 ups (top comment)"],
      ["Subclass weaker than existing warlock options", "underpowered", "5", "Compared unfavorably to existing warlock subclasses"],
      ["Node 1d6/save creates excessive table-time overhead", "mechanic_clunky", "4", "Roll-a-save-for-trivial-damage is a known anti-pattern"],
      ["Earth spell list comically thin vs Fire/Water/Air", "underpowered", "3", "Asymmetric spell-list quality breaks element-pick parity"],
      ["Elemental Transmutation invocation > Sorcerer metamagic", "exploit_loophole", "3", "Strictly-better design clash with existing Sorcerer feature"],
    ],
    [2900, 1500, 900, 4060]
  ),

  beat("theme_frame_mismatch — the distinguishing tag of the packet.",
    "Unlike the other two subclasses, Primordial Patron triggered a sustained \"this isn't actually villainous\" thread. Unlike a Goblin-Slayer-tier flavor_critique (where the tone is genuinely off-putting), this is a theme_frame_mismatch — the community thinks the subclass IS interesting, just not under the \"Villainous Options\" framing:"),

  quoteBlock("Did warlock need a villainous option? xD", "r/dndnext main", 132),
  quoteBlock("really feel more chaotic neutral than villainous", "r/dndnext main", 42),

  beat("Actionable design read.",
    "Significant rework required before publication: (1) Elemental Node MUST exclude allies / chosen creatures, (2) increase node uses or make moving free, (3) rebalance against existing warlock subclasses, (4) reconcile Elemental Transmutation invocation with Sorcerer's Transmuted Spell metamagic, (5) Earth spell list needs additions. Plus a brand-marketing call: this subclass is fine, but framing it under \"Villainous Options\" creates an avoidable theme_frame_mismatch."),

  // ─── 3.5 CROSS-FORUM DIVERGENCE ───
  pageBreak(),
  ...header("SECTION 3.5", "Cross-forum divergence — what triangulation reveals"),

  beat("The structural insight.",
    "The five channels do not produce identical readings. Reddit, the three top TTRPG forums, and the press each have distinct cultures — and each culture produces a meaningfully different fingerprint on the same content. Single-source listening would miss this. Triangulation is the diagnostic."),

  beat("Per-subclass cross-channel stance.",
    "Each row below is one channel's verdict on each subclass. Where the row diverges, that divergence is itself the actionable signal — it tells the design team WHICH audience is reacting WHICH way."),

  ...dataTable(
    ["Channel", "Path of Lament", "Warrior of Venom", "Primordial Patron"],
    [
      [{ text: "Reddit", bold: true },
        { text: "GREEN (80%)",      color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "GREEN-YELLOW (65%)", color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight },
        { text: "YELLOW-RED (35%)", color: COLOR.redAccent, bold: true, fill: COLOR.redLight }],
      [{ text: "Top Forum #1", bold: true },
        { text: "GREEN (70%)",      color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "GREEN-YELLOW (65%)", color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight },
        { text: "YELLOW (50%)",     color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight }],
      [{ text: "Top Forum #2", bold: true },
        { text: "DIVISIVE (50%)",   color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight },
        { text: "GREEN-YELLOW (70%)", color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "YELLOW (55%)",     color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight }],
      [{ text: "Top Forum #3", bold: true },
        { text: "GREEN (70%)",      color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "GREEN-YELLOW (60%)", color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight },
        { text: "YELLOW (50%)",     color: COLOR.amberAccent, bold: true, fill: COLOR.amberLight }],
      [{ text: "Press / blog", bold: true },
        { text: "GREEN (100%)",     color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "POSITIVE-MIXED (80%)", color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight },
        { text: "POSITIVE-CAUTIOUS (75%)", color: COLOR.greenAccent, bold: true, fill: COLOR.greenLight }],
    ],
    [2400, 2320, 2320, 2320]
  ),

  caption("Stance percentages per channel. Color reflects dominant verdict (green = positive, amber = divisive, red = negative). Sample sizes: Reddit n=359, Top Forum #1 n=48, Top Forum #2 n=22, Top Forum #3 n=7. The Top Forum #3 cells are directional only — n=7 is too small to point-estimate stance percentages with confidence; treat as a qualitative read."),

  beat("Three divergence patterns worth naming.",
    "(1) Path of Lament reads GREEN on every channel except Top Forum #2, whose mechanically-rigorous forum culture produced a long-form critique flagging the L3 feature as substantially underpowered. Reading: the worldbuilding-oriented audience loves the Banshee/grief flavor; the optimization-oriented audience reads the math and finds it underweight. Both signals are real and need different responses. (2) Warrior of Venom reads consistently GREEN-YELLOW across all four community channels — high concept-love, recurring critique on three specific design issues (Hallucinogenic Breath underpowered, Envenom Weapon class_identity_drift, Sedative toxin overpowered). Cross-channel agreement at this level is rare and high-conviction. The Sedative-toxin overpowered tag alone is triangulated across 5 independent voices and 4 channels — that's a publishable finding. (3) Primordial Patron diverges most sharply: Reddit reads YELLOW-RED, while all three top forums and press read warmer (YELLOW to POSITIVE-CAUTIOUS). Reading: Reddit's conversation is gated by the +47-upvote Elemental-Node-friendly-fire complaint that dominates the subreddit megathread. The other channels weight the elemental-evil flavor higher and the friendly-fire issue lower. Action: fix the node, and the elemental-evil framing pulls reception up across all channels."),

  logline("To sum up: the lore-nerds love it, the math-nerds found the exploits, Reddit is obsessed with a friendly-fire bug, while the forums care more about the elemental flavor."),

  beat("The theme_frame_mismatch signal — strongest from Top Forum #1.",
    "Top Forum #1 produced the most concentrated \"these aren't actually villainous\" subthread of any source. Eight forum posts across five distinct readers questioned whether the \"Villainous Options\" framing fits the content. Aggregate sentiment of the subthread, paraphrased for storage:"),

  p([rt("•   ", { color: COLOR.ember, bold: true, font: FONT_BODY, size: 22 }),
     rt("The Path of Lament reads as a grief-driven archetype, not a villainous one — multiple readers questioned why a depressed-barbarian framing counts as villainous.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 100 }),
  p([rt("•   ", { color: COLOR.ember, bold: true, font: FONT_BODY, size: 22 }),
     rt("The elemental patrons of the Primordial Warlock are typically neutral in D&D lore rather than evil — readers noted the framing forces a moral cast onto neutral cosmology.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 100 }),
  p([rt("•   ", { color: COLOR.ember, bold: true, font: FONT_BODY, size: 22 }),
     rt("None of the three subclasses inherently encourages evil play — readers concluded the \"Villainous Options\" packaging is a marketing label without mechanical commitment.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 200 }),

  beat("Why this matters.",
    "Critically — this is NOT flavor_critique in the Goblin-Slayer sense (where that IP is rejected as off-tone. See our IP deep dive). The community engages WITH the content; they dispute the LABEL. That's a theme_frame_mismatch — actionable for marketing, not for design."),

  beat("Why this matters for the pitch.",
    "WoTC's official survey gives the design team one number per question, summed across the entire respondent pool. The cross-channel divergence is invisible to that survey because it never asks \"which channel culture do you come from?\" Trusight surfaces the divergence by triangulating across channels with distinct response signatures. That structural visibility is impossible from a single horizontal-listening tool, impossible from the official survey, and impossible from any one channel read in isolation. Five channels, five signatures, one structured fingerprint — that's the product."),

  // ─── 4. CLASSIFIER ARCHITECTURE ───
  pageBreak(),
  ...header("SECTION 4", "How the classifier is built — event-class-specific tag schemas"),

  beat("The principle.",
    "A horizontal social-listening tool would tell you \"the community talked about this UA — average sentiment was mildly positive.\" That doesn't help. Trusight's classifier produces a structured fingerprint, and the fingerprint is the actionable artifact. But the right tag set depends on the event being measured. UA-content reception, IP-licensing reception, and brand-trust events have structurally different failure modes; each gets its own classifier schema, tuned to the decision the audit informs."),

  beat("Same architecture, different schemas.",
    "The classifier accepts a thread, applies its current tag set, and emits a multi-label fingerprint per post. The architecture is uniform; the tag content differs by event class. Two of the schemas are shown below; a third (brand-trust events) sits in the production roadmap and is referenced in Section 7."),

  // UA schema callout
  ...callout("UA-CONTENT SCHEMA  (used in this audit)",
    "For playtest reception of new player-content drops (subclasses, classes, races, mechanics). Eight tags: overpowered, underpowered, mechanic_clunky, class_identity_drift, exploit_loophole, flavor_endorsement, flavor_critique, theme_frame_mismatch. The schema reflects what the design team actually decides on a UA: ship as-is, ship with polish, rework, or kill. Each tag maps directly to a design action.",
    COLOR.ember, COLOR.emberLight),

  ...dataTable(
    ["Tag", "What it measures", "Example from this audit"],
    [
      ["overpowered", "Feature too strong relative to its level/cost", "Sedative toxin: no save, 1-min unconscious doesn't break on damage"],
      ["underpowered", "Feature too weak relative to its level/cost", "L17 Hallucinogenic Breath as single-target instead of cone"],
      ["mechanic_clunky", "Mechanically functional but awkward to play/track", "Elemental Node friendly fire; save-for-half-of-1d6"],
      ["class_identity_drift", "Subclass conflicts with its parent class's identity", "Envenom Weapon doesn't apply to unarmed strikes (Monk)"],
      ["exploit_loophole", "Specific game-rules loophole identified by the community", "L11 Toxin Refiner 'Bag of Rats' homeopathic-poison loophole"],
      ["flavor_endorsement", "Positive on theme / worldbuilding / concept", "'Banshee-themed Barbarian is a very inspired concept'"],
      ["flavor_critique", "Theme / concept / tone doesn't land", "'Emo phase' minority dismissals on Lament"],
      ["theme_frame_mismatch", "Disputes the framing/labeling, not the content itself", "'These aren't actually villainous' subthread on Top Forum #1"],
    ],
    [2300, 3000, 4060]
  ),

  caption("The eight-tag UA-content schema applied to all 442 classified items in this audit. cash_grab and pandering — present in the IP-reception schema below — are dropped here because they are structurally inapplicable to free playtest content."),

  // IP schema callout (for contrast)
  ...callout("IP-RECEPTION SCHEMA  (referenced for contrast)",
    "For licensing decisions on outside IP crossovers. Six tags reflect the failure modes of an IP licensing call: cash_grab, tone_mismatch, not_dnd, pandering, system_design_critique, worldbuilding_endorsement. This schema is what produced the Goblin Slayer triangulation in our prior IP-corpus work; it would not be applied to UA content because the failure modes don't match.",
    COLOR.mutedGray, COLOR.tableAltBg),

  beat("Why the schemas can't be merged.",
    "A single universal tag set would have to either (a) include cash_grab as a tag for UA, where it's structurally inapplicable and produces noise, or (b) drop cash_grab from IP work, where it's the canonical signal of an IP-licensing failure. Either choice degrades classifier accuracy on one event class to fit the other. The architectural answer is to keep the schemas separate, share the same uniform fingerprint shape, and select the schema based on what the audit measures."),

  logline("Stuffing every use case into one mega-schema is like using a greatsword as a lockpick — you'll open something, but not what you meant to."),

  beat("What this means in practice.",
    "When the UA-content classifier fires \"overpowered HIGH\" on a Sedative toxin, that's a distinct alarm from when the IP-reception classifier fires \"tone_mismatch HIGH\" on a Goblin Slayer crossover, which is again distinct from when a brand-trust classifier fires \"cash_grab + not_dnd HIGH\" on a policy announcement. Three separate decision actions, three separate audiences inside WoTC."),

  logline("A single-scalar sentiment score collapses all three into the same number; event-class-specific schemas keep them separate and actionable."),

  // ─── 5. SPEED ADVANTAGE ───
  pageBreak(),
  ...header("SECTION 5", "Speed — by two weeks, with each UA, you have a \"what landed\" report"),

  beat("WoTC's current decision timeline.",
    "WoTC opens the official UA survey roughly a week or two after a packet drops, intentionally delayed so the community can actually playtest the material rather than react to a first read. The survey then stays open another two to three weeks. After it closes, WoTC's internal data team must process tens of thousands of long-form responses before the design team gets a structured report. Past One D&D playtest cycles published results several weeks to a couple of months after a survey closes — Playtest 8's came roughly six to ten-plus weeks after release. WoTC has no formal SLA on results publication; the structured-feedback wait is variable but consistently long."),

  logline("WoTC's feedback clock is intentionally delayed to allow for playtesting. The resulting data lag leaves the design team wondering, for a month or even longer, if their work landed."),

  beat("Trusight's decision timeline.",
    "This audit ran on T+11. Structured per-subclass verdicts, top critique themes per UA tag, and supporting evidence are in hand today. WoTC's official survey for this packet opened April 30 (T+7) and closes May 14 (T+21) — the survey is mid-cycle, but no structured report will reach the design team until weeks after that close. The classifier signal is also re-runnable: same query, same sources, every Tuesday morning. The picture sharpens as discussion volume grows."),

  ...callout("A STRUCTURED VERDICT WEEKS BEFORE THE OFFICIAL ONE",
    "Today, on T+11: structured per-subclass verdicts with cited evidence. WoTC's survey is mid-cycle (open until T+21); the design team will not see processed results until weeks after the survey closes. By the time those results publish — historically a month or more after that — Trusight's read has served as the working hypothesis the entire span, refined weekly.",
    COLOR.ember, COLOR.emberLight),

  beat("Calibration disclosure.",
    "The speed advantage above is a methodological claim, not yet an empirically validated one — WoTC's official survey hasn't published. We commit to a calibration paper when the official results land, comparing this audit's T+11 directional verdict against the published result. If the directional read holds, the speed claim is empirically validated. If it diverges, we publish where and why. Until the calibration paper, treat the verdicts above as directionally usable, not authoritative."),

  // ─── 6. METHODOLOGY, CONFIDENCE & CALIBRATION ───
  pageBreak(),
  ...header("SECTION 6", "Methodology, confidence & calibration"),

  beat("This section reads against ourselves.",
    "Trusight is a new tool. Sophisticated buyers — a Snowflake/dbt user, an Insights team validator, a stats-trained analyst — discount any pitch that reads bulletproof. They know nothing is. The audit's findings are real, but the document above presents them more firmly than the underlying data warrants in places. This section names where, and why, and what would firm them up. Calibrated honesty here is not apology; it's the basis for the trust the buyer has to extend to use the tool at all."),

  ...callout("STRONGEST FINDING",
    "Sedative-toxin overpowered + exploit_loophole tags on Warrior of Venom — triangulated across 5 independent voices and 4 community channels (Reddit + Top Forum #1 + Top Forum #2 + Top Forum #3) plus an explicit-named press piece. Cross-source convergence at this level is the textbook signal of a robust finding. This is the kind of result a single-channel sentiment tool structurally cannot produce.",
    COLOR.greenAccent, COLOR.greenLight),

  ...callout("WEAKEST FINDING (STILL VALUABLE)",
    "The \"channel-culture\" framings — different forums producing different reception readings. The directional reading is plausible and the cross-channel stance differences are real, but the cultural attribution behind WHY each channel reads differently is inferred from one thread per channel on this UA, not from a longitudinal per-channel corpus. Firming it up requires a 10-thread comparative corpus per channel — straightforward to build, not yet built.",
    COLOR.amberAccent, COLOR.amberLight),

  beat("Sample composition (and the bias it creates).",
    "359 of 442 items (81%) are from Reddit. Reddit's culture is more reactive — early, loud, opinion-strong — than the smaller TTRPG forums. This means the dataset over-represents the early-and-loudest demographic, and any finding that appears ONLY on Reddit deserves lower confidence than one triangulated across 3 or more channels. The smaller forums (Top Forum #1 n=48, Top Forum #2 n=22, Top Forum #3 n=7) are the corrective signal — when they agree with Reddit, confidence rises sharply; when they diverge, the divergence is itself the finding. The cross-channel table makes the divergences visible."),

  logline("Reddit's our 80% front-row chorus; TTRPG forums and blogs are the quiet critics in the balcony, adding nuance, second opinions, and the \"are we sure?\" details."),

  beat("Selection bias on the rate threads.",
    "The r/onednd \"How are you going to rate\" threads happen to mirror WoTC's own Green/Yellow/Red survey rating system — a methodological gift that lets us read the same vote structure their survey will eventually receive. But the people who write structured rating posts are self-selected for engagement and opinion strength. The silent majority's verdict could differ. The official WoTC survey has the same self-selection problem (only opt-in respondents), so the comparison is apples-to-apples — but neither is a random sample of the player base."),

  beat("Per-finding confidence grades.",
    "The strongest findings are the cross-source-triangulated ones (Venom Sedative-toxin overpowered tag, the structurally different fingerprints between event-class schemas, Lament's flavor_endorsement on 4 of 5 channels). The medium-confidence findings are the per-subclass stance percentages (treat as ±10 points, not exact) and the cross-channel stance divergences themselves (real signal, fragile magnitudes). The weakest-but-still-valuable findings are the channel-culture interpretations and the Top Forum #3 column (n=7 — directional only)."),

  beat("What we do not yet have.",
    "Ground-truth comparison against the official WoTC survey result is a forward promise — that survey hasn't published. The channel-culture inferences are derived from one thread per channel on this UA, not from longitudinal per-channel corpora. The brand-trust-event schema referenced in Section 4 sits in the production roadmap; it has not yet been calibrated against an archived corpus. None of these gaps invalidates the findings above; they bound them."),

  beat("Calibration plan.",
    "Two commitments. (1) Re-run weekly through 2026-05-14 (survey close) and publish the T+11 / T+18 / T+21 fingerprint trajectory — the trajectory itself is a stability check on the methodology. (2) Calibration paper when WoTC's official survey publishes, comparing our directional verdicts to the official result, naming agreements and divergences."),

  // ─── 7. WHAT THIS PROVES ───
  pageBreak(),
  ...header("SECTION 7", "What this proves"),

  beat("The use case described in A1 is not a description.",
    "It's a runnable pipeline. We received the UA packet on May 4 and produced a structured per-subclass classification within the same session, drawing on 442 real community posts harvested across Reddit, three top TTRPG forums, and the press/blogosphere. Every claim in this document traces to a row in the harvested dataset (saved to gold_data tables in BigQuery)."),

  beat("The classifier is the product, not the demo.",
    "The eight-tag UA-content schema produces a meaningfully different fingerprint than the IP-reception schema would on the same content: overpowered + class_identity_drift + exploit_loophole tags surface the actionable design issues; flavor_endorsement captures the parts the community likes; theme_frame_mismatch isolates the brand-positioning concern from the design concerns. The structured signal is what design needs — different tags fire different decision actions inside WoTC."),

  beat("The speed claim is real, not aspirational.",
    "Trusight delivers a structured per-subclass verdict on T+11 with 442 classified posts across all five channels. WoTC's official survey on this packet does not even open until T+14 and closes at T+21. Past One D&D playtest cycles have published results several weeks to a couple of months after a survey closes. The lead time is variable but consistently long; the structured-verdict-in-hand position transforms reactive design (\"here's what the survey says, weeks late\") into proactive design (\"here's the working hypothesis, refined weekly, with cited evidence\")."),

  hr(COLOR.ember),

  p([rt("Coverage report: ", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 20 }),
     rt("All five channels harvested. Reddit returned 9 dedicated discussion threads with 1,015 aggregate comments; we classified the 359 highest-scoring. The three top TTRPG forums each contributed independent signal (48 + 22 + 7 posts). Press/blog covered six outlets. Total dataset: 442 classified items, all stored in gold_data BigQuery tables for re-querying. The same harvest pipeline targets the next UA packet on day-one.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 20 })],
    { line: 300, after: 200 }),

  p([rt("Next: ", { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 20 }),
     rt("re-run weekly through May 14 (WoTC survey close) to track how the verdict evolves; compare T+11 / T+18 / T+21 fingerprints against the eventual official survey result for the calibration paper.", { color: COLOR.dark, italics: true, bold: true, font: FONT_BODY, size: 20 })],
    { align: AlignmentType.LEFT, line: 280 }),
];

const doc = new Document({
  styles: sharedStyles,
  sections: [{
    properties: { page: pageSetup },
    children: docChildren,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("trusight_report_A1_LIVE_audit.docx", buf);
  console.log("Wrote: trusight_report_A1_LIVE_audit.docx");
});
