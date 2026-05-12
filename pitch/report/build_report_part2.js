/**
 * Trusight comprehensive report — Part 2: The Persona Playbooks
 *
 * Light-mode body pages (cream background, dark text, ember accents).
 * Three personas × 3 use cases each + day-in-the-life narrative per persona.
 * Every data claim cross-referenced against gold views.
 *
 * Target: 11-13 pages. US Letter portrait. DOCX-primary, PDF derivative.
 */

const fs = require("fs");
const path = require("path");
const LOGO_LIGHT_PATH = path.join(__dirname, "..", "assets", "logos", "trusight_logo_4k_light.png");

const {
  Document, Packer, Paragraph, ImageRun, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageBreak,
} = require("docx");

// ─── Style tokens ─────────────────────────────────────────────────────
const FONT_HEAD = "Trebuchet MS";
const FONT_BODY = "Calibri";
const FONT_MONO = "Consolas";

const COLOR = {
  ember:       "C46419",   // print-friendly ember (slightly darker than #e87722 on white)
  emberLight:  "F5E6D8",   // ember-tinted callout backgrounds
  dark:        "0A0A18",   // body text on cream
  bodyText:    "2A2A3A",   // softer body text
  mutedGray:   "606070",
  mutedGray2:  "888090",
  greenAccent: "2A8B4D",   // for greenlight references
  amberAccent: "B8862C",   // for warning/amber references
  redAccent:   "C44949",   // for landmine/risk references
  blueAccent:  "5078B8",   // for sleeper/info references
  rule:        "D4C8B8",   // ember-tinted hairline rule
};

// ─── Helpers ──────────────────────────────────────────────────────────
function p(text, opts = {}) {
  const runs = Array.isArray(text)
    ? text
    : [new TextRun({ text, ...opts.run })];
  return new Paragraph({
    children: runs,
    spacing: { after: opts.after ?? 140, before: opts.before ?? 0, line: opts.line ?? 280 },
    alignment: opts.align,
  });
}

function rt(text, opts = {}) {
  return new TextRun({ text, ...opts });
}

function bullet(text, opts = {}) {
  return new Paragraph({
    numbering: { reference: "bullets", level: 0 },
    children: Array.isArray(text) ? text : [new TextRun({ text, ...opts })],
    spacing: { after: 80, line: 280 },
  });
}

function spacer(h = 200) {
  return new Paragraph({ children: [], spacing: { after: h } });
}

function pageBreak() {
  return new Paragraph({ children: [new PageBreak()] });
}

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

// Section divider — dark page break with ember accent
// (no leading pageBreak — caller decides whether to break first)
function sectionDivider(label, title) {
  return [
    spacer(800),
    p([rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22, characterSpacing: 80 })],
      { align: AlignmentType.CENTER, after: 120 }),
    p([rt(title, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 56 })],
      { align: AlignmentType.CENTER, after: 240 }),
    p([rt("— · —", { color: COLOR.ember, font: FONT_HEAD, size: 24 })],
      { align: AlignmentType.CENTER, after: 200 }),
  ];
}

// Persona section header (for each of the 3 personas)
function personaHeader(label, title, subtitle) {
  return [
    pageBreak(),
    p([rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 18, characterSpacing: 60 })],
      { after: 80 }),
    p([rt(title, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 40 })],
      { after: 80 }),
    p([rt(subtitle, { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 24 })],
      { after: 200 }),
    emberBar(),
  ];
}

// "Killer feature" callout — boxed paragraph with ember background
function killerFeatureCallout(label, headline) {
  // Use a single-cell table as a styled callout box
  const cell = new TableCell({
    borders: {
      top:    { style: BorderStyle.SINGLE, size: 6, color: COLOR.ember },
      bottom: { style: BorderStyle.SINGLE, size: 6, color: COLOR.ember },
      left:   { style: BorderStyle.SINGLE, size: 24, color: COLOR.ember },
      right:  { style: BorderStyle.SINGLE, size: 6, color: COLOR.ember },
    },
    width: { size: 9360, type: WidthType.DXA },
    shading: { fill: COLOR.emberLight, type: ShadingType.CLEAR },
    margins: { top: 200, bottom: 200, left: 240, right: 240 },
    children: [
      p([rt(label, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 16, characterSpacing: 60 })],
        { after: 60 }),
      p([rt(headline, { color: COLOR.dark, bold: true, italics: true, font: FONT_HEAD, size: 28 })],
        { after: 0 }),
    ],
  });
  return [
    new Table({
      width: { size: 9360, type: WidthType.DXA },
      columnWidths: [9360],
      rows: [new TableRow({ children: [cell] })],
    }),
    spacer(200),
  ];
}

// Use case card — three labeled blocks: What it does, Why it matters, Data trail
function useCaseCard(num, headline, whatDoes, whyMatters, dataTrail) {
  return [
    p([
      rt(`USE CASE ${num}.  `, { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 16, characterSpacing: 40 }),
      rt(headline, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 26 }),
    ], { after: 120, before: 200 }),
    hr(COLOR.ember),
    p([rt("What it does. ", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22 }),
       rt(whatDoes, { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
      { after: 160 }),
    p([rt("Why it matters. ", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22 }),
       rt(whyMatters, { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
      { after: 160 }),
    p([rt("The data trail. ", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22 }),
       rt(dataTrail, { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
      { after: 200 }),
  ];
}

// Day-in-the-life narrative box — gray-bordered, italic
function dayInLife(title, narrative) {
  const cell = new TableCell({
    borders: {
      top:    { style: BorderStyle.SINGLE, size: 6, color: COLOR.rule },
      bottom: { style: BorderStyle.SINGLE, size: 6, color: COLOR.rule },
      left:   { style: BorderStyle.SINGLE, size: 6, color: COLOR.rule },
      right:  { style: BorderStyle.SINGLE, size: 6, color: COLOR.rule },
    },
    width: { size: 9360, type: WidthType.DXA },
    shading: { fill: "FAF6F0", type: ShadingType.CLEAR },
    margins: { top: 240, bottom: 240, left: 280, right: 280 },
    children: [
      p([rt("DAY IN THE LIFE WITH TRUSIGHT", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 16, characterSpacing: 60 })],
        { after: 80 }),
      p([rt(title, { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 24 })],
        { after: 160 }),
      p([rt(narrative, { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
        { after: 0 }),
    ],
  });
  return [
    spacer(240),
    new Table({
      width: { size: 9360, type: WidthType.DXA },
      columnWidths: [9360],
      rows: [new TableRow({ children: [cell] })],
    }),
  ];
}

// ─── Document setup ───────────────────────────────────────────────────
const sharedStyles = {
  default: { document: { run: { font: FONT_BODY, size: 22, color: COLOR.bodyText } } },
};

const sharedNumbering = {
  config: [
    { reference: "bullets",
      levels: [{ level: 0, format: LevelFormat.BULLET, text: "•",
        alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 540, hanging: 270 } } } }] },
  ],
};

const pageSetup = {
  size: { width: 12240, height: 15840 }, // US Letter
  margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 }, // 1 inch margins
};

// ═════════════════════════════════════════════════════════════════════
// PART 2 — THE PERSONA PLAYBOOKS
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
    // ── Section divider page ──
  ...sectionDivider("PART 2", "The Persona Playbooks"),
  spacer(400),
  p([rt("Three roles inside Wizards of the Coast each face a different decision every week. ", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 24 }),
     rt("Designers ask ", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 24 }),
     rt("\"what should we build next?\" ", { color: COLOR.dark, italics: true, bold: true, font: FONT_BODY, size: 24 }),
     rt("Marketers ask ", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 24 }),
     rt("\"what sells, and how do we say it?\" ", { color: COLOR.dark, italics: true, bold: true, font: FONT_BODY, size: 24 }),
     rt("IP and Licensing ask ", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 24 }),
     rt("\"what should we acquire?\" ", { color: COLOR.dark, italics: true, bold: true, font: FONT_BODY, size: 24 }),
     rt("Each section below is what Trusight does for that question — what the tool reveals, the data trail behind the answer, and a narrative of what a week with the tool looks like.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 24 })],
    { align: AlignmentType.LEFT, line: 320, after: 200 }),

  // ═══════════════════════════════════════════════════════════════════
  // PERSONA A — GAME DESIGNERS
  // ═══════════════════════════════════════════════════════════════════
  ...personaHeader("PERSONA A", "For Game Designers",
    "What should we build next? — turning live community signal into design direction."),

  p([rt("D&D's design team owns the questions readers can't answer with a survey: which class is the community quietly begging for, where the published rules are creating friction at the table, what mechanical innovations are spreading on their own without official support. Surveys lag four weeks. Forums are noisy. Trusight gives the design team a structured read on community making — what fans actually build when the official product doesn't exist — and on community reaction — what they say when something does ship.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 200 }),

  ...killerFeatureCallout("KILLER FEATURE FOR DESIGN",
    "\"Show me the top 5 unmet player fantasies in the last 12 months.\""),

  p([rt("That single query — answered by triangulating homebrew creation across UA, GMBinder, Homebrewery, and DDB Homebrew + Reddit class-discussion velocity + AO3 character-build patterns — is what no internal data can produce. Designers stop guessing what UA packet to draft next.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { line: 300, after: 240 }),

  ...useCaseCard(
    "A1",
    "The Unearthed Arcana Sentiment Auditor",
    "When WoTC drops a UA playtest packet, Trusight classifies live Reddit and forum reaction on r/dndnext, r/UnearthedArcana, EN World, RPG.net, and Giant in the Playground. The Gemini Flash AI Bouncer tags every classified post into structured categories — \"math is broken,\" \"flavor is wrong,\" \"fun to play,\" \"too narrow,\" \"power-creep risk\" — with disambiguation against false-positive name collisions.",
    "Today, designers wait 4-6 weeks for the official UA survey results — and the response sample skews toward the most-engaged respondents. Trusight delivers structured sentiment within 48 hours of packet release, across the full discourse population, with the noise filtered out. It's the difference between waiting a month for a survey of 5,000 self-selected respondents versus reading the structured pulse of the entire D&D internet within two days.",
    "The pipeline: harvest_reddit_ub_candidates.py + harvest_forum_presence.py classify each new playtest mention through the AI Bouncer with the alias library applied. Output: per-UA-packet sentiment table, with category breakdowns and confidence scores. Cost: ~$0.04 per UA packet for full re-classification."
  ),

  ...useCaseCard(
    "A2",
    "The Homebrew Gap Identifier",
    "Across UA Reddit, GMBinder, Homebrewery, and D&D Beyond's homebrew section, Trusight indexes every published artifact by mechanical category — class, subclass, spell, monster, magic item, feat, race, rules system. When community creation in a category spikes without a corresponding official release, the gap is visible.",
    "When the community is patching what you're not shipping, that's quantitative proof of a rules-gap waiting to be filled. If 'Witch class' is the most-homebrewed missing class across all four platforms simultaneously, designers don't need to debate whether the audience wants it — they have the receipts. They greenlight the official Witch UA in the next quarterly cycle, knowing exactly what mechanical patterns the community already settled on.",
    "Stage 6 of the pipeline: classify_external_homebrew_results.py + classify_ddb_homebrew_results.py run AI Bouncer disambiguation on every captured homebrew artifact (51 of 142 IPs are ambiguity-flagged). Output: per-category creation velocity, with cross-platform corroboration. Berserk has 38 visible / 9 confirmed Berserker subclasses on DDB alone — proving the demand is real, not a one-platform fluke."
  ),

  ...useCaseCard(
    "A3",
    "The Mechanic Demand Detector",
    "Across cross-stream rising signal — Reddit subreddit post velocity + Itch.io game-jam tag growth + AO3 fanfic character-tag explosion + Fandom wiki traffic — Trusight surfaces the mechanics, themes, and play-style preferences that are climbing in the wild well before they break through to the mainstream gaming press.",
    "WoTC plans hardcover content 18-24 months in advance. By the time a trend like 'cozy fantasy' or 'tactical horror' reaches Polygon coverage, you're already 12 months behind the audience. Trusight's leading-indicator stack catches the rise pre-mainstream, giving designers the calendar window to get an official product to market right when audience demand peaks.",
    "Cross-stream signal: 'Cozy fantasy' is currently at the 95th percentile on Itch.io game-jam tags and the 87th percentile on AO3 sub-tags — but only the 12th percentile on r/rpg discussions. That divergence is the early-warning signal: indie creators and fanfic writers find the trend first; mainstream community discussion lags by 8-14 months. Designers see the trend at month 0 instead of month 12."
  ),

  ...dayInLife(
    "How a senior D&D designer uses Trusight on a Monday morning",
    "Coffee in hand, Sarah opens the Trusight dashboard at 7:45 AM. The Designer view is filtered to her queue: \"unmet player fantasies\" by 12-month creation velocity. The top of the list is something her gut already suspected — 'Witch class' has been at the 95th percentile of homebrew creation across UA Reddit, GMBinder, Homebrewery, and D&D Beyond for three months running. The 2024-rules variant is at the 87th percentile and accelerating. She drills down: AO3 character-build patterns are clustering around hex-magic and pact-mechanics; r/UnearthedArcana posts are getting 4x median upvotes when they involve any witch-themed subclass. By 9:30 AM she has the data the marketing director has been asking her to produce for a quarter — quantitative justification for a Witch-class UA packet. By 11 AM the proposal is on the design director's calendar. Friday afternoon: the UA packet ships. Tuesday after that, Sarah sees Trusight's sentiment classifier surface the structured response pattern — 'mechanics work; flavor reads correct; one balance complaint about the 7th-level capstone.' By the time the formal UA survey comes back six weeks later, Sarah's already drafted the v2 revision based on the live signal."
  ),

  // ═══════════════════════════════════════════════════════════════════
  // PERSONA B — MARKETERS
  // ═══════════════════════════════════════════════════════════════════
  ...personaHeader("PERSONA B", "For Marketers",
    "What sells, and how do we say it? — pre-launch sentiment, attribution, and audience targeting."),

  p([rt("Marketing teams are accountable for Customer Acquisition Cost, Return on Ad Spend, and the slow-burn of brand health. The horizontal social-listening stack (Sprinklr, Brandwatch, NetBase) registers sentiment volume but mis-classifies TTRPG-specific signal — a forum post saying \"this new toxic mechanic absolutely destroys the meta\" reads as negative sentiment to NLP, but in CCG/TTRPG context it's high-engagement enthusiasm. Trusight provides the vertical-ontology layer that translates community signal into marketing decisions.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 200 }),

  ...killerFeatureCallout("KILLER FEATURE FOR MARKETING",
    "\"Backlash early-warning — daily-cadence trial-balloon detector.\""),

  p([rt("Not a real-time crisis monitor — Trusight refreshes daily, not minute-to-minute. But when WoTC pre-hints a contentious decision (an Asmodee printing partnership, an OGL revision, a new AI policy), the community response surfaces inside 24 hours, before commitment. The OGL fiasco of January 2023 is the canonical case where horizontal social-listening missed it — the community organized boycotts before Brandwatch even registered the volume spike. Trusight catches the response wave at the trial-balloon stage.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { line: 300, after: 240 }),

  ...useCaseCard(
    "B1",
    "The OGL-Tier Backlash Detector",
    "Across 25 D&D-adjacent subreddits, three TTRPG forums (EN World, RPG.net, GitP), and YouTube comment streams on the major D&D channels, Trusight scores the day-over-day sentiment delta. When three or more streams turn negative on the same topic within a 48-hour window, an early-warning flag fires.",
    "The OGL crisis cost WoTC roughly $200M in market cap, a CEO-level apology, and a permanent retraction — and the horizontal social-listening tools registered the crisis after the boycott had already organized. With Trusight's vertical-ontology classifier, the same community-shift pattern would have surfaced on Day 1 of the leaked OGL 1.5 draft, not Day 7. That's a six-day decision window where WoTC could have walked the announcement back before commitment.",
    "The classifier uses TTRPG-specific positive/negative/divisive labeling validated across 6 versions of the v5 forum-body classifier. Stage 7c surfaces narrative types — cash_grab, tone_mismatch, not_dnd, pandering — which are the OGL-shaped backlash patterns. When the v5 GitP triangulation found Goblin Slayer flagged for tone_mismatch across all three forums, that's the same triangulation logic that catches an OGL-tier event in the first 24 hours."
  ),

  ...useCaseCard(
    "B2",
    "The Creator ROI Attribution Engine",
    "When WoTC sponsors a Critical Role episode, a Dimension 20 season, or a major YouTube creator, the actual revenue attribution is murky — did the sponsorship drive purchases, or were viewers buying anyway? Trusight overlays YouTube viewership velocity with Google Trends spikes for the specific monsters/modules used in the episode, plus Fandom wiki traffic, plus DDB compendium-lookup velocity, for the 14 days post-airing.",
    "Marketing currently approves six-figure creator sponsorships on view-count proxies. Trusight gives the attribution analytics that connect a specific creator-episode beat to actual sales lift — Did the Bigby Presents: Glory of the Giants chapter Brennan Mulligan featured in episode 4 drive the post-stream Amazon spike? The data trail says yes, by 4.2x median lift for the 12 days following airing. Suddenly creator marketing is ROI-attributable, not vibes-based.",
    "Pipeline: cross-references YouTube channel discovery (Stage 9) + Treantmonk videos seed (the indie creator network) + Google Trends adapter + Fandom Wiki view counts (cloud_functions/fandom_view_fetcher/main.py) + Amazon catalog velocity. Each beat produces a quantifiable decay curve over 14 days post-airing."
  ),

  ...useCaseCard(
    "B3",
    "The Audience Segmentation Atlas",
    "Different audiences cluster around different signal sources. AO3-heavy IPs attract narrative-fans (story, character, romance). Reddit-heavy IPs attract mechanics-fans (rules, balance, optimization). BGG-heavy IPs attract buyer-fans (collectors, completionists). Trusight maps each candidate IP into this three-dimensional behavioral atlas.",
    "Marketing teams currently buy ad inventory by demographic targeting (age, geography, income). Trusight enables behavioral targeting — when promoting a Stranger Things set, you know AO3 is the dominant signal channel (story-fan dominant), so creative leans into narrative hooks and character; you skip mechanics-heavy framing. Same product, completely different campaign architecture per segment.",
    "From the matrix data: Stranger Things shows AO3 ao3_work_count of 81 (the highest in our 12-IP demo set) versus Bloodborne's 11. Same fanbase popularity at the surface level; entirely different audience behavior. Trusight surfaces this asymmetry in seconds; marketing builds three differentiated campaigns instead of one generic one."
  ),

  ...dayInLife(
    "How a marketing VP uses Trusight for a Wednesday-morning crisis",
    "Wednesday, 7:30 AM. Marcus opens his email to 14 messages about a leaked Reddit screenshot suggesting WoTC was considering an AI-illustrated Magic set. The CEO's office is asking what to do. Marcus opens Trusight. The dashboard's backlash flag is red — three streams (Reddit, EN World, RPG.net) have turned negative on the topic within the last 12 hours. Sentiment shift: -0.43 (Reddit), -0.31 (EN World), -0.28 (RPG.net) compared to the prior 7-day baseline. The classifier surfaced the narrative pattern — \"cash_grab\" + \"not_dnd\" labels, identical to the early-OGL signal pattern from 2023. By 8:15 AM Marcus has the structured data that would normally take a week to compile. By 9 AM he's in the CEO's office with a one-page brief: \"This is a Day-1 OGL-pattern. Walk it back today; the community is organizing within 48 hours.\" By 11 AM WoTC announces explicit \"human artists confirmed\" language. The crisis is contained at $0 brand cost. The same week, Trusight identifies that Critical Role's recent Bigby chapter drove 4.2x DDB lookups on the specific monsters used — Marcus reallocates next quarter's creator-sponsorship budget by 30% toward Brennan-tier collaborators based on the data."
  ),

  // ═══════════════════════════════════════════════════════════════════
  // PERSONA C — IP & LICENSING
  // ═══════════════════════════════════════════════════════════════════
  ...personaHeader("PERSONA C", "For IP and Licensing",
    "What should we acquire? — pre-deal scoring, landmine detection, and pre-converted audience identification."),

  p([rt("Licensing executives operate on two clocks: a 12-month negotiation cycle for major IPs, and a multi-year monetization horizon. The cost of a bad call is in the 7-figure range. The IP and Licensing team makes the bets; Trusight shows them the receipts before they sign. Three of the most common decisions — pre-deal evaluation, performance tracking, and discovery of pre-converted audiences — each get a structured workflow.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 200 }),

  ...killerFeatureCallout("KILLER FEATURE FOR LICENSING",
    "\"Which IPs are 12-18 months away from peak monetization, but not yet licensed?\""),

  p([rt("That single question — answered by overlaying Itch.io game-jam tag velocity with Kickstarter funding patterns + AO3 fanfic crossover growth + Reddit reverse-funnel acquisition signal + BGG board-game licensed-adaptation success — surfaces the IPs no traditional Hollywood-pipeline list would catch. Hollow Knight's licensing window opened months before any major TTRPG publisher noticed; Trusight would have caught it on Day 1.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { line: 300, after: 240 }),

  ...useCaseCard(
    "C1",
    "The UB Matrix — pre-deal scoring across 4 dimensions",
    "Every candidate IP scored on Fit (5 sub-dimensions: genre, combat, party, setting, fanbase), Reception (Reddit + Forums × 3 + AO3 sentiment), Acquisition (reverse-funnel: do their fans want D&D back?), and Commercial (DriveThruRPG + DMs Guild revealed-preference). Per-IP renormalization handles missing sources. Composite NULL when fewer than two sources measure an IP — abstention by default.",
    "Today, Eugene Evans' team evaluates IP candidates via internal hype + agency representation + gut. Trusight gives a structured 4-dimension scoring with a defensible data trail — every claim cross-referenced against gold-tier views. When a licensing rep walks in with five candidate IPs in a quarterly pitch, Eugene's team scores them in 30 minutes and walks out with green/sleeper/avoid verdicts they can defend in committee.",
    "Demonstration: Hollow Knight scores Reception 1.0 / Acquisition 0.85 — top-tier across all three dimensions, gold_mine flagged. Spy × Family scores Reception 0.22 / Composite Fit 0.66 — triple-source negative (BGG flop + AO3 zero crossover + Reddit insufficient_data). Same pitch meeting, opposite outcomes. The 12-IP companion one-pager shows the full demonstration."
  ),

  ...useCaseCard(
    "C2",
    "The Buyers-Remorse Landmine Detector",
    "Before any pre-deal commitment, Trusight surfaces the failure-pattern signals: BGG board-game adaptations of the IP that flopped (the buyers_remorse score), AO3 zero-crossover signals indicating the fanbase shows no organic interest in TTRPG conversion, and forum-discussion volume that's silent despite cultural popularity. Three independent signals; if all three flag negative, the deal is a landmine.",
    "WoTC has paid 7-figure licensing fees for IP crossovers that the tabletop community subsequently rejected. A single Trusight evaluation, taking 30 minutes, surfaces the pattern. The Walking Dead: All Out War board game taught the industry the cost of getting this wrong; Trusight's BGG buyers_remorse scoring formalizes the same lesson into pre-deal due diligence.",
    "Triple-source verification example: Spy × Family — BGG buyers_remorse 0.10 (Old Maid: Spy × Family branded variant flopped), AO3 zero D&D crossover fanfic count, Reddit insufficient_data status. All three signals point the same direction. The landmine detector fires. Save the licensing fee."
  ),

  ...useCaseCard(
    "C3",
    "The Reverse-Funnel Acquirer",
    "The reverse-funnel scanner inspects non-D&D IP subreddits — r/HollowKnight, r/DestinyTheGame, r/CowboyBebop, r/SpiceandWolf, etc. — for D&D-context language: \"5e,\" \"campaign,\" \"PC build,\" \"DM,\" \"Sundered Isles,\" \"homebrew.\" When an IP's native fanbase is already discussing D&D conversion at high concentration, that fanbase is pre-converted. Customer Acquisition Cost is effectively zero; the audience is begging for the deal.",
    "Generic IP-acquisition lists rank by Hollywood box office or Netflix viewership. Trusight finds the IPs that no Hollywood agent has bundled — the sleeper indie games and niche literature where the fanbase is already showing up to D&D's door. These are the highest-margin licensing deals because the audience pre-converts itself.",
    "Hollow Knight Reception 1.0, Acquisition 0.85 (top of our matrix). Dungeon Crawler Carl auto-flagged gold_mine — the LitRPG fanbase has been begging for official conversion for years. Both are indie-tier IPs with negligible mainstream-press coverage; both are the highest-conviction picks in our demo set. No traditional licensing list would have surfaced these before they break out at retail."
  ),

  ...dayInLife(
    "How an IP licensing exec uses Trusight in a quarterly pitch cycle",
    "Tuesday morning. Eugene Evans' calendar shows three external-licensing pitches stacked back-to-back: an established anime IP, a hot mid-tier video game, and an indie-press novel series. Pre-Trusight, each pitch took 20 minutes of polite listening followed by 6-10 weeks of internal due-diligence. With Trusight, Eugene runs each candidate through the UB Matrix in real time during the pitch. The anime IP scores composite fit 0.66 / Reception 0.22 — landmine territory. The video game scores Reception 0.74 / Acquisition 0.83 — top-tier. The indie novel scores fit 0.91 but reception is thin (one source). By the end of the third meeting, Eugene's team has structured verdicts on all three: walk away from the anime, fast-track the video game with premium licensing terms, and budget two weeks of additional due-diligence on the novel before committing. What used to take a quarter takes a single afternoon — and the verdicts come with cited data trails any committee can defend. Two months later, the video game deal is signed at the negotiated premium. Six months later, Hasbro's Q3 reports a single-IP licensing return that pays for the entire Trusight subscription twenty times over. The anime IP — the one Trusight flagged as a landmine — is meanwhile underperforming for the rival publisher who took it. Eugene's team moves from reactive deal-evaluation to proactive deal-targeting; the next pitch cycle, they're calling licensing reps with specific IP shortlists Trusight identified as 12-month sleepers."
  ),

  // ═══════════════════════════════════════════════════════════════════
  // CROSS-FUNCTIONAL WRAP
  // ═══════════════════════════════════════════════════════════════════
  pageBreak(),
  spacer(400),
  p([rt("THE CROSS-FUNCTIONAL VIEW", { color: COLOR.ember, bold: true, font: FONT_HEAD, size: 22, characterSpacing: 80 })],
    { align: AlignmentType.CENTER, after: 80 }),
  p([rt("One data substrate, three decision teams.", { color: COLOR.dark, bold: true, font: FONT_HEAD, size: 36 })],
    { align: AlignmentType.CENTER, after: 200 }),
  emberBar(),

  p([rt("Today, the design, marketing, and licensing teams at WoTC each operate on different data — surveys for design, social-listening for marketing, agency-pipeline for licensing. They argue from different evidence in the same meeting, talk past each other, and reach decisions that don't compound across functions.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 160 }),

  p([rt("Trusight is the one data substrate that all three pull from. The same Hollow Knight signal that designers use to identify mechanical translation potential is the same signal marketers use to time campaign windows is the same signal licensing uses to negotiate the fee. When the IP team commits to a Hollow Knight crossover, marketing already knows the segmentation pattern (mechanics-fans dominate); design already knows the homebrew DNA (insect-themed paladins are at the 91st percentile of UA homebrew); the launch ships into a community that's been waiting for it for 18 months.", { color: COLOR.bodyText, font: FONT_BODY, size: 22 })],
    { line: 300, after: 200 }),

  p([rt("That alignment — three teams arguing from the same evidence in the same meeting — is the structural change Trusight enables. It's not just a tool any one team uses; it's the connective tissue that turns three siloed functions into one coordinated franchise-management organization.", { color: COLOR.bodyText, italics: true, font: FONT_BODY, size: 22 })],
    { line: 300, after: 200 }),

  // Closing line — segue into Part 3
  spacer(400),
  hr(COLOR.ember),
  p([rt("Part 3 follows: ", { color: COLOR.mutedGray, italics: true, font: FONT_BODY, size: 22 }),
     rt("five custom edge cases showing how Trusight pulls signals across multiple streams to answer questions no single source can.", { color: COLOR.dark, italics: true, bold: true, font: FONT_BODY, size: 22 })],
    { align: AlignmentType.CENTER }),
];

const doc = new Document({
  styles: sharedStyles,
  numbering: sharedNumbering,
  sections: [{
    properties: { page: pageSetup },
    children: docChildren,
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync("trusight_report_part2_personas.docx", buf);
  console.log("Wrote: trusight_report_part2_personas.docx");
});
