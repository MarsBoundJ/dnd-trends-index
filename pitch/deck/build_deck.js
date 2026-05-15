/**
 * Trusight: a D&D Trendwatch — Pitch Deck v1
 * For WotC executive outreach (Eugene Evans, Dan Ayoub, Jerome Lalin, David Schwartz).
 *
 * Palette: Obsidian & Ember (matches the actual product UI)
 * Typography: Trebuchet MS (headers) + Calibri (body)
 * Visual motif: left-edge ember rail + bottom Trusight wordmark
 */

const pptxgen = require("pptxgenjs");
const path = require("path");

const pres = new pptxgen();

const LOGO_DARK_PATH = path.join(__dirname, "..", "assets", "logos", "trusight_logo_4k_dark.png");
pres.layout = "LAYOUT_16x9"; // 10" × 5.625" — Google Slides default
pres.author = "Phil Jones (Trusight)";
pres.title = "Trusight: a D&D Trendwatch";

// ── Palette ───────────────────────────────────────────────────────────
const C = {
  obsidian:    "0a0a18",  // primary background
  obsidian2:   "1a1a2e",  // panel/card fill
  obsidianBd:  "2a2a4a",  // panel border
  ember:       "e87722",  // primary accent
  cream:       "e0e0ff",  // body text on dark
  white:       "ffffff",  // titles, key data
  green:       "5fdc7c",  // positive
  amber:       "d9a64a",  // cautionary/divisive
  red:         "ff8888",  // backlash/negative
  blue:        "7aa3ff",  // informational
  mutedGray:   "888888",  // captions, sources
  mutedGray2:  "666666",  // very muted
};

const FONT_HEAD = "Trebuchet MS";
const FONT_BODY = "Calibri";

// ── Helpers ───────────────────────────────────────────────────────────
function addBackground(slide) {
  slide.background = { color: C.obsidian };
}

function addLeftRail(slide) {
  // Brand-signature ember rail on the left edge, full height.
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 0.12, h: 5.625,
    fill: { color: C.ember }, line: { type: "none" },
  });
}

function addFooter(slide, slideNum) {
  // Trusight beta logo bottom-center (replaces wordmark text). Aspect ~6.22:1.
  slide.addImage({
    path: LOGO_DARK_PATH,
    x: 4.225, y: 5.30, w: 1.55, h: 0.25,
  });
  // Slide number bottom-right
  slide.addText(String(slideNum), {
    x: 9.4, y: 5.30, w: 0.5, h: 0.25,
    fontSize: 9, fontFace: FONT_BODY,
    color: C.mutedGray2, align: "right", margin: 0,
  });
}

function addTitle(slide, title, opts = {}) {
  slide.addText(title, {
    x: 0.5, y: 0.4, w: 9, h: 0.8,
    fontSize: opts.size || 32, fontFace: FONT_HEAD, bold: true,
    color: opts.color || C.white, align: "left", valign: "top",
    margin: 0,
  });
}

function addEyebrow(slide, text) {
  // Small uppercase ember label above titles — section marker
  slide.addText(text, {
    x: 0.5, y: 0.20, w: 9, h: 0.22,
    fontSize: 10, fontFace: FONT_HEAD, bold: true,
    color: C.ember, align: "left", charSpacing: 4, margin: 0,
  });
}

// Standard content slide: rail + footer + slide number, dark bg
function newContentSlide(slideNum) {
  const slide = pres.addSlide();
  addBackground(slide);
  addLeftRail(slide);
  addFooter(slide, slideNum);
  return slide;
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 1 — Title
// ───────────────────────────────────────────────────────────────────────
{
  const slide = pres.addSlide();
  addBackground(slide);
  addLeftRail(slide);

  // Brand mark — large, vertically centered (Trusight beta logo). Aspect ~6.22:1.
  slide.addImage({
    path: LOGO_DARK_PATH,
    x: 0.5, y: 1.85, w: 7.46, h: 1.2,
  });
  // Tagline — italic, smaller, cream
  slide.addText("a D&D Trendwatch", {
    x: 0.5, y: 2.95, w: 9, h: 0.5,
    fontSize: 24, fontFace: FONT_HEAD, italic: true,
    color: C.cream, align: "left", margin: 0,
  });

  // Thin ember underline accent — subtle, brand-signature
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 3.65, w: 1.2, h: 0.04,
    fill: { color: C.ember }, line: { type: "none" },
  });

  // Recipient line (customizable per-recipient before send)
  slide.addText("Prepared for [Recipient Name]", {
    x: 0.5, y: 3.95, w: 9, h: 0.4,
    fontSize: 16, fontFace: FONT_BODY,
    color: C.cream, align: "left", margin: 0,
  });

  // Author + date
  slide.addText("Phil Jones  ·  May 2026", {
    x: 0.5, y: 4.95, w: 9, h: 0.3,
    fontSize: 12, fontFace: FONT_BODY,
    color: C.mutedGray, align: "left", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 2 — Hook
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(2);
  addEyebrow(slide, "THE PATTERN IN YOUR OWN Q4 NUMBERS");
  addTitle(slide, "Magic is printing. D&D is sliding.");

  slide.addText("Hasbro's own Q4 2025 numbers say so.", {
    x: 0.5, y: 1.2, w: 9, h: 0.4,
    fontSize: 18, fontFace: FONT_BODY, italic: true,
    color: C.cream, align: "left", margin: 0,
  });

  // Three big stat columns
  const stats = [
    { val: "+45%",    valSize: 60, label: "WotC segment\nfull-year revenue",       sub: "$2.2B · $1B+ operating profit",                color: C.green },
    { val: "+59%",    valSize: 60, label: "MTG specifically\nYoY (Avatar UB Q4)",   sub: "Universes Beyond is the engine",                color: C.green },
    { val: "< 44.7%", valSize: 40, label: "D&D + Digital Gaming\nimplied combined", sub: "WotC=44.7%, MTG=59% → D&D drags the avg",       color: C.amber },
  ];
  stats.forEach((s, i) => {
    const x = 0.5 + i * 3.1;
    // Card background
    slide.addShape(pres.shapes.RECTANGLE, {
      x, y: 1.95, w: 2.9, h: 2.7,
      fill: { color: C.obsidian2 }, line: { color: C.obsidianBd, width: 1 },
    });
    // Stat number
    slide.addText(s.val, {
      x, y: 2.05, w: 2.9, h: 1.0,
      fontSize: s.valSize, fontFace: FONT_HEAD, bold: true,
      color: s.color, align: "center", valign: "middle", margin: 0,
    });
    // Label
    slide.addText(s.label, {
      x: x + 0.15, y: 3.15, w: 2.6, h: 0.7,
      fontSize: 14, fontFace: FONT_BODY, bold: true,
      color: C.cream, align: "center", margin: 0,
    });
    // Sub
    slide.addText(s.sub, {
      x: x + 0.15, y: 3.95, w: 2.6, h: 0.6,
      fontSize: 10, fontFace: FONT_BODY, italic: true,
      color: C.mutedGray, align: "center", margin: 0,
    });
  });

  // Source citation
  slide.addText("Source: Hasbro Q4 / Full Year 2025 financial release (investor.hasbro.com, Feb 2026)", {
    x: 0.5, y: 4.85, w: 9, h: 0.25,
    fontSize: 9, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray2, align: "left", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 3 — The OGL setup
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(3);
  addEyebrow(slide, "WHY THIS MATTERS — JANUARY 2023");
  addTitle(slide, "The horizontal-tool failure");

  // Timeline-style content
  const events = [
    { day: "Day 1", text: "WotC drafts OGL 1.5, deauthorizing the OGL 1.0a covenant." },
    { day: "Day 3", text: "TTRPG community organizes #OpenDnD. Mass D&D Beyond cancellations begin." },
    { day: "Day 7", text: "Sprinklr / Brandwatch / NetBase finally register the volume spike — after the boycott has organized." },
    { day: "Day 14", text: "Mass exodus to Paizo. WotC issues full retraction. OGL 1.0a remains. Brand damage permanent." },
  ];

  events.forEach((e, i) => {
    const y = 1.30 + i * 0.78;
    // Day badge
    slide.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y, w: 1.0, h: 0.55,
      fill: { color: C.ember }, line: { type: "none" },
    });
    slide.addText(e.day, {
      x: 0.5, y, w: 1.0, h: 0.55,
      fontSize: 13, fontFace: FONT_HEAD, bold: true,
      color: C.obsidian, align: "center", valign: "middle", margin: 0,
    });
    // Event text
    slide.addText(e.text, {
      x: 1.7, y, w: 7.8, h: 0.55,
      fontSize: 14, fontFace: FONT_BODY,
      color: C.cream, align: "left", valign: "middle", margin: 0,
    });
  });

  // Punchline at bottom
  slide.addText('"$1B-margin business. Billions in social-listening tooling. The horizontal stack missed it."', {
    x: 0.5, y: 4.7, w: 9, h: 0.5,
    fontSize: 14, fontFace: FONT_HEAD, italic: true, bold: true,
    color: C.amber, align: "center", valign: "middle", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 4 — Why horizontal tools fail
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(4);
  addEyebrow(slide, "THE STRUCTURAL GAP");
  addTitle(slide, "Horizontal tools can't parse vertical TTRPG ontology.");

  // Example sentence in a quote panel
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 1.5, w: 9, h: 1.0,
    fill: { color: C.obsidian2 }, line: { color: C.ember, width: 1 },
  });
  slide.addText('"This toxic mechanic absolutely destroys the meta. I\'m abusing it all night."', {
    x: 0.7, y: 1.55, w: 8.6, h: 0.9,
    fontSize: 17, fontFace: FONT_HEAD, italic: true,
    color: C.cream, align: "center", valign: "middle", margin: 0,
  });

  // Two-column comparison
  const colY = 2.85;
  const colH = 1.85;

  // LEFT: How NetBase / Sprinklr / Brandwatch reads it
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: colY, w: 4.4, h: colH,
    fill: { color: C.obsidian2 }, line: { color: C.red, width: 1 },
  });
  slide.addText("HORIZONTAL TOOLS READ:", {
    x: 0.65, y: colY + 0.10, w: 4.1, h: 0.3,
    fontSize: 11, fontFace: FONT_HEAD, bold: true,
    color: C.red, charSpacing: 2, margin: 0,
  });
  slide.addText("NEGATIVE", {
    x: 0.65, y: colY + 0.50, w: 4.1, h: 0.5,
    fontSize: 28, fontFace: FONT_HEAD, bold: true,
    color: C.red, margin: 0,
  });
  slide.addText("Keywords: 'toxic', 'destroys', 'abusing'.\nGeneric NLP flags as crisis signal.", {
    x: 0.65, y: colY + 1.05, w: 4.1, h: 0.7,
    fontSize: 12, fontFace: FONT_BODY,
    color: C.cream, margin: 0,
  });

  // RIGHT: What it actually means
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 5.1, y: colY, w: 4.4, h: colH,
    fill: { color: C.obsidian2 }, line: { color: C.green, width: 1 },
  });
  slide.addText("ACTUAL TTRPG MEANING:", {
    x: 5.25, y: colY + 0.10, w: 4.1, h: 0.3,
    fontSize: 11, fontFace: FONT_HEAD, bold: true,
    color: C.green, charSpacing: 2, margin: 0,
  });
  slide.addText("HIGH ENGAGEMENT", {
    x: 5.25, y: colY + 0.50, w: 4.1, h: 0.5,
    fontSize: 24, fontFace: FONT_HEAD, bold: true,
    color: C.green, margin: 0,
  });
  slide.addText("Player adopting a new card mechanic enthusiastically.\nLeading indicator for booster-pack sales.", {
    x: 5.25, y: colY + 1.05, w: 4.1, h: 0.7,
    fontSize: 12, fontFace: FONT_BODY,
    color: C.cream, margin: 0,
  });

  // Footer line
  slide.addText("This pattern repeats across every TTRPG community discussion. Horizontal tools mis-classify the brand-purity signal that actually drives revenue.", {
    x: 0.5, y: 4.85, w: 9, h: 0.3,
    fontSize: 10, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "left", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 5 — What Trusight is
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(5);
  addEyebrow(slide, "THE PRODUCT");
  addTitle(slide, "Trusight is the vertical telemetry layer your D&D org is missing.");

  // Three definition lines, bullet-style with ember markers
  const defs = [
    { head: "Vertical, not horizontal.", body: "Built specifically for D&D's community structure. Forum threads, fanfic crossovers, homebrew artifacts, marketplace bestsellers — all parsed with TTRPG-native ontology." },
    { head: "Continuous, not project-based.", body: "Always-on telemetry. Brand trackers refresh quarterly. Trusight refreshes daily. Your team sees emerging signal weeks before it surfaces in Touchstone or Euromonitor reports." },
    { head: "Brief-shaped, not dashboard-shaped.", body: "Built for the Product Architect's workflow. When they're evaluating UB candidates for FY27, Trusight outputs the brief — Fit, Reception, Acquisition, Commercial — with the data trail they need to defend the call." },
  ];

  defs.forEach((d, i) => {
    const y = 1.65 + i * 1.10;
    // Ember marker (small square)
    slide.addShape(pres.shapes.RECTANGLE, {
      x: 0.5, y: y + 0.05, w: 0.18, h: 0.18,
      fill: { color: C.ember }, line: { type: "none" },
    });
    // Heading
    slide.addText(d.head, {
      x: 0.85, y, w: 8.6, h: 0.35,
      fontSize: 18, fontFace: FONT_HEAD, bold: true,
      color: C.white, margin: 0,
    });
    // Body
    slide.addText(d.body, {
      x: 0.85, y: y + 0.40, w: 8.6, h: 0.65,
      fontSize: 13, fontFace: FONT_BODY,
      color: C.cream, margin: 0,
    });
  });

  // Footer callout
  slide.addText("Built for one user persona: the Product Architect, evaluating crossover IPs against D&D's actual community signal.", {
    x: 0.5, y: 4.85, w: 9, h: 0.3,
    fontSize: 11, fontFace: FONT_BODY, italic: true,
    color: C.amber, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 6 — The data substrate
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(6);
  addEyebrow(slide, "BREADTH × DEPTH");
  addTitle(slide, "8 stages. 12+ live signal streams. $2.25 cumulative build cost.");

  // Stage grid: 4 columns × 2 rows. Card titles aggressively trimmed
  // to fit one line at 12pt within the narrow card width.
  const stages = [
    { num: "1", name: "Gemini baseline",  sub: "142 IPs scored on 6-dim rubric" },
    { num: "2", name: "Precedent DBs",    sub: "MTG UB + D&D crossover history" },
    { num: "3", name: "BGG proxy",        sub: "Tabletop buyers' prior reception" },
    { num: "4", name: "AO3 crossover",    sub: "Organic revealed preference" },
    { num: "5", name: "Reddit signal",    sub: "25 subs, 2-axis classification" },
    { num: "6", name: "Homebrew",         sub: "UA + GMBinder + Homebrewery + DDB" },
    { num: "7", name: "TTRPG forums",     sub: "EN World + RPG.net + GitP" },
    { num: "8", name: "Commercial",       sub: "DriveThruRPG + DMs Guild bestsellers" },
  ];

  const cardW = 2.18;
  const cardH = 1.55;
  const startX = 0.5;
  const startY = 1.45;
  const gapX = 0.10;
  const gapY = 0.10;

  stages.forEach((s, i) => {
    const col = i % 4;
    const row = Math.floor(i / 4);
    const x = startX + col * (cardW + gapX);
    const y = startY + row * (cardH + gapY);

    slide.addShape(pres.shapes.RECTANGLE, {
      x, y, w: cardW, h: cardH,
      fill: { color: C.obsidian2 }, line: { color: C.obsidianBd, width: 1 },
    });
    // Stage number badge — fixed top-left
    slide.addText(s.num, {
      x: x + 0.10, y: y + 0.10, w: 0.50, h: 0.40,
      fontSize: 22, fontFace: FONT_HEAD, bold: true,
      color: C.ember, margin: 0,
    });
    // Stage name — fixed single-line zone (vcenter on its row for consistency)
    slide.addText(s.name, {
      x: x + 0.55, y: y + 0.18, w: cardW - 0.65, h: 0.32,
      fontSize: 12, fontFace: FONT_HEAD, bold: true,
      color: C.white, margin: 0, valign: "middle",
    });
    // Stage subtext — fixed at lower-middle of card
    slide.addText(s.sub, {
      x: x + 0.10, y: y + 0.85, w: cardW - 0.20, h: 0.60,
      fontSize: 10, fontFace: FONT_BODY,
      color: C.cream, margin: 0,
    });
  });

  // Bottom callout
  slide.addText("Each stage = independent measurement. Composite scores triangulate across stages with per-IP renormalization for missing sources.", {
    x: 0.5, y: 4.95, w: 9, h: 0.3,
    fontSize: 10, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "left", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 7 — The UB Matrix
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(7);
  addEyebrow(slide, "THE FLAGSHIP FEATURE");
  addTitle(slide, "The UB Matrix — 142 candidate IPs × 4 dimensions.");

  // Four-dimension columns. detail uses rich-text array form (with
  // breakLine) so all four columns reliably render as 2 lines and
  // baselines align across the row.
  const dims = [
    { name: "FIT",         sub: "5 sub-dimensions",    line1: "genre · combat · party",       line2: "setting · fanbase overlap",  color: C.blue },
    { name: "RECEPTION",   sub: "would D&D play it",   line1: "forums (3) + Reddit + AO3",    line2: "sentiment, weighted",         color: C.green },
    { name: "ACQUISITION", sub: "would they play D&D", line1: "reverse-funnel: do their",     line2: "fans want D&D back?",         color: C.amber },
    { name: "COMMERCIAL",  sub: "revealed preference", line1: "DriveThruRPG + DMs Guild",     line2: "bestseller-tier presence",    color: C.ember },
  ];

  const colW = 2.18;
  const colH = 2.6;
  const startX = 0.5;
  const startY = 1.55;
  const gap = 0.10;

  dims.forEach((d, i) => {
    const x = startX + i * (colW + gap);
    slide.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: colW, h: colH,
      fill: { color: C.obsidian2 }, line: { color: d.color, width: 2 },
    });
    // Dimension name
    slide.addText(d.name, {
      x: x + 0.10, y: startY + 0.20, w: colW - 0.20, h: 0.5,
      fontSize: 18, fontFace: FONT_HEAD, bold: true,
      color: d.color, align: "center", margin: 0, charSpacing: 2,
    });
    // Sub
    slide.addText(d.sub, {
      x: x + 0.10, y: startY + 0.75, w: colW - 0.20, h: 0.3,
      fontSize: 11, fontFace: FONT_BODY, italic: true,
      color: C.mutedGray, align: "center", margin: 0,
    });
    // Detail — rich-text array enforces consistent 2-line break across columns
    slide.addText([
      { text: d.line1, options: { breakLine: true } },
      { text: d.line2 },
    ], {
      x: x + 0.10, y: startY + 1.30, w: colW - 0.20, h: 1.2,
      fontSize: 12, fontFace: FONT_BODY,
      color: C.cream, align: "center", valign: "middle", margin: 0,
    });
  });

  // Bottom annotation — rewritten to avoid orphan fragments
  slide.addText("Per-IP renormalization handles missing signals without penalty. Abstention is the default when fewer than two sources measure an IP.", {
    x: 0.5, y: 4.45, w: 9, h: 0.4,
    fontSize: 12, fontFace: FONT_BODY, italic: true,
    color: C.amber, align: "center", margin: 0,
  });

  slide.addText("This is the dashboard your Product Architects pull a brief from.", {
    x: 0.5, y: 4.90, w: 9, h: 0.3,
    fontSize: 12, fontFace: FONT_HEAD, bold: true,
    color: C.white, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 8 — Proof: Goblin Slayer triangulation
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(8);
  addEyebrow(slide, "PROOF #1 — TRIANGULATED BACKLASH");
  addTitle(slide, "Goblin Slayer: three forums, one verdict.");

  // Three forum cards horizontally
  const forums = [
    { name: "EN WORLD", domain: "enworld.org", quote: "...every bad stereotype from 80s D&D male teen players, including sexualized violence." },
    { name: "GIANT IN THE PLAYGROUND", domain: "forums.giantitp.com", quote: "...debating if the manga's grimdark and exploitative tone fits D&D." },
    { name: "RPG.NET", domain: "rpg.net", quote: "...gratuitous rape and dark and edgy elements, creating a tone mismatch with D&D-inspired fantasy." },
  ];

  const cardW = 2.95;
  const cardH = 2.7;
  const startX = 0.5;
  const startY = 1.4;
  const gap = 0.07;

  forums.forEach((f, i) => {
    const x = startX + i * (cardW + gap);
    slide.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: cardW, h: cardH,
      fill: { color: C.obsidian2 }, line: { color: C.red, width: 1 },
    });
    // Forum name
    slide.addText(f.name, {
      x: x + 0.15, y: startY + 0.15, w: cardW - 0.30, h: 0.30,
      fontSize: 11, fontFace: FONT_HEAD, bold: true,
      color: C.red, charSpacing: 2, margin: 0,
    });
    // Domain
    slide.addText(f.domain, {
      x: x + 0.15, y: startY + 0.45, w: cardW - 0.30, h: 0.25,
      fontSize: 10, fontFace: FONT_BODY, italic: true,
      color: C.mutedGray, margin: 0,
    });
    // Quote
    slide.addText(`"${f.quote}"`, {
      x: x + 0.15, y: startY + 0.85, w: cardW - 0.30, h: 1.6,
      fontSize: 12, fontFace: FONT_BODY, italic: true,
      color: C.cream, valign: "top", margin: 0,
    });
    // Tag at bottom
    slide.addShape(pres.shapes.RECTANGLE, {
      x: x + 0.15, y: startY + cardH - 0.45, w: cardW - 0.30, h: 0.30,
      fill: { color: C.obsidian }, line: { color: C.red, width: 1 },
    });
    slide.addText("classified: tone_mismatch", {
      x: x + 0.15, y: startY + cardH - 0.45, w: cardW - 0.30, h: 0.30,
      fontSize: 10, fontFace: FONT_HEAD, bold: true,
      color: C.red, align: "center", valign: "middle", margin: 0,
    });
  });

  // Punchline
  slide.addText("Three independent DM communities, separately classified from raw thread text — all reaching the same verdict on the same IP for the same reason.", {
    x: 0.5, y: 4.30, w: 9, h: 0.4,
    fontSize: 13, fontFace: FONT_BODY,
    color: C.cream, align: "center", margin: 0,
  });
  slide.addText("Triangulation rule: ≥2 independent forums + same narrative type before backlash surfaces. Single-forum noise filtered out.", {
    x: 0.5, y: 4.80, w: 9, h: 0.4,
    fontSize: 12, fontFace: FONT_HEAD, bold: true, italic: true,
    color: C.amber, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 9 — Proof: DMs Guild structural-zero
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(9);
  addEyebrow(slide, "PROOF #2 — UNMET CREATOR DEMAND");
  addTitle(slide, "Your own platform structurally cannot host the content your players want.");

  // Two-column comparison — both cards exactly same height + identical
  // structural slots (header, sub, big-stat region, bottom note).
  const cardY = 1.45;
  const cardH = 3.05;

  // LEFT: DMs Guild — structural zero
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: cardY, w: 4.4, h: cardH,
    fill: { color: C.obsidian2 }, line: { color: C.red, width: 1 },
  });
  slide.addText("DMS GUILD", {
    x: 0.65, y: cardY + 0.10, w: 4.1, h: 0.32,
    fontSize: 16, fontFace: FONT_HEAD, bold: true,
    color: C.red, charSpacing: 3, margin: 0,
  });
  slide.addText("WotC's licensed creator marketplace", {
    x: 0.65, y: cardY + 0.42, w: 4.1, h: 0.28,
    fontSize: 11, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, margin: 0,
  });
  // Big "0"
  slide.addText("0", {
    x: 0.65, y: cardY + 0.82, w: 4.1, h: 1.20,
    fontSize: 92, fontFace: FONT_HEAD, bold: true,
    color: C.red, align: "center", valign: "middle", margin: 0,
  });
  // Caption directly below
  slide.addText("of 8,062 bestseller-tier products", {
    x: 0.65, y: cardY + 2.05, w: 4.1, h: 0.28,
    fontSize: 13, fontFace: FONT_BODY, bold: true,
    color: C.cream, align: "center", margin: 0,
  });
  slide.addText("are about any UB candidate IP", {
    x: 0.65, y: cardY + 2.32, w: 4.1, h: 0.28,
    fontSize: 13, fontFace: FONT_BODY, bold: true,
    color: C.cream, align: "center", margin: 0,
  });
  slide.addText("(license rules forbid third-party IP)", {
    x: 0.65, y: cardY + 2.65, w: 4.1, h: 0.28,
    fontSize: 10, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "center", margin: 0,
  });

  // RIGHT: DriveThruRPG — bestselling licensed (same structural shape)
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 5.1, y: cardY, w: 4.4, h: cardH,
    fill: { color: C.obsidian2 }, line: { color: C.green, width: 1 },
  });
  slide.addText("DRIVETHRURPG", {
    x: 5.25, y: cardY + 0.10, w: 4.1, h: 0.32,
    fontSize: 16, fontFace: FONT_HEAD, bold: true,
    color: C.green, charSpacing: 3, margin: 0,
  });
  slide.addText("Open-licensing TTRPG marketplace", {
    x: 5.25, y: cardY + 0.42, w: 4.1, h: 0.28,
    fontSize: 11, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, margin: 0,
  });
  // Three product rows occupy the same vertical area as the "0"
  const dtrpg = [
    { brand: "FALLOUT RPG",    detail: "Modiphius — 5 Platinum-tier SKUs" },
    { brand: "CYBERPUNK RED",  detail: "R. Talsorian — 25 SKUs, 11 licensed" },
    { brand: "COSMERE RPG",    detail: "Brotherwise — Stormlight + Mistborn" },
  ];
  dtrpg.forEach((d, i) => {
    const ry = cardY + 0.85 + i * 0.62;
    slide.addText(d.brand, {
      x: 5.25, y: ry, w: 4.1, h: 0.28,
      fontSize: 13, fontFace: FONT_HEAD, bold: true,
      color: C.green, margin: 0,
    });
    slide.addText(d.detail, {
      x: 5.25, y: ry + 0.28, w: 4.1, h: 0.28,
      fontSize: 12, fontFace: FONT_BODY,
      color: C.cream, margin: 0,
    });
  });
  slide.addText("(open licensing captures the revenue)", {
    x: 5.25, y: cardY + 2.65, w: 4.1, h: 0.28,
    fontSize: 10, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "center", margin: 0,
  });

  // Punchline
  slide.addText("The asymmetry quantifies revenue you're structurally leaving on the table.", {
    x: 0.5, y: 4.85, w: 9, h: 0.35,
    fontSize: 13, fontFace: FONT_HEAD, bold: true, italic: true,
    color: C.amber, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 10 — Proof: Stranger Things asymmetry
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(10);
  addEyebrow(slide, "PROOF #3 — RECEPTION ≠ ACQUISITION");
  addTitle(slide, "Stranger Things: license for retention, not acquisition.");

  // Side-by-side metric comparison — cards positioned below the title's
  // descender zone (avoids "acquisition" g/q descenders touching frame).
  const cardY = 1.55;
  const cardH = 2.65;

  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: cardY, w: 4.4, h: cardH,
    fill: { color: C.obsidian2 }, line: { color: C.green, width: 1 },
  });
  slide.addText("RECEPTION", {
    x: 0.65, y: cardY + 0.10, w: 4.1, h: 0.30,
    fontSize: 13, fontFace: FONT_HEAD, bold: true,
    color: C.green, charSpacing: 3, margin: 0,
  });
  slide.addText("D&D players want it", {
    x: 0.65, y: cardY + 0.42, w: 4.1, h: 0.28,
    fontSize: 12, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, margin: 0,
  });
  slide.addText("0.85", {
    x: 0.65, y: cardY + 0.80, w: 4.1, h: 1.30,
    fontSize: 84, fontFace: FONT_HEAD, bold: true,
    color: C.green, align: "center", valign: "middle", margin: 0,
  });
  slide.addText("Forums 7 positive / 1 divisive  ·  AO3 crossover HIGH", {
    x: 0.65, y: cardY + 2.20, w: 4.1, h: 0.40,
    fontSize: 11, fontFace: FONT_BODY,
    color: C.cream, align: "center", margin: 0,
  });

  slide.addShape(pres.shapes.RECTANGLE, {
    x: 5.1, y: cardY, w: 4.4, h: cardH,
    fill: { color: C.obsidian2 }, line: { color: C.amber, width: 1 },
  });
  slide.addText("ACQUISITION", {
    x: 5.25, y: cardY + 0.10, w: 4.1, h: 0.30,
    fontSize: 13, fontFace: FONT_HEAD, bold: true,
    color: C.amber, charSpacing: 3, margin: 0,
  });
  slide.addText("Stranger Things fans want D&D back", {
    x: 5.25, y: cardY + 0.42, w: 4.1, h: 0.28,
    fontSize: 12, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, margin: 0,
  });
  slide.addText("0.54", {
    x: 5.25, y: cardY + 0.80, w: 4.1, h: 1.30,
    fontSize: 84, fontFace: FONT_HEAD, bold: true,
    color: C.amber, align: "center", valign: "middle", margin: 0,
  });
  slide.addText("Reverse-funnel: ST fanbases show weak D&D pull-back", {
    x: 5.25, y: cardY + 2.20, w: 4.1, h: 0.40,
    fontSize: 11, fontFace: FONT_BODY,
    color: C.cream, align: "center", margin: 0,
  });

  // Strategic translation
  slide.addText("Strategic translation:", {
    x: 0.5, y: 4.35, w: 9, h: 0.30,
    fontSize: 12, fontFace: FONT_HEAD, bold: true,
    color: C.ember, align: "center", margin: 0,
  });
  slide.addText('"License Stranger Things for retention, not acquisition. The 2019 Starter Set was a retention play. No other tool surfaces this gap."', {
    x: 0.5, y: 4.65, w: 9, h: 0.55,
    fontSize: 12, fontFace: FONT_BODY, italic: true,
    color: C.cream, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 11 — Proof: Tyranny canary (the disambiguation slide)
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(11);
  addEyebrow(slide, "PROOF #4 — TRUST THE ABSTENTION");
  addTitle(slide, "Tyranny: the canary that proves the disambiguation works.");

  // Top context line — clean gap from headline above
  slide.addText('"Tyranny" is an Obsidian Entertainment 2016 RPG. Also a common English noun about despots — fills D&D forums for unrelated reasons.', {
    x: 0.5, y: 1.45, w: 9, h: 0.5,
    fontSize: 13, fontFace: FONT_BODY, italic: true,
    color: C.cream, align: "center", margin: 0,
  });

  // Six-version timeline showing canary stays NULL.
  // v3a = Stage 6a raw (undisambiguated); v3b = Stage 6c (disambig refined).
  const versions = [
    { v: "v1",  name: "baseline\n(presence-only)" },
    { v: "v2",  name: "two-layer\ndisambig" },
    { v: "v3a", name: "+ DDB AI\nBouncer raw" },
    { v: "v3b", name: "+ disambig\nrefined" },
    { v: "v4",  name: "+ Playwright\nbodies" },
    { v: "v5",  name: "+ GitP\ntriangulation" },
  ];

  const cellW = 1.45;
  const cellH = 1.55;
  const startX = 0.5;
  const startY = 2.10;
  const gap = 0.06;

  versions.forEach((vv, i) => {
    const x = startX + i * (cellW + gap);
    slide.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: cellW, h: cellH,
      fill: { color: C.obsidian2 }, line: { color: C.green, width: 1 },
    });
    slide.addText(vv.v, {
      x, y: startY + 0.08, w: cellW, h: 0.30,
      fontSize: 18, fontFace: FONT_HEAD, bold: true,
      color: C.ember, align: "center", margin: 0,
    });
    slide.addText(vv.name, {
      x: x + 0.05, y: startY + 0.40, w: cellW - 0.10, h: 0.50,
      fontSize: 9, fontFace: FONT_BODY, italic: true,
      color: C.mutedGray, align: "center", margin: 0,
    });
    // Verdict checkmark
    slide.addText("NULL", {
      x, y: startY + 0.95, w: cellW, h: 0.30,
      fontSize: 14, fontFace: FONT_HEAD, bold: true,
      color: C.green, align: "center", margin: 0, charSpacing: 2,
    });
    slide.addText("✓ correct", {
      x, y: startY + 1.22, w: cellW, h: 0.25,
      fontSize: 9, fontFace: FONT_BODY, italic: true,
      color: C.green, align: "center", margin: 0,
    });
  });

  // Bottom callouts — tightened to fit cleanly
  slide.addText("Six builds. Six independent re-classifications. Tyranny correctly returns 'no_confirmed_signal' every time.", {
    x: 0.5, y: 3.85, w: 9, h: 0.3,
    fontSize: 12, fontFace: FONT_BODY,
    color: C.cream, align: "center", margin: 0,
  });
  slide.addText("51 of 142 IPs are ambiguity-flagged. Two-layer disambiguation: co-term gating + AI Bouncer.", {
    x: 0.5, y: 4.20, w: 9, h: 0.3,
    fontSize: 11, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "center", margin: 0,
  });
  slide.addText("If Trusight doesn't have signal, it says so. Your licensing decisions can't survive false positives.", {
    x: 0.5, y: 4.65, w: 9, h: 0.4,
    fontSize: 12, fontFace: FONT_HEAD, bold: true, italic: true,
    color: C.amber, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 12 — Integration into your stack
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(12);
  addEyebrow(slide, "FITS YOUR EXISTING STACK");
  addTitle(slide, "Drops into Snowflake → dbt → Looker. No new BI tool.");

  // Pipeline flow visual
  const pipeline = [
    { label: "TRUSIGHT", sub: "vertical telemetry layer", color: C.ember },
    { label: "SNOWFLAKE", sub: "Secure Share into your warehouse", color: C.blue },
    { label: "dbt", sub: "materialize signals into your models", color: C.green },
    { label: "LOOKER", sub: "surface in dashboards your team already uses", color: C.amber },
  ];

  const stepW = 2.0;
  const stepH = 1.4;
  const startX = 0.6;
  const startY = 1.80;
  const gap = 0.30;

  pipeline.forEach((p, i) => {
    const x = startX + i * (stepW + gap);
    slide.addShape(pres.shapes.RECTANGLE, {
      x, y: startY, w: stepW, h: stepH,
      fill: { color: C.obsidian2 }, line: { color: p.color, width: 2 },
    });
    slide.addText(p.label, {
      x: x + 0.10, y: startY + 0.20, w: stepW - 0.20, h: 0.45,
      fontSize: 16, fontFace: FONT_HEAD, bold: true,
      color: p.color, align: "center", margin: 0,
    });
    slide.addText(p.sub, {
      x: x + 0.10, y: startY + 0.70, w: stepW - 0.20, h: 0.65,
      fontSize: 10, fontFace: FONT_BODY,
      color: C.cream, align: "center", margin: 0,
    });

    // Arrow between cards
    if (i < pipeline.length - 1) {
      const arrowX = x + stepW + 0.05;
      slide.addText("›", {
        x: arrowX, y: startY + 0.40, w: 0.20, h: 0.6,
        fontSize: 32, fontFace: FONT_HEAD, bold: true,
        color: C.ember, align: "center", valign: "middle", margin: 0,
      });
    }
  });

  // Speaks-Hight's-language callout
  slide.addText("Speaks the live-service language your org runs on:", {
    x: 0.5, y: 3.60, w: 9, h: 0.3,
    fontSize: 12, fontFace: FONT_HEAD, bold: true,
    color: C.ember, align: "center", margin: 0,
  });

  // Translation table
  const trans = [
    { left: "Continuous instrumentation",  right: "(not project-based research)" },
    { left: "Cohort-style analysis",        right: "(not aggregate sentiment)" },
    { left: "LTV-shaped output",            right: "(not vanity counts)" },
    { left: "Always-on dashboards",         right: "(not quarterly reports)" },
  ];
  trans.forEach((t, i) => {
    const y = 4.00 + i * 0.20;
    slide.addText([
      { text: t.left, options: { color: C.white, bold: true, fontFace: FONT_BODY } },
      { text: "  " + t.right, options: { color: C.mutedGray, italic: true, fontFace: FONT_BODY } },
    ], {
      x: 0.5, y, w: 9, h: 0.25,
      fontSize: 11, align: "center", margin: 0,
    });
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 13 — The ask
// ───────────────────────────────────────────────────────────────────────
{
  const slide = newContentSlide(13);
  addEyebrow(slide, "WHAT WE'RE PROPOSING");
  addTitle(slide, "Three concrete steps.");

  const asks = [
    {
      num: "1",
      head: "30-MINUTE LIVE WALKTHROUGH",
      sub: "Pick five UB candidate IPs of your choice. We screen-share the dashboard + walk you through the data trail behind each score.",
      time: "Next 7 days",
    },
    {
      num: "2",
      head: "14-DAY EVALUATION WINDOW",
      sub: "Full read access to the Trusight dashboard. Snowflake share for your analysts. Direct line to me for any questions during the eval.",
      time: "Days 8–22",
    },
    {
      num: "3",
      head: "Q3 FY26 BUDGET-CYCLE ALIGNMENT",
      sub: "If the eval makes the case for you, license terms negotiated for the budget window you control. SaaS pricing scaled to team size.",
      time: "Aligned to your fiscal calendar",
    },
  ];

  asks.forEach((a, i) => {
    const y = 1.45 + i * 1.15;
    // Number badge
    slide.addShape(pres.shapes.OVAL, {
      x: 0.5, y, w: 0.8, h: 0.8,
      fill: { color: C.ember }, line: { type: "none" },
    });
    slide.addText(a.num, {
      x: 0.5, y, w: 0.8, h: 0.8,
      fontSize: 36, fontFace: FONT_HEAD, bold: true,
      color: C.obsidian, align: "center", valign: "middle", margin: 0,
    });
    // Head + inline time tag (right-aligned within same row)
    slide.addText(a.head, {
      x: 1.5, y, w: 5.6, h: 0.4,
      fontSize: 16, fontFace: FONT_HEAD, bold: true,
      color: C.white, charSpacing: 1, margin: 0,
    });
    slide.addText(a.time, {
      x: 7.1, y: y + 0.05, w: 2.4, h: 0.35,
      fontSize: 11, fontFace: FONT_BODY, italic: true,
      color: C.amber, align: "right", margin: 0,
    });
    // Sub spans full width
    slide.addText(a.sub, {
      x: 1.5, y: y + 0.40, w: 8.0, h: 0.6,
      fontSize: 12, fontFace: FONT_BODY,
      color: C.cream, margin: 0,
    });
  });

  // Pricing callout
  slide.addText("$15K–$100K / month SaaS license, scalable to team size. Pricing transparency available upon request.", {
    x: 0.5, y: 4.95, w: 9, h: 0.3,
    fontSize: 11, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "center", margin: 0,
  });
}

// ───────────────────────────────────────────────────────────────────────
// SLIDE 14 — Contact / close
// ───────────────────────────────────────────────────────────────────────
{
  const slide = pres.addSlide();
  addBackground(slide);
  addLeftRail(slide);

  // Brand mark (Trusight beta logo). Aspect ~6.22:1.
  slide.addImage({
    path: LOGO_DARK_PATH,
    x: 0.5, y: 1.0, w: 6.22, h: 1.0,
  });
  slide.addText("a D&D Trendwatch", {
    x: 0.5, y: 1.85, w: 9, h: 0.4,
    fontSize: 18, fontFace: FONT_HEAD, italic: true,
    color: C.cream, align: "left", margin: 0,
  });

  // Underline accent
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0.5, y: 2.45, w: 1.0, h: 0.04,
    fill: { color: C.ember }, line: { type: "none" },
  });

  // Contact block
  slide.addText("Phil Jones", {
    x: 0.5, y: 2.75, w: 9, h: 0.4,
    fontSize: 22, fontFace: FONT_HEAD, bold: true,
    color: C.white, align: "left", margin: 0,
  });
  slide.addText("Founder, Trusight", {
    x: 0.5, y: 3.15, w: 9, h: 0.3,
    fontSize: 14, fontFace: FONT_BODY, italic: true,
    color: C.mutedGray, align: "left", margin: 0,
  });

  // Contact lines.
  slide.addText([
    { text: "phil@trusightdata.ai",     options: { color: C.cream, bold: true, breakLine: true, fontFace: FONT_BODY } },
    { text: "calendly.com/trusight",     options: { color: C.cream, breakLine: true, fontFace: FONT_BODY } },
    { text: "trusightdata.ai",           options: { color: C.cream, fontFace: FONT_BODY } },
  ], {
    x: 0.5, y: 3.60, w: 9, h: 1.1,
    fontSize: 14, align: "left", margin: 0,
  });

  // Closing line
  slide.addText("The brief WotC's Product Architects don't have yet — but should.", {
    x: 0.5, y: 4.90, w: 9, h: 0.4,
    fontSize: 13, fontFace: FONT_HEAD, italic: true,
    color: C.amber, align: "left", margin: 0,
  });
}

// ── Write file ────────────────────────────────────────────────────────
pres.writeFile({ fileName: "trusight_deck_v1.pptx" })
  .then((fileName) => console.log(`Wrote: ${fileName}`));
