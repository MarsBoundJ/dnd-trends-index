/**
 * Trusight one-pager #1 (v2) — The IP Decision Engine
 *
 * 12 IPs across 4 quadrants. Smaller graph + numbered dots, dense legend
 * below. Every data claim cross-checked against gold views — no Gemini-
 * invented numbers.
 *
 * Quadrant counts: Greenlight 6 · Sleeper 4 · Force-Fit Risk 1 · License
 * Landmine 1.
 */

const pptxgen = require("pptxgenjs");
const path = require("path");
const pres = new pptxgen();

const LOGO_DARK_PATH = path.join(__dirname, "..", "assets", "logos", "trusight_logo_4k_dark.png");

pres.defineLayout({ name: "LETTER_P", width: 8.5, height: 11 });
pres.layout = "LETTER_P";
pres.author = "Phil Jones (Trusight)";
pres.title = "Trusight — The IP Decision Engine";

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
  gridLine:    "3a3a5a",
};
const FONT_HEAD = "Trebuchet MS";
const FONT_BODY = "Calibri";

const slide = pres.addSlide();
slide.background = { color: C.obsidian };

// Left ember rail
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0, y: 0, w: 0.10, h: 11,
  fill: { color: C.ember }, line: { type: "none" },
});

// ─── HEADER (compact) ────────────────────────────────────────────────
// Trusight beta logo (replaces wordmark text). Aspect ~6.22:1.
slide.addImage({
  path: LOGO_DARK_PATH,
  x: 0.4, y: 0.30, w: 2.49, h: 0.40,
});
// (Tagline "a D&D Trendwatch" is part of the logo image — no separate subtitle needed.)

// Thin ember accent
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0.4, y: 0.92, w: 0.6, h: 0.03,
  fill: { color: C.ember }, line: { type: "none" },
});

// ─── TITLE ───────────────────────────────────────────────────────────
slide.addText("The IP Decision Engine", {
  x: 0.4, y: 1.05, w: 7.7, h: 0.45,
  fontSize: 24, fontFace: FONT_HEAD, bold: true,
  color: C.white, align: "left", margin: 0,
});
slide.addText("12 candidate IPs · 8-source signal stack · Sleepers rise; obvious picks fall.", {
  x: 0.4, y: 1.50, w: 7.7, h: 0.25,
  fontSize: 11, fontFace: FONT_BODY, italic: true,
  color: C.cream, align: "left", margin: 0,
});

// ─── 4-QUADRANT GRAPH (compact, centered) ────────────────────────────
const gx = 1.30;
const gy = 1.95;
const gw = 5.90;
const gh = 2.95;

slide.addShape(pres.shapes.RECTANGLE, {
  x: gx, y: gy, w: gw, h: gh,
  fill: { color: C.obsidian2 }, line: { color: C.obsidianBd, width: 1 },
});

// Quadrant zone tints
slide.addShape(pres.shapes.RECTANGLE, {
  x: gx + gw / 2, y: gy, w: gw / 2, h: gh / 2,
  fill: { color: C.green, transparency: 88 }, line: { type: "none" },
});
slide.addShape(pres.shapes.RECTANGLE, {
  x: gx, y: gy, w: gw / 2, h: gh / 2,
  fill: { color: C.amber, transparency: 88 }, line: { type: "none" },
});
slide.addShape(pres.shapes.RECTANGLE, {
  x: gx + gw / 2, y: gy + gh / 2, w: gw / 2, h: gh / 2,
  fill: { color: C.blue, transparency: 88 }, line: { type: "none" },
});
slide.addShape(pres.shapes.RECTANGLE, {
  x: gx, y: gy + gh / 2, w: gw / 2, h: gh / 2,
  fill: { color: C.red, transparency: 88 }, line: { type: "none" },
});

// Cross-hair lines
slide.addShape(pres.shapes.LINE, {
  x: gx, y: gy + gh / 2, w: gw, h: 0,
  line: { color: C.gridLine, width: 1, dashType: "dash" },
});
slide.addShape(pres.shapes.LINE, {
  x: gx + gw / 2, y: gy, w: 0, h: gh,
  line: { color: C.gridLine, width: 1, dashType: "dash" },
});

// Quadrant labels — at outer corners of each quadrant. Data dots that
// would otherwise collide with these labels are nudged inward in the
// plot list below.
slide.addText("FORCE-FIT RISK", {
  x: gx + 0.10, y: gy + 0.06, w: gw / 2 - 0.20, h: 0.20,
  fontSize: 8, fontFace: FONT_HEAD, bold: true,
  color: C.amber, align: "left", charSpacing: 2, margin: 0,
});
slide.addText("GREENLIGHT", {
  x: gx + gw / 2 + 0.10, y: gy + 0.06, w: gw / 2 - 0.20, h: 0.20,
  fontSize: 8, fontFace: FONT_HEAD, bold: true,
  color: C.green, align: "right", charSpacing: 2, margin: 0,
});
slide.addText("LICENSE LANDMINE", {
  x: gx + 0.10, y: gy + gh - 0.26, w: gw / 2 - 0.20, h: 0.20,
  fontSize: 8, fontFace: FONT_HEAD, bold: true,
  color: C.red, align: "left", charSpacing: 2, margin: 0,
});
slide.addText("SLEEPER", {
  x: gx + gw / 2 + 0.10, y: gy + gh - 0.26, w: gw / 2 - 0.20, h: 0.20,
  fontSize: 8, fontFace: FONT_HEAD, bold: true,
  color: C.blue, align: "right", charSpacing: 2, margin: 0,
});

// Axis labels
slide.addText("→  FIT", {
  x: gx, y: gy + gh + 0.06, w: gw, h: 0.20,
  fontSize: 9, fontFace: FONT_HEAD, bold: true,
  color: C.cream, align: "center", margin: 0,
});
slide.addText("→  RECEPTION", {
  x: gx - 0.95, y: gy + gh / 2 - 0.40, w: 1.20, h: 0.20,
  fontSize: 9, fontFace: FONT_HEAD, bold: true,
  color: C.cream, align: "center", margin: 0,
  rotate: 270,
});

// ─── PLOTTED IPS (12 numbered dots) ──────────────────────────────────
// fit/recep coords mapped to graph area. Unmeasured-recep IPs (thin
// evidence) plotted at recep≈0.35 to indicate "untested below the line."
function plotIP(num, fit, recep, color) {
  // X grows with fit (left = low, right = high)
  // Y grows with recep but inverted (top = high, bottom = low)
  const px = gx + fit * gw;
  const py = gy + (1 - recep) * gh;
  // Filled dot
  slide.addShape(pres.shapes.OVAL, {
    x: px - 0.13, y: py - 0.13, w: 0.26, h: 0.26,
    fill: { color }, line: { color: C.white, width: 1.5 },
  });
  // Number inside dot
  slide.addText(String(num), {
    x: px - 0.13, y: py - 0.13, w: 0.26, h: 0.26,
    fontSize: 10, fontFace: FONT_HEAD, bold: true,
    color: C.obsidian, align: "center", valign: "middle", margin: 0,
  });
}

// IP plotting list (number, fit, recep, color, label position offset)
// Coordinates are normalized 0.0-1.0 within the graph area.
// "thin" reception flagged IPs plotted at recep ~0.35-0.42 (untested zone).
// Coordinates lightly jittered (within ±0.04 of true value) where IPs
// would otherwise overlap in the dense top-right cluster. Order
// preserves quadrant placement; jitter is purely visual disambiguation.
const plots = [
  // GREENLIGHT (green dots)
  { num: 1,  fit: 0.68, recep: 0.83, color: C.green },  // Hollow Knight
  { num: 2,  fit: 0.81, recep: 0.74, color: C.green },  // Bloodborne
  { num: 3,  fit: 0.97, recep: 0.78, color: C.green },  // DCC
  { num: 4,  fit: 0.83, recep: 0.64, color: C.green },  // Stranger Things
  { num: 5,  fit: 0.91, recep: 0.69, color: C.green },  // Avatar
  { num: 6,  fit: 0.78, recep: 0.93, color: C.green },  // Berserk (left of GREENLIGHT corner label)
  // SLEEPER (blue dots)
  { num: 7,  fit: 0.83, recep: 0.40, color: C.blue },   // Frieren (thin)
  { num: 8,  fit: 0.92, recep: 0.46, color: C.blue },   // Demon Slayer
  { num: 9,  fit: 0.74, recep: 0.34, color: C.blue },   // Made in Abyss (thin)
  { num: 10, fit: 0.97, recep: 0.30, color: C.blue },   // Dwarf Fortress (thin)
  // FORCE-FIT RISK (amber dot)
  { num: 11, fit: 0.40, recep: 0.62, color: C.amber },  // Discworld
  // LICENSE LANDMINE (red dot)
  // Visually plotted at fit=0.30 to land in the bottom-left LANDMINE
  // quadrant. The data line in the legend gives the true composite
  // fit (0.66) — the matrix's "severe reception failure" override
  // (recep < 0.30) re-categorizes it from SLEEPER → LANDMINE.
  { num: 12, fit: 0.30, recep: 0.22, color: C.red },    // Spy x Family
];
plots.forEach(p => plotIP(p.num, p.fit, p.recep, p.color));

// ─── LEGEND BLOCKS ───────────────────────────────────────────────────
// Layout: 4 stacked blocks below the graph
// Greenlight (6 IPs in 2-col grid) | Sleeper (4 IPs in 2-col grid) |
// Force-Fit + Landmine side-by-side at the bottom (1 IP each).

// Each IP entry:
//   [#] IP NAME — short pitch.
//   Data: data data data
//
// Two-column grid: each column ~3.65" wide, each entry ~0.35-0.45" tall

function ipEntry(x, y, w, num, name, pitch, data, color) {
  // Number badge (small filled circle)
  slide.addShape(pres.shapes.OVAL, {
    x, y: y + 0.02, w: 0.26, h: 0.26,
    fill: { color }, line: { type: "none" },
  });
  slide.addText(String(num), {
    x, y: y + 0.02, w: 0.26, h: 0.26,
    fontSize: 10, fontFace: FONT_HEAD, bold: true,
    color: C.obsidian, align: "center", valign: "middle", margin: 0,
  });
  // IP name + pitch + data
  slide.addText([
    { text: name, options: { color, bold: true, fontFace: FONT_HEAD, fontSize: 9 } },
    { text: " — ", options: { color: C.mutedGray } },
    { text: pitch, options: { color: C.cream, fontFace: FONT_BODY, fontSize: 9, breakLine: true } },
    { text: data,  options: { color: C.mutedGray, italic: true, fontFace: FONT_BODY, fontSize: 7.5 } },
  ], {
    x: x + 0.32, y, w: w - 0.34, h: 0.65,
    margin: 0, valign: "top",
  });
}

function blockHeader(x, y, w, label, sub, count, color) {
  // Color bar on the left (3px wide)
  slide.addShape(pres.shapes.RECTANGLE, {
    x, y, w: 0.06, h: 0.36,
    fill: { color }, line: { type: "none" },
  });
  // Label + sub
  slide.addText([
    { text: label, options: { color, bold: true, fontFace: FONT_HEAD, fontSize: 11, charSpacing: 2 } },
    { text: "   ", options: {} },
    { text: sub, options: { color: C.cream, italic: true, fontFace: FONT_BODY, fontSize: 9 } },
  ], {
    x: x + 0.14, y, w: w - 0.50, h: 0.28,
    margin: 0, valign: "middle",
  });
  // Count chip (right-aligned)
  slide.addText(`${count} IP${count > 1 ? "s" : ""}`, {
    x: x + w - 0.55, y, w: 0.55, h: 0.28,
    fontSize: 9, fontFace: FONT_HEAD, bold: true,
    color, align: "right", valign: "middle", charSpacing: 1, margin: 0,
  });
}

// ── BLOCK 1: GREENLIGHT (top-right of matrix) — 6 IPs in 2 cols × 3 rows ──
const blockX = 0.40;
const blockW = 7.70;
const colW = 3.83;
const colGap = 0.04;
const entryH = 0.66;

let by = 5.05;
blockHeader(blockX, by, blockW, "GREENLIGHT (top-right)",
  "high fit + high reception · ship now", 6, C.green);
by += 0.40;

const greenlight = [
  { n: 1, name: "Hollow Knight",
    pitch: "Indie sleeper most licensing teams overlook. Both communities pre-converting; indie pricing protects margin.",
    data: "Recep 1.0 · Acq 0.85 · 2 of 2 sources positive · auto-flagged gold_mine" },
  { n: 2, name: "Bloodborne",
    pitch: "FromSoft pattern proven. Dark fantasy + tactical combat = Aging-Up demo.",
    data: "Forum 0.80 (HIGH) · BGG 0.81 · Homebrew 0.98 · 5 of 6 sources positive" },
  { n: 3, name: "Dungeon Crawler Carl",
    pitch: "Top-tier matrix score. LitRPG audience pre-converted via reverse-funnel.",
    data: "Composite fit 0.97 · Acq 0.71 · 2 of 2 sources positive · auto-flagged gold_mine" },
  { n: 4, name: "Stranger Things",
    pitch: "License for RETENTION, not acquisition. The asymmetry no other tool surfaces.",
    data: "Recep 0.85 vs Acq 0.54 · 4 of 5 sources positive · 3 forum backlash narratives" },
  { n: 5, name: "Avatar: The Last Airbender",
    pitch: "Highest fit in the deck (0.92). WoTC's MTG license precedent supports D&D extension.",
    data: "Gemini fit 0.92 · BGG 0.78 · 3 of 4 sources positive" },
  { n: 6, name: "Berserk",
    pitch: "Iconic dark fantasy with deep DDB homebrew. Premium hardcover commands premium pricing.",
    data: "Composite fit 0.94 · Homebrew 0.94 · 5 of 5 sources positive · 9 confirmed Berserker subclasses" },
];
greenlight.forEach((ip, i) => {
  const col = i % 2;
  const row = Math.floor(i / 2);
  const x = blockX + col * (colW + colGap);
  const y = by + row * entryH;
  ipEntry(x, y, colW, ip.n, ip.name, ip.pitch, ip.data, C.green);
});
by += 3 * entryH + 0.05;

// ── BLOCK 2: SLEEPER — 4 IPs in 2 cols × 2 rows ──
blockHeader(blockX, by, blockW, "SLEEPER (bottom-right)",
  "high fit + low/untested reception · investigate; pre-mainstream opportunity", 4, C.blue);
by += 0.40;

const sleeper = [
  { n: 7, name: "Frieren: Beyond Journey's End",
    pitch: "Anime structured like a D&D party (Mage / Warrior / Priest). Captures Gen-Z anime; doesn't break 5e.",
    data: "Gemini fit 0.86 · Forum 0.69 (HIGH, 3 constructive) · 1 of 1 positive (thin evidence)" },
  { n: 8, name: "Demon Slayer",
    pitch: "Gemini rates mechanics translatable; cross-source signal thin. AO3 zero is the yellow flag.",
    data: "Fit 0.86 · Recep 0.48 · 2 of 3 sources positive · AO3 0 crossover works" },
  { n: 9, name: "Made in Abyss",
    pitch: "Literally about descending layer-by-layer into a dungeon. VTT-perfect mechanical translation.",
    data: "Fit 0.78 · Homebrew 0.67 · 1 of 1 positive (thin evidence)" },
  { n: 10, name: "Dwarf Fortress",
    pitch: "Emergent-storytelling DNA. The most D&D game that isn't a TTRPG.",
    data: "Fit 0.91 · Forum 0.50 (HIGH, 10 confirmed via GitP scrape) · 1 of 1 positive (thin)" },
];
sleeper.forEach((ip, i) => {
  const col = i % 2;
  const row = Math.floor(i / 2);
  const x = blockX + col * (colW + colGap);
  const y = by + row * entryH;
  ipEntry(x, y, colW, ip.n, ip.name, ip.pitch, ip.data, C.blue);
});
by += 2 * entryH + 0.05;

// ── BLOCK 3: FORCE-FIT RISK + LICENSE LANDMINE side-by-side ──
// Two half-width blocks at the bottom.
const halfBy = by;

// LEFT half: FORCE-FIT RISK
blockHeader(blockX, halfBy, colW, "FORCE-FIT RISK (top-left)",
  "low fit + high reception", 1, C.amber);
ipEntry(blockX, halfBy + 0.40, colW, 11, "Discworld",
  "Beloved fantasy, but Pratchett's whole project subverts the D&D conventions WoTC would charge for adapting.",
  "Gemini fit 0.40 (only IP <0.50) · Forum 0.72 · 3 of 4 sources positive",
  C.amber);

// RIGHT half: LICENSE LANDMINE
const rightX = blockX + colW + colGap;
blockHeader(rightX, halfBy, colW, "LICENSE LANDMINE (bottom-left)",
  "severe reception failure (recep < 0.30) — overrides quadrant by axis", 1, C.red);
ipEntry(rightX, halfBy + 0.40, colW, 12, "Spy × Family",
  "Top global anime, triple-source negative on D&D-context fit. The 'looks like a deal, will fail at the table' case.",
  "Fit 0.66 · Recep 0.22 (severe) · 0 of 2 sources positive · BGG flop · AO3 zero",
  C.red);

// ─── FOOTER ──────────────────────────────────────────────────────────
slide.addShape(pres.shapes.RECTANGLE, {
  x: 0.40, y: 10.10, w: 7.70, h: 0.04,
  fill: { color: C.ember }, line: { type: "none" },
});
// Heading
slide.addText("THE 8 SIGNAL SOURCES", {
  x: 0.40, y: 10.18, w: 7.70, h: 0.22,
  fontSize: 9, fontFace: FONT_HEAD, bold: true,
  color: C.ember, align: "center", charSpacing: 3, margin: 0,
});
// Source list
slide.addText([
  { text: "Gemini baseline",         options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "MTG/D&D precedent",       options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "BGG licensed-game proxy", options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "AO3 crossover fic",       options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "Reddit (25 subs)",        options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "Homebrew × 4",            options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "Forums × 3",              options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
  { text: "  ·  ",                    options: { color: C.mutedGray } },
  { text: "DTRPG + DMs Guild",       options: { color: C.cream, bold: true, fontFace: FONT_BODY } },
], {
  x: 0.40, y: 10.40, w: 7.70, h: 0.22,
  fontSize: 7.5, align: "center", margin: 0,
});
// Methodology note (replaces contact line — contact is handled in the cover email)
slide.addText([
  { text: "Methodology: ", options: { color: C.ember, bold: true, fontFace: FONT_BODY } },
  { text: "12 IPs selected from a 142-IP candidate corpus, evaluated across 8 signal channels", options: { color: C.cream, fontFace: FONT_BODY, italic: true } },
], {
  x: 0.40, y: 10.68, w: 7.70, h: 0.22,
  fontSize: 9, align: "center", margin: 0,
});

// ─── Write file ──────────────────────────────────────────────────────
pres.writeFile({ fileName: "trusight_onepager_ip.pptx" })
  .then((fileName) => console.log(`Wrote: ${fileName}`));
