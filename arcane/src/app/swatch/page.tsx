/*
 * Arcane Analytics — Step 1 token-verification harness (moved to /swatch in Step 2).
 *
 * This page proves the Obsidian & Ember palette and three fonts are wired correctly.
 * Rarity tier swatches updated to the metal ladder (copper/silver/gold/platinum/mithral)
 * as reconciled in §9.16 of FRONTEND_DESIGN_SPEC.md.
 *
 * Not a real page — kept as a permanent reference for palette QA.
 */

type Swatch = {
  token: string;
  hex: string;
  role: string;
  className: string;
  onDark?: boolean;
};

const surfaces: Swatch[] = [
  { token: "obsidian",  hex: "#0B0D12", role: "background base",       className: "bg-obsidian" },
  { token: "onyx",      hex: "#141821", role: "card surface",           className: "bg-onyx" },
  { token: "iron",      hex: "#1C2230", role: "elevated surface",       className: "bg-iron" },
  { token: "bronze",    hex: "#3A2E1F", role: "resting border",         className: "bg-bronze" },
  { token: "ember",     hex: "#B8692A", role: "active / glowing border",className: "bg-ember" },
];

const textSwatches: Swatch[] = [
  { token: "parchment", hex: "#E8E3D5", role: "primary text",  className: "bg-parchment", onDark: true },
  { token: "ash",       hex: "#8A8578", role: "secondary text", className: "bg-ash" },
];

const data: Swatch[] = [
  { token: "ember-bright", hex: "#E87722", role: "data hot",  className: "bg-ember-bright" },
  { token: "arcane",       hex: "#5FC9E7", role: "data cold", className: "bg-arcane",    onDark: true },
  { token: "druid",        hex: "#6BAA75", role: "positive",  className: "bg-druid",     onDark: true },
  { token: "void",         hex: "#8B5CF6", role: "negative",  className: "bg-void" },
];

// Updated to the metal ladder per §9.16.
// copper/silver share no hues with druid/arcane — double-duty retired.
const tiers: Swatch[] = [
  { token: "rarity-copper",   hex: "#8C6239", role: "copper — exploratory (0–69%)",    className: "bg-rarity-copper" },
  { token: "rarity-silver",   hex: "#6B6B70", role: "silver — solid but cautious (70–79%)", className: "bg-rarity-silver" },
  { token: "rarity-gold",     hex: "#D4A94A", role: "gold — confident (80–89%)",       className: "bg-rarity-gold",     onDark: true },
  { token: "rarity-platinum", hex: "#D1D5DB", role: "platinum — high confidence (90–94%)", className: "bg-rarity-platinum", onDark: true },
  { token: "rarity-mithral",  hex: "#7AB8E0", role: "mithral — exceptional (95–99%)",  className: "bg-rarity-mithral",  onDark: true },
];

function SwatchGrid({ title, swatches }: { title: string; swatches: Swatch[] }) {
  return (
    <section className="space-y-4">
      <h2 className="font-display text-2xl font-semibold tracking-tight text-parchment">
        {title}
      </h2>
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {swatches.map((s) => (
          <div
            key={s.token}
            className="flex items-center gap-4 rounded-lg border border-bronze bg-onyx p-4"
          >
            <div
              className={`${s.className} h-14 w-14 shrink-0 rounded-md ring-1 ring-inset ring-black/30`}
              aria-hidden
            />
            <div className="min-w-0 flex-1">
              <div className="font-sans text-sm font-medium text-parchment">{s.token}</div>
              <div className="font-mono text-xs text-ash">{s.hex}</div>
              <div className="font-sans text-xs text-ash">{s.role}</div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

export default function SwatchPage() {
  return (
    <main className="mx-auto max-w-5xl px-6 py-16 space-y-16">
      <header className="space-y-4">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Step 1 of 16 · Skeleton · token verification
        </p>
        <h1 className="font-display text-5xl font-semibold tracking-tight text-parchment">
          Palette & Fonts
        </h1>
        <p className="font-sans text-lg text-ash max-w-2xl">
          Obsidian &amp; Ember palette verification. Rarity tier swatches updated to
          the metal ladder (§9.16) — copper through mithral.
          See <span className="font-mono text-xs text-ember">/test-card-chrome</span> for
          the CardChrome component harness.
        </p>
      </header>

      <SwatchGrid title="Surfaces & Borders" swatches={surfaces} />
      <SwatchGrid title="Text" swatches={textSwatches} />
      <SwatchGrid title="Data Accents" swatches={data} />
      <SwatchGrid title="Confidence Tiers (Metal Ladder)" swatches={tiers} />

      <section className="space-y-4">
        <h2 className="font-display text-2xl font-semibold tracking-tight text-parchment">
          Typography
        </h2>
        <div className="space-y-6 rounded-lg border border-bronze bg-onyx p-8">
          <div className="space-y-2">
            <p className="font-mono text-xs uppercase tracking-widest text-ash">
              Spectral · headers &amp; concept names
            </p>
            <p className="font-display text-3xl font-semibold text-parchment">
              The Paladin Opportunity
            </p>
            <p className="font-display text-base text-parchment/80">
              Spectral is a serif designed for on-screen reading. It carries the arcane warmth
              of WotC book typography even at small sizes.
            </p>
          </div>
          <div className="space-y-2">
            <p className="font-mono text-xs uppercase tracking-widest text-ash">
              Inter · body &amp; UI
            </p>
            <p className="font-sans text-base text-parchment">
              Inter is the boring, correct choice for body text. Used by Stripe, Linear, and Figma,
              it keeps the chrome precise while Spectral carries the soul.
            </p>
          </div>
          <div className="space-y-2">
            <p className="font-mono text-xs uppercase tracking-widest text-ash">
              JetBrains Mono · numbers &amp; data
            </p>
            <p className="font-mono text-base text-parchment">
              trend_score = 88.4 &nbsp; confidence = 0.92 &nbsp; lens = overview
            </p>
            <p className="font-mono text-sm text-ash">
              1234567890 &nbsp; {"{} [] () <>"} &nbsp; +/-*= &nbsp; 0xDEADBEEF
            </p>
          </div>
        </div>
      </section>

      <footer className="border-t border-bronze pt-6">
        <p className="font-mono text-xs text-ash">
          FRONTEND_DESIGN_SPEC.md · Step 1 verification ·{" "}
          <span className="text-ember-bright">palette &amp; fonts only</span>
        </p>
      </footer>
    </main>
  );
}
