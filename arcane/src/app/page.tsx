/*
 * Arcane Analytics — Step 1 skeleton verification page.
 *
 * This is NOT a real page. It exists only to prove the Obsidian & Ember
 * palette and the three fonts (Spectral / Inter / JetBrains Mono) are
 * wired correctly before we build CardChrome in Step 2. It will be
 * replaced by the real landing page ("State of the D&D Multiverse",
 * §3.1) later in the build order.
 *
 * Source of truth: FRONTEND_DESIGN_SPEC.md §4.1 (palette) and §4.2 (fonts).
 */

type Swatch = {
  token: string;
  hex: string;
  role: string;
  className: string;
  onDark?: boolean;
};

const surfaces: Swatch[] = [
  { token: "obsidian", hex: "#0B0D12", role: "background base", className: "bg-obsidian" },
  { token: "onyx", hex: "#141821", role: "card surface", className: "bg-onyx" },
  { token: "iron", hex: "#1C2230", role: "elevated surface", className: "bg-iron" },
  { token: "bronze", hex: "#3A2E1F", role: "resting border", className: "bg-bronze" },
  { token: "ember", hex: "#B8692A", role: "active / glowing border", className: "bg-ember" },
];

const text: Swatch[] = [
  { token: "parchment", hex: "#E8E3D5", role: "primary text", className: "bg-parchment", onDark: true },
  { token: "ash", hex: "#8A8578", role: "secondary text", className: "bg-ash" },
];

const data: Swatch[] = [
  { token: "ember-bright", hex: "#E87722", role: "data hot", className: "bg-ember-bright" },
  { token: "arcane", hex: "#5FC9E7", role: "data cold", className: "bg-arcane", onDark: true },
  { token: "druid", hex: "#6BAA75", role: "positive", className: "bg-druid", onDark: true },
  { token: "void", hex: "#8B5CF6", role: "negative", className: "bg-void" },
];

const rarity: Swatch[] = [
  { token: "rarity-common", hex: "#6B6B70", role: "common — low confidence", className: "bg-rarity-common" },
  { token: "rarity-uncommon", hex: "#6BAA75", role: "uncommon — medium", className: "bg-rarity-uncommon", onDark: true },
  { token: "rarity-rare", hex: "#5FC9E7", role: "rare — high", className: "bg-rarity-rare", onDark: true },
  { token: "rarity-mythic", hex: "#D4A94A", role: "mythic — exceptional", className: "bg-rarity-mythic", onDark: true },
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
              className={`${s.className} h-14 w-14 shrink-0 rounded-md ring-1 ring-inset ring-black/40`}
              aria-hidden
            />
            <div className="min-w-0 flex-1">
              <div className="font-sans text-sm font-medium text-parchment">
                {s.token}
              </div>
              <div className="font-mono text-xs text-ash">{s.hex}</div>
              <div className="font-sans text-xs text-ash">{s.role}</div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

export default function Home() {
  return (
    <main className="mx-auto max-w-5xl px-6 py-16 space-y-16">
      <header className="space-y-4">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Step 1 of 16 &middot; Skeleton
        </p>
        <h1 className="font-display text-5xl font-semibold tracking-tight text-parchment">
          Arcane Analytics
        </h1>
        <p className="font-sans text-lg text-ash max-w-2xl">
          A Fantasy Bloomberg Terminal for the D&amp;D multiverse. This page is a
          token-verification harness, not a real view. If the palette and fonts
          below look correct, the skeleton is wired up and we can build
          CardChrome next.
        </p>
      </header>

      <SwatchGrid title="Surfaces & Borders" swatches={surfaces} />
      <SwatchGrid title="Text" swatches={text} />
      <SwatchGrid title="Data Accents" swatches={data} />
      <SwatchGrid title="Rarity Tiers (Confidence)" swatches={rarity} />

      <section className="space-y-4">
        <h2 className="font-display text-2xl font-semibold tracking-tight text-parchment">
          Typography
        </h2>
        <div className="space-y-6 rounded-lg border border-bronze bg-onyx p-8">
          <div className="space-y-2">
            <p className="font-mono text-xs uppercase tracking-widest text-ash">
              Spectral &middot; headers &amp; concept names
            </p>
            <p className="font-display text-3xl font-semibold text-parchment">
              The Paladin Opportunity
            </p>
            <p className="font-display text-base text-parchment/80">
              Spectral is a serif designed for on-screen reading. It carries the
              arcane warmth of WotC book typography even at small sizes.
            </p>
          </div>

          <div className="space-y-2">
            <p className="font-mono text-xs uppercase tracking-widest text-ash">
              Inter &middot; body &amp; UI
            </p>
            <p className="font-sans text-base text-parchment">
              Inter is the boring, correct choice for body text. Used by Stripe,
              Linear, and Figma, it keeps the chrome precise while Spectral
              carries the soul.
            </p>
          </div>

          <div className="space-y-2">
            <p className="font-mono text-xs uppercase tracking-widest text-ash">
              JetBrains Mono &middot; numbers &amp; data
            </p>
            <p className="font-mono text-base text-parchment">
              trend_score = 88.4 &nbsp; confidence = 0.92 &nbsp; lens = overview
            </p>
            <p className="font-mono text-sm text-ash">
              1234567890 &nbsp; {"{ } [ ] ( ) < >"} &nbsp; +/-*= &nbsp; 0xDEADBEEF
            </p>
          </div>
        </div>
      </section>

      <section className="space-y-4">
        <h2 className="font-display text-2xl font-semibold tracking-tight text-parchment">
          Card Preview
        </h2>
        <p className="font-sans text-sm text-ash">
          A single preview of the uniform every real card will wear in Step 2
          (CardChrome). Bronze resting border, onyx surface, two icon slots
          reserved in the header, confidence hinted via a rarity accent.
        </p>
        <article className="rounded-xl border border-bronze bg-onyx shadow-[0_1px_0_0_rgba(232,227,213,0.04)_inset]">
          <header className="flex items-center justify-between border-b border-bronze px-5 py-3">
            <div>
              <p className="font-mono text-[10px] uppercase tracking-widest text-ash">
                Marketing &middot; Blue Ocean
              </p>
              <h3 className="font-display text-lg font-semibold text-parchment">
                Oath of Vengeance
              </h3>
            </div>
            <div className="flex items-center gap-2">
              <span className="inline-block h-4 w-4 rounded-sm bg-bronze" aria-hidden />
              <span className="inline-block h-4 w-4 rounded-sm bg-bronze" aria-hidden />
            </div>
          </header>
          <div className="space-y-3 px-5 py-4">
            <p className="font-sans text-sm text-parchment/90">
              Placeholder copy. In Step 3 this becomes a real Tremor chart
              backed by a BigQuery composite view.
            </p>
            <div className="flex items-center justify-between pt-2">
              <span className="font-mono text-xs text-ash">confidence 88%</span>
              <span className="inline-flex items-center rounded-full border border-rarity-rare/60 px-2 py-0.5 font-mono text-[10px] uppercase tracking-widest text-rarity-rare">
                rare
              </span>
            </div>
          </div>
        </article>
      </section>

      <footer className="border-t border-bronze pt-6">
        <p className="font-mono text-xs text-ash">
          FRONTEND_DESIGN_SPEC.md &middot; Step 1 verification &middot;{" "}
          <span className="text-ember-bright">skeleton only</span>
        </p>
      </footer>
    </main>
  );
}
