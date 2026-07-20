import { lazy, Suspense, useEffect, useState } from "react";
import { shouldRenderScene } from "./scene/capabilities";
import { Architecture, FAQ, Footer, PaywallBuster, Pricing, Problem, Showcase, Sources } from "./sections";

// Three.js is code-split so the initial bundle stays light; the canvas loads
// after first paint, over the static poster.
const Hero3D = lazy(() => import("./scene/Hero3D").then((m) => ({ default: m.Hero3D })));

const REPO = "https://github.com/lakesense/lakesense";

export default function App() {
  const [scene, setScene] = useState(false);
  useEffect(() => setScene(shouldRenderScene()), []);

  return (
    <div className="relative">
      {/* Hero */}
      <header className="relative flex min-h-[92vh] flex-col overflow-hidden">
        {/* Background: poster always, live scene on top when supported. */}
        <div className="hero-poster absolute inset-0 -z-10" aria-hidden="true">
          {scene && (
            <Suspense fallback={null}>
              <Hero3D />
            </Suspense>
          )}
          {/* Video slot: drop public/hero.mp4 to activate (see website/README.md).
              Left unbundled so we never ship someone else's footage. */}
          <div className="pointer-events-none absolute inset-0 bg-gradient-to-b from-transparent via-transparent to-abyss" />
        </div>

        <nav className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-6">
          <img src="/wordmark.svg" alt="LakeSense" className="h-8" />
          <div className="flex items-center gap-5 text-sm text-muted">
            <a href="#wedge" className="hidden hover:text-ink sm:inline">Why free</a>
            <a href="#product" className="hidden hover:text-ink sm:inline">Product</a>
            <a href="#pricing" className="hidden hover:text-ink sm:inline">Pricing</a>
            <a href={REPO} className="rounded-full border border-white/15 px-3 py-1.5 hover:border-aqua/60 hover:text-aqua">
              GitHub
            </a>
          </div>
        </nav>

        <div className="mx-auto flex w-full max-w-6xl flex-1 flex-col justify-center px-6">
          <div className="max-w-3xl">
            <div className="mb-5 inline-flex items-center gap-2 rounded-full border border-aqua/30 bg-aqua/10 px-3 py-1 font-mono text-xs text-aqua">
              <span className="relative flex h-1.5 w-1.5">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-aqua opacity-75" />
                <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-aqua" />
              </span>
              open-core · Apache-2.0
            </div>
            <h1 className="glow-text font-display text-5xl font-semibold leading-[1.05] tracking-tight md:text-7xl">
              Your data pipelines,
              <br />
              <span className="text-aqua">finally self-aware.</span>
            </h1>
            <p className="mt-6 max-w-2xl text-lg text-muted md:text-xl">
              Replicate 25+ sources into open lakehouse formats with a Go-native engine that proves
              the data is correct, tells the right person when it isn't, and gives away free what
              Fivetran, Monte Carlo, and PagerDuty charge for.
            </p>
            <div className="mt-9 flex flex-wrap gap-4">
              <a
                href={`${REPO}#quickstart`}
                className="rounded-full bg-aqua px-6 py-3 font-medium text-abyss transition-transform hover:scale-[1.03]"
              >
                Get started
              </a>
              <a
                href={REPO}
                className="rounded-full border border-white/15 px-6 py-3 font-medium text-ink transition-colors hover:border-aqua/60 hover:text-aqua"
              >
                ★ Star on GitHub
              </a>
            </div>
          </div>
        </div>
      </header>

      <main>
        <Problem />
        <PaywallBuster />
        <Showcase />
        <Sources />
        <Architecture />
        <Pricing />
        <FAQ />
      </main>
      <Footer />
    </div>
  );
}
