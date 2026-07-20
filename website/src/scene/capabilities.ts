// Decide whether to render the live WebGL hero or a static poster. Respects
// prefers-reduced-motion and falls back when WebGL is unavailable or on small
// screens (where the animation costs more than it adds).
export function shouldRenderScene(): boolean {
  if (typeof window === "undefined") return false;
  // ?still forces the static poster (used for captures/print and by anything
  // that wants the animation off without changing OS settings).
  if (window.location.search.includes("still")) return false;
  const reduced = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
  if (reduced) return false;
  if (window.innerWidth < 768) return false;
  try {
    const canvas = document.createElement("canvas");
    const gl = canvas.getContext("webgl2") || canvas.getContext("webgl");
    return !!gl;
  } catch {
    return false;
  }
}
