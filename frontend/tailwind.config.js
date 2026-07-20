/** @type {import('tailwindcss').Config} */
// LakeSense "abyssal depth-sounder" theme. Colors are CSS variables (defined in
// index.css) so the dark default and light theme swap via [data-theme] without
// touching component classes.
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "rgb(var(--bg) / <alpha-value>)",
        surface: "rgb(var(--surface) / <alpha-value>)",
        raised: "rgb(var(--raised) / <alpha-value>)",
        line: "rgb(var(--line) / <alpha-value>)",
        text: "rgb(var(--text) / <alpha-value>)",
        muted: "rgb(var(--muted) / <alpha-value>)",
        faint: "rgb(var(--faint) / <alpha-value>)",
        signal: "rgb(var(--signal) / <alpha-value>)",
        "signal-dim": "rgb(var(--signal-dim) / <alpha-value>)",
        warn: "rgb(var(--warn) / <alpha-value>)",
        danger: "rgb(var(--danger) / <alpha-value>)",
        info: "rgb(var(--info) / <alpha-value>)",
      },
      fontFamily: {
        display: ['"Space Grotesk Variable"', "system-ui", "sans-serif"],
        sans: ['"Geist Variable"', "system-ui", "sans-serif"],
        mono: ['"Geist Mono Variable"', "ui-monospace", "monospace"],
      },
      borderRadius: {
        card: "12px",
        control: "8px",
      },
      boxShadow: {
        // Depth: a faint waterline highlight on top + soft ambient below.
        raised: "inset 0 1px 0 0 rgb(255 255 255 / 0.04), 0 1px 2px 0 rgb(0 0 0 / 0.4)",
        glow: "0 0 0 1px rgb(var(--signal) / 0.4), 0 0 20px -4px rgb(var(--signal) / 0.35)",
      },
      keyframes: {
        ping: { "75%, 100%": { transform: "scale(2)", opacity: "0" } },
        rise: { from: { opacity: "0", transform: "translateY(6px)" }, to: { opacity: "1", transform: "translateY(0)" } },
        shimmer: { "100%": { transform: "translateX(100%)" } },
      },
      animation: {
        rise: "rise 0.24s ease-out both",
        "ping-slow": "ping 2.2s cubic-bezier(0,0,0.2,1) infinite",
      },
    },
  },
  plugins: [],
};
