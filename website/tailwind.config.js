/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        abyss: "#060A12",
        surface: "#0B1220",
        aqua: "#2EE6D6",
        violet: "#8B7BFF",
        ink: "#E6EDF0",
        muted: "#93A4B5",
        faint: "#5C6B7A",
      },
      fontFamily: {
        display: ['"Space Grotesk Variable"', "system-ui", "sans-serif"],
        sans: ['"Geist Variable"', "system-ui", "sans-serif"],
        mono: ['"Geist Mono Variable"', "ui-monospace", "monospace"],
      },
    },
  },
  plugins: [],
}
