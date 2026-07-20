// Theme control. Dark is the default; the choice persists in localStorage and
// is applied to the root element as data-theme (index.css keys off it).
export type Theme = "dark" | "light";

const KEY = "lakesense-theme";

export function getTheme(): Theme {
  const saved = localStorage.getItem(KEY);
  return saved === "light" ? "light" : "dark";
}

export function applyTheme(t: Theme) {
  document.documentElement.setAttribute("data-theme", t);
  localStorage.setItem(KEY, t);
}
