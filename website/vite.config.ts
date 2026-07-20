import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// base is "/" for local/root hosting and for a custom domain. For a GitHub
// Pages *project* site (mohdimran043.github.io/lakesense/), the deploy workflow
// sets VITE_BASE=/lakesense/. All public-asset references use
// import.meta.env.BASE_URL so they resolve correctly under either.
export default defineConfig({
  base: process.env.VITE_BASE || "/",
  plugins: [react()],
});
