import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard talks to the control plane at same-origin /api (nginx proxies
// it in production). In dev, proxy to the local backend on :8080.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": { target: "http://localhost:8080", changeOrigin: true },
      "/healthz": "http://localhost:8080",
    },
  },
});
