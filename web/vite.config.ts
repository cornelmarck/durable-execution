import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, /api is proxied to the Go server so the browser sees one origin (no CORS).
// Override the target with DURABLE_API, e.g. a kubectl port-forward.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": process.env.DURABLE_API ?? "http://localhost:8080",
    },
  },
});
