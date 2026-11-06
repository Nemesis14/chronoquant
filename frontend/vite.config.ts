import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// API + built SPA: 127.0.0.1:8600. Proxy WITHOUT rewrite (native /api prefix).
const API = 'http://127.0.0.1:8600'

export default defineConfig({
  plugins: [react()],
  build: { chunkSizeWarningLimit: 5000 },
  server: {
    port: 5173,
    proxy: {
      '/api': { target: API, changeOrigin: true },
      '/mcp': { target: API, changeOrigin: true },
    },
  },
})
