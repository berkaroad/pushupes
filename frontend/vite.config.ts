import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Dev server proxies /api/* to a pushupes node; strip the prefix on the way.
// envPrefix additionally exposes PUSHUPES_ADMIN_ENDPOINTS (the console's
// multi-admin pool, comma-separated) to import.meta.env at build time —
// VITE_-prefixed vars alone would not reach the client bundle.
export default defineConfig({
  plugins: [react()],
  envPrefix: ['VITE_', 'PUSHUPES_'],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.PUSHUPES_API ?? 'http://127.0.0.1:8091',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ''),
      },
    },
  },
})
