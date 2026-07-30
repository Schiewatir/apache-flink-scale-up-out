import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Dev proxy forwards API + WebSocket to the backend so `npm run dev` works
// against a locally running backend on port 8088.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8088',
        changeOrigin: true,
        ws: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
});
