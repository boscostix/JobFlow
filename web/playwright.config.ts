import { defineConfig } from '@playwright/test'

const liveURL = process.env.JOBFLOW_E2E_URL
const localURL = 'http://127.0.0.1:4173'

export default defineConfig({
  testDir: './tests',
  use: { baseURL: liveURL || localURL, browserName: 'chromium' },
  webServer: liveURL ? undefined : {
    command: 'npm run dev -- --host 127.0.0.1 --port 4173 --strictPort',
    url: localURL,
  },
})
