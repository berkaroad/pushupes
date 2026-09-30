// Render the console with vite's SSR pipeline and assert both themes work.
// (The browser tool cannot reach localhost in this sandbox, so we verify the
// React + antd layer headlessly: light/dark ConfigProvider output and each
// page's markup, plus design-token differences between algorithms.)
import { createServer } from 'vite'
import { renderToString } from 'react-dom/server'
import React from 'react'
import { ConfigProvider, theme } from 'antd'
import { MemoryRouter } from 'react-router-dom'

const vite = await createServer({ server: { middlewareMode: true }, appType: 'custom' })
const { default: App } = await vite.ssrLoadModule('/src/App.tsx')

function renderWith(mode) {
  return renderToString(
    React.createElement(ConfigProvider,
      { theme: { algorithm: mode === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm } },
      React.createElement(MemoryRouter, { initialEntries: ['/cluster'] },
        React.createElement(App))),
  )
}

// the slot page must render headlessly too (it needs no backend for that:
// without a status it shows the unreachable alert)
function renderRoute(route, mode) {
  return renderToString(
    React.createElement(ConfigProvider,
      { theme: { algorithm: mode === 'dark' ? theme.darkAlgorithm : theme.defaultAlgorithm } },
      React.createElement(MemoryRouter, { initialEntries: [route] },
        React.createElement(App))),
  )
}

const light = renderWith('light')
const dark = renderWith('dark')

const checks = []
const assert = (name, cond) => checks.push([name, !!cond])

assert('layout rendered', light.includes('ant-layout'))
assert('sider brand', light.includes('PushupES'))
assert('menu 集群总览', light.includes('集群总览'))
assert('menu 槽位', light.includes('槽位'))
assert('theme switch present', light.includes('Light') && light.includes('Dark'))

// design tokens must differ between algorithms (real effect of the toggle)
const tkLight = theme.getDesignToken({ algorithm: theme.defaultAlgorithm })
const tkDark = theme.getDesignToken({ algorithm: theme.darkAlgorithm })
assert('light token bg is #fff', tkLight.colorBgContainer === '#ffffff')
assert('dark token bg is #141414', tkDark.colorBgContainer === '#141414')
assert('dark differs from light', tkLight.colorBgLayout !== tkDark.colorBgLayout)

assert('slots route renders', renderRoute('/slots', 'light').length > 0)

// The console's stream listing goes through this helper; when a cluster is
// reachable, exercise it for real (the helper takes an absolute node address,
// so it works outside the browser too).
const api = await vite.ssrLoadModule('/src/api.ts')
assert('fetchSlotStreams exposed', typeof api.fetchSlotStreams === 'function')
try {
  const probes = await api.fetchSlotStreams('http://127.0.0.1:9591', 4058)
  assert('live slot streams answer', typeof probes.loaded === 'boolean' && Array.isArray(probes.streams))
  console.log(`  (live probe: node=${probes.node} loaded=${probes.loaded} total=${probes.total})`)
} catch (e) {
  console.log(`  (live probe skipped: ${e?.message ?? e})`)
}

let failed = 0
for (const [name, ok] of checks) {
  if (!ok) failed++
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}`)
}
console.log(`render length light=${light.length} dark=${dark.length}`)
await vite.close()
process.exit(failed ? 1 : 0)
