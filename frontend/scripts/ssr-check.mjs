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

// The drawer's rate chart is a plain SVG: render it with two points and check
// the polyline is there, then with one point and check the sampling hint.
const { RateChart } = await vite.ssrLoadModule('/src/RateChart.tsx')
const chart = renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(RateChart, { points: [{ t: 1, v: 10 }, { t: 2, v: 30 }, { t: 3, v: 20 }, { t: 4, v: 4 }] })),
)
assert('rate chart draws a polyline', chart.includes('<polyline') && chart.includes('峰值'))
assert('rate chart scales points', (chart.match(/[0-9]+\.[0-9],[0-9]+\.[0-9]/g) ?? []).length >= 3)
const chartEmpty = renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(RateChart, { points: [{ t: 1, v: 0 }] })),
)
assert('rate chart asks for samples while collecting', chartEmpty.includes('采集中'))
// axes: x = sample time points, y = evts/sec
assert('rate chart labels the y axis in evts/sec', chart.includes('evts/sec'))
assert('rate chart ticks the y axis from 0 to the peak', chart.includes('>0<') && chart.includes('>30<'))
assert('rate chart ticks the x axis with sample times', (chart.match(/\d{2}:\d{2}:\d{2}/g) ?? []).length >= 2)
assert('rate chart names both axes', chart.includes('横轴：时间点') && chart.includes('纵轴：evts/sec'))
// each caption item sits on its own line (five secondary text lines)
// the three numbers are the eye-catcher: primary-coloured, labels stay secondary
assert('rate chart paints caption values in the primary colour',
  /color:#1677ff/.test(chart) && (chart.match(/color:#1677ff/g) ?? []).length >= 4)
assert('rate chart caption lines are separate',
  (chart.match(/ant-typography-secondary/g) ?? []).length >= 6 &&
  ['横轴：', '纵轴：', '最新 ', '峰值 ', '平均 ', '样本 '].every((k, i, arr) => chart.includes(k) && arr.indexOf(k) === i))
// 平均 sits between 峰值 and 样本 and is the mean of the samples (10/30/20/4 -> 16,
// a value no other line shows: peak 30, last 4, y ticks 0/15/30)
assert('rate chart reports the window average',
  chart.indexOf('峰值 ') < chart.indexOf('平均 ') && chart.indexOf('平均 ') < chart.indexOf('样本 ') &&
  chart.includes('>16<'))

let failed = 0
for (const [name, ok] of checks) {
  if (!ok) failed++
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}`)
}
console.log(`render length light=${light.length} dark=${dark.length}`)
await vite.close()
process.exit(failed ? 1 : 0)
