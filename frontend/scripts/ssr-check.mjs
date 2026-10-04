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

// Every command that mutates Raft state (migrate / remove-replica / plan) is
// controller-only: it must be posted to the Raft leader's admin address, never
// to a follower (a follower refuses with 429 + err_id 1005 naming the
// controller; nothing is forwarded). The console resolves that address from
// the status table it already holds — these offline assertions pin that path,
// including the real race (the leader moved between the status read and the
// request): one status re-read, one retry, then a readable error.
const followerSeen = {
  raft: { state: 'Follower', leader: 'node-1' },
  peers: { 'node-1': { id: 'node-1', admin_addr: 'http://follower:8091' } },
  slots: {},
}
const afterMove = {
  raft: { state: 'Follower', leader: 'node-2' },
  peers: {
    'node-1': { id: 'node-1', admin_addr: 'http://follower:8091' },
    'node-2': { id: 'node-2', admin_addr: 'http://controller:8092' },
  },
  slots: {},
}
const notController = {
  response: {
    status: 429,
    data: {
      err_id: 1005,
      error: 'not the controller (Raft leader): the controller is node-2 at http://controller:8092; '
        + 'send this command directly to that admin address (this node does not forward it)',
    },
  },
}
assert('controllerAdminAddr finds the controller admin address',
  api.controllerAdminAddr(afterMove) === 'http://controller:8092')
{
  let msg = ''
  try { api.controllerAdminAddr({ raft: { leader: '' }, peers: {}, slots: {} }) } catch (e) { msg = e?.message ?? '' }
  assert('controllerAdminAddr refuses a table without a controller', /没有选出控制器/.test(msg))
}
assert('isNotControllerError recognizes the follower refusal',
  api.isNotControllerError(notController) === true && api.isNotControllerError(new Error('boom')) === false)
{
  const tries = []
  let refreshed = 0
  const out = await api.withController(
    async (addr) => { tries.push(addr); if (tries.length === 1) throw notController; return `ok ${addr}` },
    followerSeen,
    async () => { refreshed++; return afterMove },
  )
  assert('withController retries once against the new controller after a leader change',
    tries.length === 2 && tries[0] === 'http://follower:8091' && tries[1] === 'http://controller:8092' &&
    refreshed === 1 && out === 'ok http://controller:8092')
}
{
  let calls = 0
  let msg = ''
  try {
    await api.withController(async () => { calls++; throw notController }, followerSeen, async () => afterMove)
  } catch (e) { msg = e?.message ?? '' }
  assert('withController gives a readable error when the retry also refuses',
    calls === 2 && /控制器已切到 node-2，请重试/.test(msg))
}
{
  let refreshed = 0
  const out = await api.withController(async (addr) => `ok ${addr}`, afterMove, async () => { refreshed++; return afterMove })
  assert('withController adds no request while the cached table is current',
    out === 'ok http://controller:8092' && refreshed === 0)
}
{
  let calls = 0
  let msg = ''
  try {
    await api.withController(async () => { calls++; throw new Error('unknown target node "x"') }, afterMove, async () => afterMove)
  } catch (e) { msg = e?.message ?? '' }
  assert('withController passes non-controller errors straight through', calls === 1 && /unknown target node/.test(msg))
}

// Live probe: hand migrateSlot a deliberately unknown target, which the
// controller rejects before it touches any state; getting *that* answer proves
// the call reached the controller (a follower would answer "not the
// controller" instead), and it exercises the shared addressing helper end to
// end against a real cluster.
const probeBase = process.env.PUSHUPES_API_PROBE ?? 'http://127.0.0.1:9591'
try {
  const axios = (await import('axios')).default
  const st = (await axios.get(`${probeBase}/admin/cluster/status`, { timeout: 3000 })).data
  const ctlAddr = api.controllerAdminAddr(st)
  const anySlot = Number(Object.keys(st.slots ?? {})[0] ?? 0)
  try {
    await api.migrateSlot(anySlot, '__no_such_node__', st)
    assert('migrate is addressed to the controller', false)
  } catch (e) {
    const msg = e?.response?.data?.error ?? e?.message ?? String(e)
    assert('migrate is addressed to the controller', /unknown target node/.test(msg))
    console.log(`  (live migrate probe via ${ctlAddr} -> ${msg})`)
  }
} catch (e) {
  console.log(`  (live migrate probe skipped: ${e?.message ?? e})`)
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

// A replica whose local copy is queued for automatic cleanup after a migration
// renders WEAKENED (secondary text token + reduced opacity: theme-aware, no
// hardcoded colour) and says so, with the expected cleanup time; an ordinary
// replica keeps its normal tag, and nothing is weakened without a queue.
const { ReplicaTags, dropHint } = await vite.ssrLoadModule('/src/ReplicaTags.tsx')
const rt = (props) => renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(ReplicaTags, props)),
)
const NOW = 1_900_000_000_000
// The two migration shapes, pinned to the BACKEND marker (the console never
// infers "queued" from "was a migration source"):
//   - an IN-SET hand-over removes nobody, so the backend reports no drop for
//     the slot (dropping[s] === 0): no node is weakened, the set stands;
//   - an OUT-OF-SET hand-over takes the former source out of the replica set,
//     so it reports a deadline for it: that copy is weakened (and, being
//     surplus, it is rendered next to the set rather than inside it).
const inSetMoved = rt({ replicas: ['node-1', 'node-2'], leader: 'node-1', dropAt: {}, surplus: [], now: NOW })
assert('an in-set hand-over (no marker) greys nothing',
  !/opacity:0\.55/.test(inSetMoved) && /ant-tag-gold[^>]*>node-1</.test(inSetMoved) && /ant-tag[^>]*>node-2</.test(inSetMoved))
const outOfSetMoved = rt({
  replicas: ['node-1', 'node-2'], leader: 'node-2',
  dropAt: { 'node-3': NOW / 1000 + 30 }, surplus: ['node-3'], now: NOW,
})
assert('an out-of-set hand-over (marker on the removed copy) greys exactly that copy',
  /opacity:0\.55/.test(outOfSetMoved) && outOfSetMoved.includes('node-3') && !/opacity:0\.55<\/Tag>[\s\S]*>node-1</.test(outOfSetMoved))

const queued = rt({ replicas: ['node-1', 'node-2'], leader: 'node-1', dropAt: { 'node-2': NOW / 1000 + 600 }, now: NOW })
const unqueued = rt({ replicas: ['node-1', 'node-2'], leader: 'node-1', dropAt: {}, now: NOW })
assert('a queued replica renders weakened', /opacity:0\.55/.test(queued) && /ant-typography-secondary/.test(queued))
assert('the queued replica is the one weakened',
  queued.indexOf('node-2') > queued.indexOf('ant-typography-secondary') && queued.indexOf('ant-typography-secondary') > -1)
assert('the leader keeps its normal coloured tag', /ant-tag-gold[^>]*>node-1</.test(queued))
assert('no weakening without a queue', !/opacity:0\.55/.test(unqueued) && !/ant-typography-secondary/.test(unqueued))
assert('the unqueued replica keeps the ordinary tag', /ant-tag[^>]*>node-2</.test(unqueued))
const hint = dropHint('node-2', NOW / 1000 + 600, NOW)
assert('the cleanup hint names the node and the expected time',
  /迁移后待自动清理/.test(hint) && hint.includes('node-2') && /\d{2}:\d{2}:\d{2}/.test(hint) && /10 分/.test(hint))
assert('no hint for a replica that is not queued', dropHint('node-3', 0, NOW) === '')

// The node list marks a peer OFFLINE (中文「离线」+ weakened dashed card) when
// it has not announced a client address — the same online rule the rebalancer
// uses (OnlinePeerIDs). An online peer renders no 离线 tag.
const { PeerCard, isOnlinePeer } = await vite.ssrLoadModule('/src/PeerCard.tsx')
assert('isOnlinePeer: announced client addr is online', isOnlinePeer({ client_addr: 'http://h:8591' }) === true)
assert('isOnlinePeer: empty / missing client addr is offline',
  isOnlinePeer({ client_addr: '' }) === false && isOnlinePeer({ client_addr: undefined }) === false)
assert('isOnlinePeer: a marked-down peer is offline even with an announced addr',
  isOnlinePeer({ client_addr: 'http://h:8591', down: true }) === false)
const pc = (props) => renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(PeerCard, props)),
)
const offlineCard = pc({ peer: { id: 'node-2', peer_addr: 'http://h:8392', admin_addr: '', client_addr: '' }, current: false, slots: 5 })
assert('an offline peer is tagged 离线', offlineCard.includes('离线'))
assert('an offline card is weakened (dashed border)', /border-style:dashed/.test(offlineCard))
assert('an offline peer shows - for the missing admin addr', offlineCard.includes('>-<'))
const downCard = pc({ peer: { id: 'node-3', peer_addr: 'http://h:8393', admin_addr: 'http://h:8093', client_addr: 'http://h:8593', down: true }, current: false, slots: 0 })
assert('a marked-down peer with announced addresses is still tagged 离线',
  downCard.includes('离线') && downCard.includes('http://h:8593') && /border-style:dashed/.test(downCard))
const onlineCard = pc({ peer: { id: 'node-1', peer_addr: 'http://h:8391', admin_addr: 'http://h:8091', client_addr: 'http://h:8591' }, current: true, slots: 561 })
assert('an online peer renders no 离线 tag', !onlineCard.includes('离线') && onlineCard.includes('当前') && !/border-style:dashed/.test(onlineCard))

let failed = 0
for (const [name, ok] of checks) {
  if (!ok) failed++
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}`)
}
console.log(`render length light=${light.length} dark=${dark.length}`)
await vite.close()
process.exit(failed ? 1 : 0)
