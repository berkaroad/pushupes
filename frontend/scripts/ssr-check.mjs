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
assert('sider brand logo', light.includes('src="/logo.svg"') && light.includes('alt="PushupES"'))
assert('menu 集群', light.includes('集群') && !light.includes('集群总览'))
assert('menu 槽位', light.includes('槽位'))
assert('theme switch present', light.includes('Light') && light.includes('Dark'))

// design tokens must differ between algorithms (real effect of the toggle)
const tkLight = theme.getDesignToken({ algorithm: theme.defaultAlgorithm })
const tkDark = theme.getDesignToken({ algorithm: theme.darkAlgorithm })
assert('light token bg is #fff', tkLight.colorBgContainer === '#ffffff')
assert('dark token bg is #141414', tkDark.colorBgContainer === '#141414')
assert('dark differs from light', tkLight.colorBgLayout !== tkDark.colorBgLayout)

// Statistic values sit one step above the label: the stat cards are a
// label/value grid, and antd's default content font (fontSizeHeading3 = 24)
// dwarfs the 14px title. ThemeProvider — the wrapper main.tsx puts around the
// whole App — sets the component token once (16px: above the 14px title, far
// lighter than 24), so every Statistic (today: the cluster page's eight
// cards) reads 16 in BOTH themes; the algorithms do not override it, which is
// why the default-size assertion below holds for both token sets.
const { ThemeProvider } = await vite.ssrLoadModule('/src/theme.tsx')
assert('the theme pins the Statistic content size', String(ThemeProvider).includes('Statistic: { contentFontSize: 16 }'))
assert('the size it overrides is 24px by default',
  tkLight.fontSizeHeading3 === 24 && tkDark.fontSizeHeading3 === 24)

assert('slots route renders', renderRoute('/slots', 'light').length > 0)

// The console's stream listing goes through this helper; when a cluster is
// reachable, exercise it for real (the helper takes an absolute node address,
// so it works outside the browser too).
const api = await vite.ssrLoadModule('/src/api.ts')
assert('fetchSlotStreams exposed', typeof api.fetchSlotStreams === 'function')
// addClusterNode / removeClusterNode were the console's membership clients;
// membership is CLI-only now (POST / DELETE /admin/cluster/nodes, documented
// in DESIGN.md), so BOTH are gone — the page cannot re-wire an add or remove
// button to a live call without failing these.
assert('addClusterNode client is gone', api.addClusterNode === undefined)
assert('removeClusterNode client is gone', api.removeClusterNode === undefined)
try {
  const probes = await api.fetchSlotStreams('http://127.0.0.1:9591', 4058)
  assert('live slot streams answer', typeof probes.loaded === 'boolean' && Array.isArray(probes.streams))
  console.log(`  (live probe: node=${probes.node} loaded=${probes.loaded} total=${probes.total})`)
} catch (e) {
  console.log(`  (live probe skipped: ${e?.message ?? e})`)
}

// The cluster page's "存储大小" card reads status.storage_bytes (the sum of
// every slot's leader copy). The page's SSR render is the no-data branch (its
// status comes from a fetch SSR does not run), so this is checked in two
// halves: the WIRING in the compiled page module (property access survives the
// transform, so it pins that the card reads what the backend sends) and the
// CONTRACT against a reachable node (the field exists, is a number, and the
// completeness flag is a boolean).
const { default: ClusterPage } = await vite.ssrLoadModule('/src/pages/ClusterPage.tsx')
const pageSrc = String(ClusterPage)
assert('cluster page shows the storage card', pageSrc.includes('存储大小'))
// The replica policy tier moved into the replicated table with an admin
// change endpoint; the cluster page owns its control. These pin the wiring:
// the card carries the selector, the read-back shows the live factor AND the
// tier (not just the factor the old derived-only line showed), and a change
// goes through the controller-resolving setReplicaPolicy call.
assert('cluster page shows the replica policy selector', pageSrc.includes('副本策略'))
assert('cluster page reads replica_policy back', pageSrc.includes('replica_policy'))
assert('cluster page reads replica_factor back', pageSrc.includes('replica_factor'))
assert('policy change routes through setReplicaPolicy', pageSrc.includes('setReplicaPolicy'))
// The policy card is display-first: 修改 enters the edit state, the Select
// holds a DRAFT (nothing fires on selection), 保存 confirms, 取消 returns to
// the pre-edit display — all three controls must exist as separate steps.
assert('policy card has the edit entry button', pageSrc.includes('startPolicyEdit'))
assert('policy card has save and cancel', pageSrc.includes('savePolicy') && pageSrc.includes('cancelPolicyEdit'))
assert('policy edit holds a draft state', pageSrc.includes('policyDraft'))
// The Raft-state card was removed from the stat row: the leader/commit line
// under the node cards already carries what an operator needs, and the state
// was the only thing that card said. Pinned so it cannot come back by accident.
assert('the Raft-state card is gone', !pageSrc.includes('Raft 状态'))
assert('storage card reads storage_bytes', pageSrc.includes('storage_bytes'))
assert('storage card flags a lower bound', pageSrc.includes('storage_bytes_complete'))
// The stat row's slot card merged the old 迁移中槽位 into 槽位（迁移中 / 总数）
// ("1 / 1680") — that card is now the 存储数量 card: the records stored
// cluster-wide, folded SERVER-side by /admin/cluster/status storage_count
// (the leader copies' seq ledgers, sampled with storage_bytes in one fan-out;
// _complete=false renders it as a lower bound). The two write-speed cards
// stay on their own row.
assert('storage-count card replaced the slot card',
  pageSrc.includes('"存储数量"') && pageSrc.includes('storage_count')
  && !pageSrc.includes('槽位（迁移中 / 总数）')
  && !pageSrc.includes('"迁移中槽位"') && !pageSrc.includes('"槽位总数"'))
// The node cards say 控制节点 / 数据节点 — the （在线 / 总数） suffix is gone,
// the value line ("2 / 3") carries the counts.
assert('control and data node cards by plane',
  pageSrc.includes('"控制节点"') && pageSrc.includes('"数据节点"')
  && !pageSrc.includes('（在线 / 总数）'))
assert('the two cards count voters and learners',
  pageSrc.includes('role === "voter"') && pageSrc.includes('role === "learner"'))
assert('the summary row is four span-6 cards', (pageSrc.match(/span: 6/g) ?? []).length === 4)
// The node directory splits into two blocks: 控制节点 on top (voters as
// plain cards — no chart, no flow control — the answering node keeps 当前
// here) and 数据节点 below (learners as full cards, no 当前). The density
// follows the menu rail (RailContext): data 2 per row and 1 collapsed,
// control 3 per row and 2 collapsed; the bare span-12s left are the two
// speed cards. The merged 节点 title is gone.
assert('the plain node block is gone', !pageSrc.includes('title: "节点"'))
assert('the node blocks carry the split grids',
  pageSrc.includes('RailContext')
  && (pageSrc.match(/span: railCollapsed \? 24 : 12/g) ?? []).length === 1
  && (pageSrc.match(/span: railCollapsed \? 12 : 8/g) ?? []).length === 1
  && (pageSrc.match(/span: 8/g) ?? []).length === 0 && (pageSrc.match(/span: 12/g) ?? []).length === 2
  && pageSrc.includes('plain'))
assert('当前 rides the control block only',
  (pageSrc.match(/current: p\.id === status\.node/g) ?? []).length === 1)
// Membership is CLI-only in BOTH directions: no add button, no add modal, no
// 移除 button anywhere on the cluster page — the console reads the fleet, the
// operator changes it.
assert('the console offers no membership UI',
  !pageSrc.includes('添加节点') && !pageSrc.includes('移除')
  && !pageSrc.includes('doAdd') && !pageSrc.includes('setAdding')
  && !pageSrc.includes('askRemove') && !pageSrc.includes('removingId'))
// The two rate cards are CLIENT-only now (rebalance/migration traffic moved
// to the charts' dashed total lines), and the titles say so. The old bare
// title must be gone, not merely overlapped by the new substring.
assert('client write-speed is two cards, named for the client path',
  pageSrc.includes('title: "客户端写入速度（数量）"')
  && pageSrc.includes('title: "客户端写入速度（大小）"')
  && !pageSrc.includes('title: "写入速度（数量）"')
  && !pageSrc.includes('写入速度（数量 / 大小）'))
// 存储数量 moved server-side: the cluster status answers storage_count (the
// leader-copy seq fold shared with storage_bytes), so the page must NOT carry
// its own per-node leader fold any more — a re-introduced client-side fold
// would be a second, divergent source for the same card.
assert('storage count reads the cluster-side fold (no client-side leader fold)',
  pageSrc.includes('storage_count') && !pageSrc.includes('leaderWriteTotals'))
// The 同步状态 card rides the same 2s writes poll: the page reads each node's
// synced_slots pair and names the three states (pinning the compiled forms
// of both the state strings and the remaining-slots template).
assert('the sync-state card is wired to the writes poll',
  pageSrc.includes('同步状态') && pageSrc.includes('synced_slots'))
assert('the sync state renders 同步中 / 构建中（剩余 …） / 待就位（…）',
  pageSrc.includes('同步中') && pageSrc.includes('构建中（剩余 ')
  && pageSrc.includes('待就位（') && pageSrc.includes('个槽位待补齐副本集')
  && pageSrc.includes('pending_seats'))
// The same gauge, per card: each data node's own led − synced (from its
// synced_slots poll) drives the orange 同步中 N tag. The wiring reads the
// pair with the conservative floor and hands it to exactly ONE of the two
// PeerCard call sites — the data-plane block; control cards never take it.
assert('the per-card syncing count is derived from each node\'s synced_slots',
  pageSrc.includes('Math.max(0, pair[0] - pair[1])') && pageSrc.includes('setSyncingByNode'))
assert('only the data-plane cards are handed the syncing count',
  pageSrc.split('syncing: syncingByNode').length - 1 === 1)
// The fenced local copies ride the same poll (the endpoint grew a
// pending_slots field and was renamed /admin/writes -> /admin/status):
// the page reads the array per node and hands it to exactly ONE PeerCard
// call site — the data-plane block. The old endpoint name is pinned gone.
assert('the page reads pending_slots off the renamed /admin/status poll',
  pageSrc.includes('pending_slots') && pageSrc.includes('setFencedByNode')
  && !pageSrc.includes('admin/writes'))
assert('only the data-plane cards are handed the fenced list',
  pageSrc.split('fenced: fencedByNode').length - 1 === 1)
// One refresh cadence rules this page (自动刷新 2s): the status poll AND the
// /admin/status poll behind the value cards and the node charts both run on
// AUTO_REFRESH_MS, and the writes interval is gated by the same switch —
// off means nothing refreshes. Pinned against the old 5s constant too.
assert('both pollers share the 2s auto-refresh cadence and the switch gates them',
  pageSrc.includes('自动刷新 2s') && !pageSrc.includes('自动刷新 5s')
  && pageSrc.includes('setInterval(refresh, AUTO_REFRESH_MS)')
  && pageSrc.includes('auto ? setInterval(tick, AUTO_REFRESH_MS) : null')
  && !pageSrc.includes('WRITES_POLL_MS') && !pageSrc.includes('5000'))
assert('write-speed polls the light counters endpoint', pageSrc.includes('fetchNodeStatus') && !pageSrc.includes('fetchNodeWrites'))
assert('write-speed reads the byte counters', pageSrc.includes('total_write_bytes'))
// The 槽位健康 card: the counted slotcheck verdict (operator call 2026-10-24:
// labels Chinese, behind amber, one-minute re-trigger budget, NO per-slot
// drill-down — detail stays in the CLI). The result rides the existing status
// poll and the trigger is event-driven, so the page must still carry exactly
// its two AUTO_REFRESH_MS intervals and nothing else. SSR text interpolation
// is comment-split, so count phrases are matched across the seam, not as
// literals.
const { SlotHealthCard } = await vite.ssrLoadModule('/src/SlotHealthCard.tsx')
const sh = (health) => renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(SlotHealthCard, { health, onRecheck: () => {} })),
)
const AT = '2026-10-24T07:41:00Z'
const healthOk = sh({ state: 'done', compared: 1024, diverged: 0, behind: 0, not_loaded: 0, unreachable: 0, scanned_at: AT })
assert('an all-zero round collapses to a green 健康 check and hides every kind line',
  healthOk.includes('槽位健康') && /健康<\/div/.test(healthOk) && /anticon-check-circle/.test(healthOk)
  && !/anticon-close-circle/.test(healthOk) && /1024<!-- --> 槽已对比/.test(healthOk)
  && !/分叉|落后|未加载|不可达/.test(healthOk))
const healthD82 = sh({ state: 'done', compared: 1024, diverged: 82, behind: 0, not_loaded: 0, unreachable: 0, scanned_at: AT })
assert('a diverged round shows only its crossed red line, zero kinds absent',
  /分叉<!-- --> <!-- -->82/.test(healthD82) && healthD82.includes('#ff4d4f')
  && /anticon-close-circle/.test(healthD82) && !/anticon-check-circle/.test(healthD82)
  && !/落后|未加载|不可达/.test(healthD82))
const healthMixed = sh({ state: 'done', compared: 1024, diverged: 1, behind: 3, not_loaded: 0, unreachable: 2, scanned_at: AT })
assert('behind keeps the amber while the damaging kinds keep the red',
  healthMixed.includes('#faad14') && healthMixed.includes('#ff4d4f') && /落后<!-- --> <!-- -->3/.test(healthMixed))
const healthScan = sh({ state: 'scanning' })
assert('a first scan in flight says 检测中', healthScan.includes('检测中') && /anticon-loading/.test(healthScan))
const healthErr = sh({ state: 'error', error: 'dial tcp: no node answered', compared: 1024, diverged: 2, behind: 0, not_loaded: 0, unreachable: 0, scanned_at: AT })
assert('a failed round keeps the last numbers next to its error',
  /分叉<!-- --> <!-- -->2/.test(healthErr) && healthErr.includes('检测失败')
  && healthErr.includes('dial tcp: no node answered'))
assert('the health card rides the status poll; the page adds no new clock',
  pageSrc.includes('slot_health') && pageSrc.includes('triggerHealthScan')
  && pageSrc.includes('HEALTH_MAX_AGE_MS') && (pageSrc.match(/setInterval/g) ?? []).length === 2)
const { humanBytes, humanBytesPerSec, utcStamp } = await vite.ssrLoadModule('/src/format.ts')
assert('byte formatter renders sizes', humanBytes(0) === '0 B' && humanBytes(1536) === '1.5 KiB' && humanBytes(70496329) === '67.2 MiB')
assert('byte formatter renders rates', humanBytesPerSec(0) === '0 B/s' && humanBytesPerSec(1536) === '1.5 KiB/s')
// The stream listing's time column must read the same in every timezone: it
// renders UTC, and a missing stamp is a dash, never a 1970 date.
assert('UTC stamp renders unix seconds as UTC',
  utcStamp(1700000000) === '2023-11-14 22:13:20' && utcStamp(0) === '-' && utcStamp(undefined) === '-')
const { default: SlotsPage } = await vite.ssrLoadModule('/src/pages/SlotsPage.tsx')
const slotsSrc = String(SlotsPage)
assert('the slot drawer lists each stream\'s latest UTC time',
  slotsSrc.includes('最近UTC时间') && slotsSrc.includes('unix_time') && slotsSrc.includes('utcStamp'))
// The 迁移目标 column is gone: a migrating slot already names its target —
// the incoming replica shows in the 副本集 tags, the state sits in the status
// column — so a dedicated column was pure extra width. Pinned out (both the
// title string and the backend field name) so it cannot creep back.
assert('the migration-target column is gone',
  !slotsSrc.includes('迁移目标') && !slotsSrc.includes('migrating_to'))
// The detail entry is the Slot cell itself (Typography.Link -> openDetail);
// the old 详情 button in the ops column is gone — pinned in the compiled
// children form so it cannot come back as a button.
assert('the detail entry is the Slot column link',
  slotsSrc.includes('Typography.Link') && slotsSrc.includes('openDetail(v)'))
assert('the 详情 button is gone', !slotsSrc.includes('children: "详情"'))
// The slot table's page-size changer must actually take effect. A controlled
// `pageSize` prop makes rc-pagination pin the size and discard the user's
// choice (the bug: changing page size did nothing), so the table seeds
// `defaultPageSize` and lets the changer own the state. The drawer's stream
// table keeps a hardcoded `pageSize: 20` on purpose (no size changer shown),
// so the assertion targets the row table's non-controlled seed, not the
// presence of any `pageSize`.
assert('the slot table seeds page size uncontrolled',
  slotsSrc.includes('defaultPageSize: 32') && !slotsSrc.includes('pagination: { pageSize: 32'))
// The Leader filter's candidate list is the DATA plane (learner ∪ both —
// the role half of the backend's HostingIDs): a pure control voter can never
// lead a slot, so listing it offered options that always filtered to nothing.
// Pinned both ways: the Select maps the filtered set, and the directory-wide
// `nodeIDs.map` is gone from this control (the migrate-target picker keeps
// `nodeIDs.filter` on purpose).
assert('the Leader filter lists data-plane members only',
  slotsSrc.includes('role !== "voter"') && slotsSrc.includes('dataNodeIDs.map')
  && !slotsSrc.includes('nodeIDs.map'))
const liveAdmin = (process.env.PUSHUPES_ADMIN ?? '').trim()
if (liveAdmin) {
  try {
    const st = await (await fetch(`${liveAdmin}/admin/cluster/status`)).json()
    // The sync gauge must read back in its pinned shape: a [led, synced]
    // pair of non-negative ints with synced <= led (a node cannot report
    // more converged slots than it leads).
    const w = await (await fetch(`${liveAdmin}/admin/status`)).json()
    // The client-origin pair the rate cards diff must exist and align with
    // the total arrays it pairs against (same slot per index).
    assert('live status carries client_writes/client_write_bytes aligned with total_writes',
      Array.isArray(w.client_writes) && Array.isArray(w.client_write_bytes)
      && w.client_writes.length === w.total_writes.length
      && w.client_write_bytes.length === w.total_writes.length
      && w.client_writes.every((v, i) => v <= w.total_writes[i]))
    // The per-node 存储数量 scalar: a non-negative integer, and it must equal
    // the sum of total_writes at the slots THIS node leads — the client can
    // always re-derive the backend fold from the same answer.
    assert('live status carries storage_count as a non-negative int',
      Number.isInteger(w.storage_count) && w.storage_count >= 0)
    // …and it is re-derivable from the same answer: the node's own
    // total_writes summed at the slots the table says IT leads. storage_bytes
    // is the byte twin — a slot this node only replicates must answer 0
    // (leader-only rule), so the console's per-node footer and the cluster
    // cards talk about the same copies.
    let ledSeq = 0
    let strayBytes = 0
    for (const [slot, pl] of Object.entries(st.slots)) {
      const i = Number(slot)
      if (pl.leader === st.node) ledSeq += w.total_writes[i]
      else if ((w.storage_bytes?.[i] ?? 0) !== 0) strayBytes++
    }
    assert('live storage_count equals total_writes summed at led slots', w.storage_count === ledSeq)
    assert('live storage_bytes is leader-only (replicated slots answer 0)', strayBytes === 0)
    const pair = w.synced_slots
    assert('live writes carries synced_slots as [led, synced]',
      Array.isArray(pair) && pair.length === 2
      && pair.every((x) => Number.isInteger(x) && x >= 0) && pair[1] <= pair[0])
    // 待就位 rides next to the pair: a plain non-negative count of leading
    // slots whose replica SET is short of the factor (outside led).
    assert('live status carries pending_seats',
      Number.isInteger(w.pending_seats) && w.pending_seats >= 0)
    // The fenced view rides the same answer: an array of {slot, reason,
    // table_leader, table_epoch} verdicts (possibly empty).
    assert('live status carries pending_slots in the fenced-view shape',
      Array.isArray(w.pending_slots)
      && w.pending_slots.every((v) => Number.isInteger(v.slot) && typeof v.reason === 'string' && typeof v.table_leader === 'string' && Number.isInteger(v.table_epoch)))
    console.log(`  (live sync gauge: led=${pair[0]} synced=${pair[1]} of slot_count=${st.slot_count})`)
    assert('live status carries storage_bytes', typeof st.storage_bytes === 'number')
    assert('live status carries storage_bytes_complete', typeof st.storage_bytes_complete === 'boolean')
    assert('live status carries storage_count', typeof st.storage_count === 'number' && st.storage_count >= 0)
    assert('live status carries storage_count_complete', typeof st.storage_count_complete === 'boolean')
    console.log(`  (live probe: storage_bytes=${st.storage_bytes} → ${humanBytes(st.storage_bytes)}, storage_count=${st.storage_count}, complete=${st.storage_bytes_complete})`)
  } catch (e) {
    assert('live status reachable', false)
    console.log(`  (live probe failed: ${e?.message ?? e})`)
  }
} else {
  console.log('  (live storage probe skipped: set PUSHUPES_ADMIN=http://host:port to run it)')
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
// Offline is a STATE, not a frame style: every card keeps the same solid
// border and the 离线 tag carries the state (a faded/dashed card read as a
// broken frame). The assertion below is the one that used to demand the dashed
// variant — it now pins the opposite, so a re-introduced dash is caught.
assert('an offline card keeps the same frame (state carried by the tag)', !/border-style:dashed/.test(offlineCard) && offlineCard.includes('ant-card'))
assert('an offline peer shows - for the missing admin addr', offlineCard.includes('>-<'))
const downCard = pc({ peer: { id: 'node-3', peer_addr: 'http://h:8393', admin_addr: 'http://h:8093', client_addr: 'http://h:8593', down: true }, current: false, slots: 0 })
assert('a marked-down peer with announced addresses is still tagged 离线',
  downCard.includes('离线') && downCard.includes('http://h:8593') && !/border-style:dashed/.test(downCard))
const onlineCard = pc({ peer: { id: 'node-1', peer_addr: 'http://h:8391', admin_addr: 'http://h:8091', client_addr: 'http://h:8591' }, current: true, slots: 561 })
assert('an online peer renders no 离线 tag', !onlineCard.includes('离线') && onlineCard.includes('当前') && !/border-style:dashed/.test(onlineCard))
// The control-plane (plain) card shape: flow-control tag and button gone,
// rate chart gone, action row gone; identity/offline/当前 intact. The data
// card keeps the 流控 face and the chart. NEVER a 移除 button: membership is
// CLI-only, and no PeerCard prop can bring it back.
const plainPeer = { id: 'node-9', peer_addr: 'http://h:8399', admin_addr: 'http://h:8099', client_addr: 'http://h:8599' }
const plainCard = pc({ peer: plainPeer, current: true, slots: 0, plain: true, flowControlConfigured: true })
const fullCard = pc({ peer: plainPeer, slots: 3, flowControlConfigured: true, onFlowControl: () => {} })
// Membership stays out of the console, but "danger-styled" is not the same thing
// as "removes a member": the drain card legitimately carries a danger 取消缩容
// button (it undoes a scale-down the operator started), so the check is on the
// wording, not on antd's danger class.
assert('no peer card ever renders 移除', !plainCard.includes('移除') && !fullCard.includes('移除'))
assert('a plain card renders no 流控 face', !plainCard.includes('流控') && !plainCard.includes('anticon-control'))
assert('a plain card renders no rate chart',
  !plainCard.includes('evts/sec') && !plainCard.includes('polyline') && !plainCard.includes('采集中'))
assert('a plain card renders no slots tag', !plainCard.includes(' slots'))
assert('a plain card still marks the answering node', plainCard.includes('当前'))
assert('the data card keeps the 流控 face and the chart',
  fullCard.includes('流控') && fullCard.includes('采集中'))
assert('a card without the current prop renders no 当前 tag', !fullCard.includes('当前'))
// The leader-side replica pending tag: an orange 同步中 N when the node
// still leads slots whose replicas have not caught up; gone at zero, and
// never on a plain (control-plane) card.
const syncingCard = pc({ peer: plainPeer, slots: 3, syncing: 7 })
assert('a pending leader card shows the orange 同步中 tag',
  syncingCard.includes('同步中') && syncingCard.includes('ant-tag-orange'))
assert('a fully synced leader card shows no 同步中 tag',
  !pc({ peer: plainPeer, slots: 3, syncing: 0 }).includes('同步中'))
assert('a control-plane card never shows 同步中',
  !pc({ peer: plainPeer, slots: 3, syncing: 9, plain: true }).includes('同步中'))
// The red 已围栏 N tag: shown when the node reports fenced copies (count off
// the array length; SSR splits the number behind a comment marker, so pin
// the text prefix), never at zero, never on a plain card. The tooltip
// content itself is not in SSR output — the cap shape is pinned on the
// compiled card text instead.
const fenceViews = (n) => Array.from({ length: n }, (_, i) => ({ slot: i, reason: 'owner record names a different cluster generation', table_leader: 'node-1', table_epoch: 9 }))
assert('a fenced node card shows the red 已围栏 tag',
  pc({ peer: plainPeer, slots: 3, fenced: fenceViews(12) }).includes('已围栏 ') && pc({ peer: plainPeer, slots: 3, fenced: fenceViews(1) }).includes('ant-tag-red'))
assert('a card with no fenced copies shows no 已围栏 tag', !pc({ peer: plainPeer, slots: 3, fenced: [] }).includes('已围栏'))
assert('a control-plane card never shows 已围栏', !pc({ peer: plainPeer, slots: 3, fenced: fenceViews(3), plain: true }).includes('已围栏'))
const pcardSrc = String(pc && (await vite.ssrLoadModule('/src/PeerCard.tsx')).PeerCard)
assert('the fenced tooltip lists slot + reason, capped at ten rows with a total line',
  pcardSrc.includes('slice(0, 10)') && pcardSrc.includes('共 ') && pcardSrc.includes('个槽')
  && pcardSrc.includes('槽 ') && pcardSrc.includes('reason'))
// The data card's FOOTER carries this node's leader-copy footprint — the
// per-card mirror of the cluster 存储数量/存储大小 cards: storage_count (the
// seq total of the slots it leads) and Σ storage_bytes (the byte twin,
// followers answer 0). '-'-filled until the node answers, and never on a
// plain control-plane card (it leads nothing, so the row is not rendered).
const storageCard = pc({ peer: plainPeer, slots: 3, storageCount: 1920, storageBytes: 2069533 })
assert('a data card shows its storage count and size at the bottom',
  storageCard.includes('存储数量') && /1,?920/.test(storageCard)
  && storageCard.includes('存储大小') && storageCard.includes('2.0 MiB'))
assert('the storage footer renders - until the node answers',
  pc({ peer: plainPeer, slots: 3 }).includes('存储数量') && /储数量[\s\S]{0,400}?-/.test(pc({ peer: plainPeer, slots: 3 })))
assert('a control-plane card carries no storage footer',
  !plainCard.includes('存储数量') && !plainCard.includes('存储大小'))

// The node-card dual-axis chart: FOUR polylines over one time axis — solid
// client lines and dashed total lines per unit — with a same-timestamp
// hover box, and the sampling hint below two points.
const { NodeRateChart } = await vite.ssrLoadModule('/src/NodeRateChart.tsx')
const nrPoints = [
  { t: Date.parse('2026-10-10T08:00:00'), msg: 100, kib: 12, totalMsg: 180, totalKib: 22 },
  { t: Date.parse('2026-10-10T08:00:02'), msg: 200, kib: 25, totalMsg: 260, totalKib: 40 },
  { t: Date.parse('2026-10-10T08:00:04'), msg: 150, kib: 18, totalMsg: 330, totalKib: 60 },
  { t: Date.parse('2026-10-10T08:00:06'), msg: 300, kib: 30, totalMsg: 450, totalKib: 75 },
]
const nrChart = renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(NodeRateChart, { points: nrPoints })),
)
assert('node chart draws four polylines (solid client + dashed total per unit)',
  (nrChart.match(/<polyline/g) ?? []).length === 4
  && (nrChart.match(/stroke-dasharray="5 4"/g) ?? []).length === 2)
assert('node chart labels both y axes', nrChart.includes('msg/s') && nrChart.includes('KiB/s'))
assert('node chart legend names all four lines with latest values',
  nrChart.includes('客户端写入速度（数量）300') && nrChart.includes('客户端写入速度（大小）30')
  && nrChart.includes('总写入速度（数量）450') && nrChart.includes('总写入速度（大小）75'))
// The legend is TWO columns (operator's call): 数量 on the left, 大小 on the
// right, one metric per line inside each column — so the document order is
// 数量 ×2 then 大小 ×2, inside the chart's outer vertical stack (three
// ant-space-vertical in the rendered HTML: outer + two columns).
const legendOrder = ['客户端写入速度（数量）', '总写入速度（数量）', '客户端写入速度（大小）', '总写入速度（大小）']
  .map((s) => nrChart.indexOf(s))
assert('node chart legend splits into 数量 (left) / 大小 (right) columns, one metric per line',
  legendOrder.every((i) => i >= 0)
  && legendOrder.every((v, i) => i === 0 || legendOrder[i - 1] < v)
  && (nrChart.match(/ant-space-vertical/g) ?? []).length === 3)
// The hover box cannot be exercised by SSR (no mouse), so pin its wiring in
// the compiled function text: pointer handlers + the same-timestamp rows.
const nrSrc = String(NodeRateChart)
assert('node chart hovers into a same-timestamp metric box',
  nrSrc.includes('onMouseMove') && nrSrc.includes('onMouseLeave')
  && nrSrc.includes('客户端写入速度（数量） ${') && nrSrc.includes('总写入速度（数量） ${'))
// The guide sits AT THE MOUSE, not at a rect-guessed position: the mouse x
// maps through the SVG's own transform (letterbox margins when the card is
// wider than the 440 viewBox were pushing the line off the cursor), the
// guide line carries that raw x while markers and box snap to the nearest
// sample. Pinned on the compiled forms.
assert('hover guide tracks the mouse through the SVG transform',
  nrSrc.includes('getScreenCTM') && nrSrc.includes('matrixTransform')
  && nrSrc.includes('x1: hover.x') && nrSrc.includes('xAt(hover.i)'))
assert('node chart ticks the x axis with sample times', (nrChart.match(/\d{2}:\d{2}:\d{2}/g) ?? []).length >= 2)
const nrEmpty = renderToString(
  React.createElement(ConfigProvider, { theme: { algorithm: theme.defaultAlgorithm } },
    React.createElement(NodeRateChart, { points: nrPoints.slice(0, 1) })),
)
assert('node chart asks for samples while collecting', nrEmpty.includes('采集中') && !nrEmpty.includes('<polyline'))

let failed = 0
for (const [name, ok] of checks) {
  if (!ok) failed++
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}`)
}
console.log(`render length light=${light.length} dark=${dark.length}`)
await vite.close()
process.exit(failed ? 1 : 0)
