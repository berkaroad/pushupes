// Headless test of the console's admin pool + leader-following logic.
// Stands up three fake pushupes admin servers on localhost, compiles the REAL
// src/api.ts with esbuild (browser format), injects a window/global shim, and
// drives it through: startup probe -> pin to leader -> leader moves -> re-pin;
// plus failover when the configured first seed is down.
import { build } from 'esbuild'
import http from 'node:http'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// ---- fake cluster ----------------------------------------------------------
// nodes[id] = { admin: 'http://127.0.0.1:PORT', leader: id|null, alive: bool }
const nodes = {}
let hits = [] // request log: `${id}:${path}`

function makeServer(id) {
  const srv = http.createServer((req, res) => {
    if (!nodes[id].alive) { req.socket.destroy(); return }
    hits.push(id + ':' + req.url.split('?')[0])
    if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*' }); res.end(); return }
    if (req.url.startsWith('/healthz')) {
      res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' })
      res.end('ok')
      return
    }
    if (req.url.startsWith('/admin/cluster/status')) {
      const peers = {}
      for (const [pid, n] of Object.entries(nodes)) peers[pid] = { id: pid, peer_addr: n.peer, admin_addr: n.admin, client_addr: n.client }
      const lead = nodes[id].leader
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
      res.end(JSON.stringify({
        node: id,
        raft: { state: lead === id ? 'Leader' : 'Follower', leader: lead ?? '' },
        peers, slots: {}, slot_count: 8, writes: [0, 0],
        // the new machine-readable redirect every node answers with
        controller: lead ?? '',
        controller_admin_addr: lead ? nodes[lead].admin : '',
      }))
      return
    }
    if (req.url.startsWith('/admin/writes')) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
      res.end(JSON.stringify({ node: id, slot_count: 8, writes: [1, 2], bytes: [10, 20], streams: [3, 4] }))
      return
    }
    if (req.url.includes('migrate') || req.url.includes('remove-replica')) {
      if (nodes[id].leader !== id) {
        res.writeHead(425, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
        res.end(JSON.stringify({ error: `not the controller: ${nodes[id].leader}`, err_id: 1005, controller: nodes[id].leader, controller_admin_addr: nodes[nodes[id].leader]?.admin }))
        return
      }
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
      res.end(JSON.stringify({ ok: true }))
      return
    }
    res.writeHead(404); res.end()
  })
  return new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => {
      nodes[id].port = srv.address().port
      nodes[id].admin = `http://127.0.0.1:${nodes[id].port}`
      resolve(srv)
    })
  })
}

for (const id of ['node-1', 'node-2', 'node-3']) {
  nodes[id] = { peer: `127.0.0.1:0`, client: `127.0.0.1:0`, leader: null, alive: true }
}
const servers = await Promise.all(['node-1', 'node-2', 'node-3'].map(makeServer))
// leadership: node-2 is the current Raft leader on every node's view
for (const n of Object.values(nodes)) n.leader = 'node-2'

// ---- compile the real api.ts for a browser-ish world ------------------------
const out = await build({
  entryPoints: [path.join(__dirname, '..', 'src', 'api.ts')],
  bundle: true, format: 'esm', platform: 'browser', target: 'es2020',
  define: { 'import.meta.env.PUSHUPES_ADMIN_ENDPOINTS': 'undefined' },
  write: false,
})
const code = out.outputFiles[0].text

// Configure the pool BEFORE the module loads — exactly what index.html does.
globalThis.window = { __PUSHUPES_ADMIN__: `${nodes['node-1'].admin},${nodes['node-2'].admin},${nodes['node-3'].admin}` }
const modUrl = 'data:text/javascript;base64,' + Buffer.from(code).toString('base64')
const api = await import(modUrl)

const checks = []
const assert = (name, cond, extra = '') => checks.push([name, !!cond, extra])
const resetHits = () => { hits = [] }

// 1) startup probe: first configured seed answers, client pins to node-2 (leader)
resetHits()
const st1 = await api.ensureLeader()
assert('boot status came from seed node-1', hits[0]?.startsWith('node-1:'), hits.join(','))
assert('boot resolved leader node-2', st1.raft.leader === 'node-2')
// The returned snapshot must be the LEADER's own view, not the seed's: the
// console renders st.node (the "this node" marker) and st.raft.state straight
// from it, so a follower's answer would show "Follower" and tag node-1 current
// even though traffic is correctly pinned to node-2.
assert('ensureLeader returns the LEADER\'s own status', st1.node === 'node-2', `node=${st1.node}`)
assert('ensureLeader reports Leader state', st1.raft.state === 'Leader', String(st1.raft.state))
resetHits()
await api.getClusterStatus()
assert('polls go straight to the pinned leader', hits.every((h) => h.startsWith('node-2:')), hits.join(','))

// 2) leader moves to node-3: next poll must re-pin there
nodes['node-2'].leader = 'node-3'
nodes['node-3'].leader = 'node-3'
nodes['node-1'].leader = 'node-3'
resetHits()
await api.getClusterStatus() // answered by old pin (node-2), names node-3 as leader
resetHits()
const stMoved = await api.getClusterStatus()
assert('re-pinned after leader moved', hits.every((h) => h.startsWith('node-3:')), hits.join(','))
assert('poll after the move reports the NEW leader as current', stMoved.node === 'node-3' && stMoved.raft.state === 'Leader', `node=${stMoved.node} state=${stMoved.raft.state}`)

// 3) controller-only command lands on the pinned leader directly
resetHits()
await api.migrateSlot(0, 'node-1', st1) // stale snapshot says node-2 -> refusal -> refresh -> node-3
assert('migrate retried onto the new controller', hits.some((h) => h.startsWith('node-2:') && h.includes('migrate')) && hits.some((h) => h.startsWith('node-3:') && h.includes('migrate')), hits.join(','))

// 4) pinned leader dies: getClusterStatus falls back to probing the pool
nodes['node-3'].alive = false
resetHits()
const st4 = await api.getClusterStatus()
assert('survived dead leader via pool probe', st4.node === 'node-1' || st4.node === 'node-2', hits.join(','))

// 5) all seeds dead: readable error (fresh module instance, its boot probe
// runs against the now-dead cluster)
for (const n of Object.values(nodes)) n.alive = false
let bootErr = null
const api2 = await import(modUrl + '#fresh')
try { await api2.ensureLeader(); assert('all-dead probe throws', false) }
catch (e) { bootErr = e.message }
assert('all-dead probe throws readable error', /network|不可达|failed|ECONNREFUSED|status code/i.test(String(bootErr)), String(bootErr))

for (const s of servers) s.close()

let failed = 0
for (const [name, ok, extra] of checks) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${!ok && extra ? '  [' + extra + ']' : ''}`)
  if (!ok) failed++
}
process.exit(failed ? 1 : 0)
