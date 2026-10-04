// Regression probe for the console's auto-refresh.
//
// The cluster page polls ensureLeader() every 5s and stores the result with
// setStatus(). Two things have to hold or the view silently freezes at the
// boot-time snapshot with no error anywhere:
//
//   1. the status must be RE-READ, not served from a cached promise; and
//   2. each read must return a NEW object — React bails out of a state update
//      whose value is reference-equal to the current one, so reusing a snapshot
//      object makes every tick a no-op.
//
// It runs the REAL src/api.ts (compiled with esbuild, the same module the
// bundle ships) against two fake admin nodes whose status carries a counter
// that changes on every request, so "did the page see fresh data" is directly
// observable.
import { build } from 'esbuild'
import http from 'node:http'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

let statusHits = 0
const nodes = {}

function makeServer(id) {
  const srv = http.createServer((req, res) => {
    if (req.url.startsWith('/healthz')) {
      res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' })
      res.end('ok')
      return
    }
    if (req.url.startsWith('/admin/cluster/status')) {
      statusHits++
      res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
      res.end(JSON.stringify({
        node: id,
        raft: { state: id === 'node-1' ? 'Leader' : 'Follower', leader: 'node-1' },
        peers: Object.fromEntries(Object.entries(nodes).map(([pid, n]) => [pid, { id: pid, admin_addr: n.admin }])),
        slots: {}, slot_count: 8,
        writes: [statusHits], // changes on every request
        controller: 'node-1', controller_admin_addr: nodes['node-1'].admin,
      }))
      return
    }
    res.writeHead(404)
    res.end()
  })
  return new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => {
      nodes[id].port = srv.address().port
      nodes[id].admin = `http://127.0.0.1:${nodes[id].port}`
      resolve(srv)
    })
  })
}

for (const id of ['node-1', 'node-2']) nodes[id] = {}
const servers = await Promise.all(['node-1', 'node-2'].map(makeServer))

// Compile the real api.ts, with the pool configured the way index.html would.
const out = await build({
  entryPoints: [path.join(__dirname, '..', 'src', 'api.ts')],
  bundle: true, format: 'esm', platform: 'browser', target: 'es2020',
  define: { 'import.meta.env.PUSHUPES_ADMIN_ENDPOINTS': 'undefined' },
  write: false,
})
globalThis.window = { __PUSHUPES_ADMIN__: `${nodes['node-1'].admin},${nodes['node-2'].admin}` }
const api = await import('data:text/javascript;base64,' + Buffer.from(out.outputFiles[0].text).toString('base64'))

const checks = []
const assert = (name, cond, extra = '') => checks.push([name, !!cond, extra])

// --- the page's first render ------------------------------------------------
const boot = await api.ensureLeader()
const bootHits = statusHits
assert('boot read the status', bootHits >= 1, `hits=${bootHits}`)

// --- one 5s tick later ------------------------------------------------------
const tick = await api.ensureLeader()
assert('a tick issues a fresh status read', statusHits > bootHits,
  `hits stayed at ${bootHits} — ensureLeader served a cached result`)
assert('a tick returns a NEW object (React re-renders on a changed reference)', tick !== boot,
  'the same object came back, so setStatus() is a no-op and the page freezes')
assert('a tick carries the fresh value', tick.writes?.[0] !== boot.writes?.[0],
  `writes ${JSON.stringify(boot.writes)} -> ${JSON.stringify(tick.writes)}`)

// --- and the pin survives the polls ----------------------------------------
assert('polls stay pinned to the leader', tick.node === 'node-1', tick.node)

for (const s of servers) s.close()
let failed = 0
for (const [name, ok, extra] of checks) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${!ok && extra ? '  [' + extra + ']' : ''}`)
  if (!ok) failed++
}
process.exit(failed ? 1 : 0)
