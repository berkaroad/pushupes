// Faithful reproduction of the DEV workflow the user runs:
//   PUSHUPES_ADMIN_ENDPOINTS="http://127.0.0.1:8091,http://127.0.0.1:8092,http://127.0.0.1:8093" npm run dev
//
// It uses vite's REAL transform pipeline (ssrLoadModule -> import.meta.env gets
// the envPrefix'd vars) and sets window.__PUSHUPES_ADMIN__ = '' exactly as
// index.html does, which is the combination that used to collapse the pool to
// the single-node /api dev proxy.
import { createServer } from 'vite'
import { execFileSync, spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.resolve(__dirname, '..', '..')
const POOL = 'http://127.0.0.1:8091,http://127.0.0.1:8092,http://127.0.0.1:8093'
process.env.PUSHUPES_ADMIN_ENDPOINTS = POOL

// what a browser gets: index.html defines the override, empty by default
globalThis.window = { __PUSHUPES_ADMIN__: '' }

const checks = []
const assert = (n, c, x = '') => checks.push([n, !!c, x])

const vite = await createServer({ server: { middlewareMode: true }, appType: 'custom' })
const api = await vite.ssrLoadModule('/src/api.ts')

// ---- A) the pool must come from the env var, not collapse to /api ----------
const pool = api.adminPool()
assert('env var produces the 3-endpoint pool (not /api)', pool.length === 3, JSON.stringify(pool))
assert('pool entries are the configured admins', pool.join(',') === POOL, JSON.stringify(pool))

// ---- B) boot pins to the current leader ------------------------------------
// The cluster has to be up for this to mean anything: report that as a failed
// check rather than dying on an unhandled rejection, which would skip section E
// and leave node-1 killed.
let st1
try {
  st1 = await api.ensureLeader()
} catch (e) {
  assert('boot reached the cluster (.cluster running?)', false, String(e?.message ?? e))
}
if (st1) {
  assert('boot resolved a leader', !!st1.raft.leader, st1.raft.leader)
  // The redirect is usable if EITHER source is present: the controller_* fields
  // (new server) or the peers[leader].admin_addr fallback (older server). The
  // console must work against both, so accept either.
  const leaderAdmin = st1.controller_admin_addr || st1.peers?.[st1.raft.leader]?.admin_addr
  assert('leader admin address is resolvable (controller field or peers fallback)', !!leaderAdmin, String(leaderAdmin))
}

// ---- helper: kill / restart one node by its admin port ---------------------
const port = (i) => 8090 + i
function pidOf(i) {
  const out = execFileSync('sh', ['-c',
    `ps -eo pid,args | grep '[b]in/pushupes' | grep -- "-admin 127.0.0.1:${port(i)} " || true`]).toString().trim()
  return Number(out.split(/\s+/)[0]) || 0
}
function killNode(i) {
  const pid = pidOf(i)
  if (!pid) return false
  process.kill(pid, 'SIGTERM')
  return true
}
function startNode(i) {
  const dir = path.join(ROOT, '.cluster', `node-${i}`)
  const args = [
    '-node', `node-${i}`,
    '-admin', `127.0.0.1:${port(i)}`, '-peer', `127.0.0.1:${8390 + i}`, '-client', `127.0.0.1:${8590 + i}`,
    '-data', './data',
    '-peers', 'node-1=http://127.0.0.1:8391,node-2=http://127.0.0.1:8392,node-3=http://127.0.0.1:8393',
    // 每槽副本数不是配置项：3 个成员推导出 2 份（ReplicaCountForMembers）。
    '-segment-bytes', '256MiB',
  ]
  if (i === 1) args.push('-bootstrap')
  const child = spawn(path.join(ROOT, 'bin', 'pushupes'), args, { cwd: dir, stdio: 'ignore', detached: true })
  child.unref()
  execFileSync('sh', ['-c', `echo ${child.pid} > '${dir}/node.pid'`])
  return child.pid
}
async function alive(i) {
  try {
    const r = await fetch(`http://127.0.0.1:${port(i)}/admin/cluster/status`, { signal: AbortSignal.timeout(2500) })
    return r.ok
  } catch { return false }
}
async function waitAlive(i, ms = 30000) {
  const end = Date.now() + ms
  while (Date.now() < end) { if (await alive(i)) return true; await new Promise((r) => setTimeout(r, 1000)) }
  return false
}

// ---- C) the user's repro: stop the FIRST pool entry, keep polling ----------
assert('node-1 is up before the kill', await alive(1))
killNode(1)
await new Promise((r) => setTimeout(r, 1500))

let recovered = false
let lastErr = ''
for (let i = 0; i < 6; i++) {
  try {
    const st = await api.getClusterStatus()
    if (st?.raft?.leader) { recovered = true; break }
  } catch (e) { lastErr = String(e?.message ?? e) }
  await new Promise((r) => setTimeout(r, 1000))
}
assert('polls keep working after the first pool entry dies', recovered, lastErr)

// ---- D) the page-level contract: never renders "无法连接后端" ---------------
// ClusterPage shows the alert only when status is null; with the pool walking
// in place, a status is still obtained above, so this is the same assertion.

// ---- E) restore node-1 -----------------------------------------------------
// Always, and even if an assertion above threw: this script kills a real node
// of the .cluster, so a failure that skipped the restart would leave the
// cluster short a node (and the next run would then fail on ECONNREFUSED with
// no hint of why).
try {
	startNode(1)
	assert('node-1 came back', await waitAlive(1), `port ${port(1)}`)
} catch (e) {
	assert('node-1 came back', false, String(e?.message ?? e))
}

let failed = 0
for (const [n, ok, x] of checks) { console.log(`${ok ? 'PASS' : 'FAIL'}  ${n}${!ok && x ? '  [' + x + ']' : ''}`); if (!ok) failed++ }
await vite.close()
process.exit(failed ? 1 : 0)
