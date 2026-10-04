// Live integration check for the console's admin pool against a REAL pushupes
// cluster with NO HTTP proxy in front: the browser layer (src/api.ts compiled
// with esbuild, run on Node) talks to every node's admin address directly over
// CORS.
//
//   1. boot through the configured pool -> probe finds the leader, pins there;
//   2. SIGTERM the leader's process;
//   3. wait for the surviving nodes to elect a new Raft leader;
//   4. the NEXT getClusterStatus must walk PUSHUPES_ADMIN_ENDPOINTS in order
//      until one answers, then re-pin all traffic to the new leader;
//   5. restart the killed node (cluster left whole).
//
// Usage: node scripts/live-leader-follow-check.mjs [admin1,admin2,admin3]
// (default http://127.0.0.1:8091,http://127.0.0.1:8092,http://127.0.0.1:8093,
//  i.e. the ports scripts/cluster.sh uses)
import { build } from 'esbuild'
import { execFileSync, spawn } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const ROOT = path.resolve(__dirname, '..', '..')
const POOL = (process.argv[2] ?? 'http://127.0.0.1:8091,http://127.0.0.1:8092,http://127.0.0.1:8093')
  .split(',').map((s) => s.trim()).filter(Boolean)

const checks = []
const assert = (name, cond, extra = '') => checks.push([name, !!cond, extra])
const report = () => {
  let failed = 0
  for (const [name, ok, extra] of checks) {
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${!ok && extra ? '  [' + extra + ']' : ''}`)
    if (!ok) failed++
  }
  return failed
}

async function statusFrom(addr) {
  const r = await fetch(`${addr}/admin/cluster/status`, { signal: AbortSignal.timeout(5000) })
  return r.json()
}

// ---- compile the REAL api.ts, configure the pool before module load --------
const out = await build({
  entryPoints: [path.join(__dirname, '..', 'src', 'api.ts')],
  bundle: true, format: 'esm', platform: 'browser', target: 'es2020',
  define: { 'import.meta.env.PUSHUPES_ADMIN_ENDPOINTS': 'undefined' },
  write: false,
})
globalThis.window = { __PUSHUPES_ADMIN__: POOL.join(',') }
const modUrl = 'data:text/javascript;base64,' + Buffer.from(out.outputFiles[0].text).toString('base64')
const api = await import(modUrl)

// ---- ground truth: who leads right now? ------------------------------------
let leaderAddr = ''
for (const a of POOL) {
  try {
    const st = await statusFrom(a)
    if (st.raft.state === 'Leader') { leaderAddr = a; break }
  } catch { /* node down */ }
}
assert('a live leader exists before the test', !!leaderAddr, POOL.join(','))
if (!leaderAddr) process.exit(report() ? 1 : 0)

// ---- 1) boot through the pool ------------------------------------------------
const st1 = await api.ensureLeader()
assert('boot resolved raft leader id', !!st1.raft.leader, `leader=${st1.raft.leader}`)
const pinnedAdmin = st1.peers?.[st1.raft.leader]?.admin_addr
assert('pinned leader resolves to its own admin addr', !!pinnedAdmin, String(pinnedAdmin))

// ---- 2) kill the leader node -------------------------------------------------
const leaderPort = new URL(leaderAddr).port
const pidLine = execFileSync('sh', ['-c',
  `ps -eo pid,args | grep '[b]in/pushupes' | grep -- "-admin 127.0.0.1:${leaderPort} " || true`]).toString().trim()
const leaderPid = Number(pidLine.split(/\s+/)[0])
if (!leaderPid) { console.log(`FAIL could not find the pid of the leader on :${leaderPort}; aborting without killing anything`); process.exit(1) }
console.log(`killing leader pid=${leaderPid} (admin :${leaderPort})`)
process.kill(leaderPid, 'SIGTERM')

// ---- 3) wait for the new election on the survivors ---------------------------
let newLeaderId = st1.raft.leader
const deadline = Date.now() + 30_000
while (newLeaderId === st1.raft.leader && Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1000))
  for (const a of POOL) {
    if (a === leaderAddr) continue
    try {
      const st = await statusFrom(a)
      if (st.raft.leader && st.raft.leader !== st1.raft.leader) { newLeaderId = st.raft.leader; break }
    } catch { /* still converging */ }
  }
}
assert('survivors elected a new Raft leader after the kill', newLeaderId !== st1.raft.leader, `${st1.raft.leader} -> ${newLeaderId}`)

// ---- 4) the console follows: poll walks the pool, re-pins --------------------
const st2 = await api.getClusterStatus()
assert('poll recovered by walking the pool (answered despite dead pin)', !!st2?.raft?.leader, JSON.stringify(st2)?.slice(0, 60))
assert('status names the new leader', st2.raft.leader === newLeaderId, `${st2.raft.leader} vs ${newLeaderId}`)
// The console must render the LEADER as the node it is talking to: the page
// tags `status.node` as current and shows `raft.state`, so a follower's answer
// reads as "not following the leader".
assert('console reports the NEW leader as its current node', st2.node === newLeaderId && st2.raft.state === 'Leader', `node=${st2.node} state=${st2.raft.state}`)
const st3 = await api.getClusterStatus()
assert('subsequent polls stay consistent on the new leader', st3.raft.leader === newLeaderId && st3.node === newLeaderId)

// ---- 5) restore the killed node ----------------------------------------------
const nodeIdx = POOL.indexOf(leaderAddr) + 1   // scripts/cluster.sh port convention: admin = ADMIN_BASE + i - 1
const dir = path.join(ROOT, '.cluster', `node-${nodeIdx}`)
console.log(`restarting node-${nodeIdx} from ${dir}`)
const child = spawn(path.join(ROOT, 'bin', 'pushupes'), [
  '-node', `node-${nodeIdx}`,
  '-admin', `127.0.0.1:${8090 + nodeIdx}`, '-peer', `127.0.0.1:${8390 + nodeIdx}`,
  '-client', `127.0.0.1:${8590 + nodeIdx}`,
  '-data', './data',
  '-peers', 'node-1=http://127.0.0.1:8391,node-2=http://127.0.0.1:8392,node-3=http://127.0.0.1:8393',
  '-replication-factor', '2', '-segment-bytes', '256MiB',
], { cwd: dir, stdio: 'ignore', detached: true })
child.unref()
execFileSync('sh', ['-c', `echo ${child.pid} > '${dir}/node.pid'`])
await new Promise((r) => setTimeout(r, 4000))
try {
  const back = await statusFrom(leaderAddr)
  assert('killed node rejoined and answers again', !!back.node, back.node)
} catch (e) {
  assert('killed node rejoined and answers again', false, String(e))
}

process.exit(report() ? 1 : 0)
