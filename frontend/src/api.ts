import axios from 'axios'
import type {
  ClusterStatus, NodeWrites, SlotDescribe, SlotStreams,
} from './types'

// ---- admin endpoint pool ----------------------------------------------------
//
// The console talks to the cluster through a POOL of admin base URLs, not one
// fixed host. Configuration (in priority order):
//   1. window.__PUSHUPES_ADMIN__ — set in index.html before the bundle loads;
//      works for `vite preview` and any static deployment (edit dist/index.html
//      or serve your own copy).
//   2. PUSHUPES_ADMIN_ENDPOINTS — build-time env, comma-separated, e.g.
//      PUSHUPES_ADMIN_ENDPOINTS="http://10.0.0.1:8091,http://10.0.0.2:8091" npm run build
//   3. '/api' — the dev-server proxy fallback (single node behind /api/*).
//
// With more than one entry the client probes the pool at startup, finds which
// node holds the Raft leadership, and pins ALL subsequent traffic (status
// polls, per-node writes, slot reads, controller commands) to that leader's
// own admin base. When the leader moves — its node stops answering, or the
// status table names a different leader — the pin follows it automatically;
// when every pinned route fails, the whole pool is re-probed.
export function adminPool(): string[] {
  const g: any = typeof window !== 'undefined' ? window : globalThis
  // NOTE the || (not ??): index.html always DEFINES window.__PUSHUPES_ADMIN__,
  // usually as '' — with ?? the empty string would win over the env var and
  // silently force every deployment onto the single-node /api proxy (which is
  // exactly how a dev session with PUSHUPES_ADMIN_ENDPOINTS set ended up with
  // no pool at all: kill the one proxied node and the console dies). An empty
  // override must mean "not configured", not "no endpoints".
  const override = String(g.__PUSHUPES_ADMIN__ ?? '').trim()
  const raw = override || (import.meta as any).env?.PUSHUPES_ADMIN_ENDPOINTS || ''
  const parts = String(raw).split(',').map((s: string) => s.trim()).filter(Boolean)
  return parts.length ? parts : ['/api']
}

// normalizeAdminBase turns one configured/admin address into a fetchable
// base: bare host:port gets an http:// scheme; anything else is kept as-is
// ('/api' proxy path, full http(s) URL).
function normalizeAdminBase(addr: string): string {
  const a = addr.trim()
  if (!a) return a
  return a.includes('://') ? a : `http://${a}`
}

let currentBase = adminPool()[0]

// resolveLeaderBase maps a status snapshot taken from ANY pool member onto the
// BASE URL of the node that holds the Raft leadership. Preferred source: the
// controller_* fields every node answers with (id + admin address resolved
// server-side). Fallback for older nodes without them: peers[leader].admin_addr
// keyed by raft.leader. Falls back to the answering node when the table has no
// resolvable leader yet (mid-election, brand-new cluster) — pinning to a live
// node is always safe, reads work everywhere and writes re-resolve the
// controller before posting.
function resolveLeaderBase(st: ClusterStatus, cameFrom: string): string {
  if (st?.controller_admin_addr) return normalizeAdminBase(st.controller_admin_addr)
  const leader = st?.raft?.leader ?? ''
  const addr = leader ? st?.peers?.[leader]?.admin_addr : undefined
  return addr ? normalizeAdminBase(addr) : cameFrom
}

// probePool walks the configured pool until one node answers BOTH its health
// endpoint and /admin/cluster/status, pins the client to the leader's base
// resolved from that answer, and returns the status OF THAT PINNED NODE.
//
// Returning the seed's own snapshot would be wrong even though the cluster
// view is replicated: the page renders `node` (the PeerCard "this node"
// marker) and `raft.state` straight from it, so a console pinned to the leader
// but handed a follower's answer would show "Follower" and tag the wrong node
// as current — reading as "the console did not follow the leader". Hence the
// extra read from the pinned base whenever the pin differs from the seed.
//
// The health pre-check matters too: a half-dead node can hold its listener
// open while every request hangs until timeout — probing /healthz first (a
// tiny answer) fails fast on those and moves on to the next address instead of
// burning the full status timeout per seed.
async function probePool(timeoutMs = 5000): Promise<{ st: ClusterStatus; base: string }> {
  let lastErr: any
  for (const b of adminPool()) {
    try {
      await axios.get(`${b}/healthz`, { timeout: Math.min(2000, timeoutMs) })
      const { data } = await axios.get<ClusterStatus>(`${b}/admin/cluster/status`, { timeout: timeoutMs })
      const base = resolveLeaderBase(data, b)
      currentBase = base
      if (base === normalizeAdminBase(b)) return { st: data, base } // seed IS the leader
      // Pin moved to another node: read once from there so the caller renders
      // the leader's own view. If that read fails, the seed's snapshot is
      // still a valid cluster view — better than failing the whole probe.
      try {
        const leader = await axios.get<ClusterStatus>(`${base}/admin/cluster/status`, { timeout: timeoutMs })
        currentBase = resolveLeaderBase(leader.data, base)
        return { st: leader.data, base: currentBase }
      } catch {
        return { st: data, base }
      }
    } catch (e) {
      lastErr = e // this seed is down/hanging/unreachable — try the next
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error('所有配置的 admin 地址均不可达')
}

// getClusterStatus refreshes the status through the currently-pinned base and
// re-checks the pin on every answer: the poll doubles as the leader-follow
// check (while the leader lives, requests keep landing on it; when it hands
// leadership over, the next poll re-pins to the new one). If the pinned node
// stops answering — or answers late enough that the UI would show an error —
// the client immediately walks the rest of the pool in order (health-probed)
// and takes the first good answer, so one dead node never surfaces as
// "无法连接后端".
export async function getClusterStatus(): Promise<ClusterStatus> {
  try {
    const { data } = await axios.get<ClusterStatus>(`${currentBase}/admin/cluster/status`, { timeout: 4000 })
    currentBase = resolveLeaderBase(data, currentBase)
    return data
  } catch {
    // the pinned node died or stalled mid-request: don't sit out a second
    // long timeout against it — go straight to the pool walk.
    const { st } = await probePool()
    return st
  }
}

// ensureLeader runs the startup probe once when several admins are configured:
// it pins the client to the leader BEFORE the first page render, so pages
// never paint against a follower. Single-entry configs skip the extra request.
// Returns the status it resolved with; concurrent callers share one probe.
let bootProbe: Promise<ClusterStatus> | null = null
export function ensureLeader(): Promise<ClusterStatus> {
  if (adminPool().length <= 1) {
    return getClusterStatus()
  }
  if (!bootProbe) {
    bootProbe = probePool().then((r) => r.st)
    bootProbe.catch(() => { bootProbe = null }) // allow a retry after a failed boot
  }
  return bootProbe
}

// fetchNodeWrites pulls one node's per-slot counters AND gauges over CORS —
// the light endpoint (no full per-slot table) the console polls every 2s:
// `writes` is diffed into per-slot write rates, `bytes`/`streams` are shown as
// columns. A node that has not loaded a slot reports 0 for it rather than
// opening it (opening scans the slot's WAL), so the caller takes the answer
// from whichever node holds the slot. Admin addresses from the status API
// carry an http:// scheme; older/bare host:port values are tolerated.
export async function fetchNodeWrites(addr: string): Promise<NodeWrites> {
  const { data } = await axios.get<NodeWrites>(`${normalizeAdminBase(addr)}/admin/writes`, { timeout: 5000 })
  return data
}

// fetchSlotStreams lists one slot's event streams (aggregate id + latest
// version) with a cursor: pass the previous page's next_after to continue.
// The listing is served from the node's in-memory slot index, so it reads no
// WAL file; a node that has not opened the slot answers loaded:false and the
// caller asks the leader / a replica (their admin addresses are in the status
// table) instead.
export async function fetchSlotStreams(
  addr: string, slot: number, after = '', limit = 200,
): Promise<SlotStreams> {
  const { data } = await axios.get<SlotStreams>(`${normalizeAdminBase(addr)}/admin/slots/${slot}/streams`, {
    params: { limit, after },
    timeout: 8000,
  })
  return data
}

// fetchSlotDescribe reads one slot's view from a SPECIFIC node: every number
// in it is local (hw/isr only exist on the leader, last_seq/size come from that
// node's copy of the slot), so the console asks the slot's holder — leader
// first, then its replicas — instead of whichever node it is proxied to.
export async function fetchSlotDescribe(addr: string, slot: number): Promise<SlotDescribe> {
  const { data } = await axios.get<SlotDescribe>(`${normalizeAdminBase(addr)}/admin/slots/${slot}/describe`, { timeout: 8000 })
  return data
}

// ---- controller-addressed write commands -----------------------------------
//
// Every admin command that mutates Raft state (the replicated slot table) —
// migrate, remove-replica, plan — is controller-only: only the Raft leader can
// commit it. A follower REFUSES such a command (425, err_id 1005) instead of
// forwarding it: the admin plane is not a proxy for the controller, and
// node-to-node traffic lives on the peer plane. The status answer carries the
// controller's identity directly (controller / controller_admin_addr), so the
// console always knows where to post — even when its pinned base is a
// follower (mid-election, stale pin). Everything here is the single path for
// those writes — no write command may be addressed anywhere else.

// controllerAdminAddr is the shared "which node may I write to?" lookup: the
// controller (Raft leader) admin address from a status snapshot — preferred
// field first, then the peers-table fallback for older nodes. It throws a
// readable error when the snapshot has no elected (or not yet registered)
// controller — the case withController retries after a status refresh.
export function controllerAdminAddr(st: ClusterStatus | null): string {
  if (st?.controller_admin_addr) return st.controller_admin_addr
  const leader = st?.raft?.leader ?? ''
  const addr = leader ? st?.peers?.[leader]?.admin_addr : undefined
  if (!addr) {
    throw new Error(
      leader
        ? `找不到控制器 ${leader} 的 admin 地址（它可能刚当选、还没注册），请稍后重试`
        : '集群暂时没有选出控制器（Raft leader），请稍后重试',
    )
  }
  return addr
}

// isNotControllerError recognizes a follower's refusal of a controller-only
// command: the admin plane answers 429 with err_id 1005 (data.ErrIDNotLeader)
// and a message that says "not the controller" and names the node that is.
export function isNotControllerError(e: any): boolean {
  const d = e?.response?.data
  if (d?.err_id === 1005) return true
  const msg = d?.error ?? e?.message ?? ''
  return /not the controller|not the cluster leader|not controller/i.test(String(msg))
}

// withController is the shared executor for controller-only commands: it
// resolves the controller from the status snapshot the console already holds
// (no extra request while the leader is stable) and calls the command against
// that node directly.
//
// The real race it closes: the leader can change between the status read and
// the request landing, in which case the node we picked refuses with "not the
// controller". We then re-read the status ONCE — which also re-pins the pool
// to the new leader — and retry against it; if that also refuses, the caller
// gets a readable error ("控制器已切到 X，请重试") instead of a raw transport
// error. A transport failure on the target (it died between the two reads)
// likewise falls through to one fresh pool-walking status refresh + retry.
export async function withController<T>(
  call: (addr: string) => Promise<T>,
  status: ClusterStatus | null = null,
  refresh: () => Promise<ClusterStatus> = getClusterStatus,
): Promise<T> {
  let st = status
  for (let attempt = 0; attempt < 2; attempt++) {
    let addr: string
    try {
      addr = controllerAdminAddr(st)
    } catch (e) {
      if (attempt >= 1) throw e
      st = await refresh() // the cached table has no usable controller yet
      continue
    }
    try {
      return await call(addr)
    } catch (e) {
      const refused = isNotControllerError(e)
      const unreachable = !refused && /network|timeout|ECONN|status code 0/i.test(String((e as any)?.message ?? ''))
      if (!refused && !unreachable) throw e
      if (attempt >= 1) {
        throw new Error(`控制器已切到 ${st?.controller || st?.raft?.leader || '(未知)'}，请重试`)
      }
      st = await refresh() // the leader moved (or died) between the status read and the request
    }
  }
  throw new Error('控制器不可用，请重试')
}

// migrateSlot starts a hot migration, addressed to the controller: it is a
// controller-only command and the console's /api proxy may well be fronting a
// follower. Pass the cached status table (no extra request) or let it fetch.
export async function migrateSlot(
  slot: number, toNode: string, status: ClusterStatus | null = null,
): Promise<void> {
  await withController(
    (addr) => axios.post(
      `${normalizeAdminBase(addr)}/admin/slots/${slot}/migrate`,
      { to_node: toNode },
      { timeout: 60000 },
    ),
    status,
  )
}

// removeSlotReplica reclaims one member from a slot's replica set (the
// operator's fallback when an automatic post-migration reclaim could not run),
// addressed to the controller for the same reason as migrateSlot.
export async function removeSlotReplica(
  slot: number, node: string, status: ClusterStatus | null = null,
): Promise<void> {
  await withController(
    (addr) => axios.post(
      `${normalizeAdminBase(addr)}/admin/slots/${slot}/remove-replica`,
      { node },
      { timeout: 30000 },
    ),
    status,
  )
}
