import axios from 'axios'
import type {
  ClusterStatus, NodeWrites, SlotDescribe, SlotStreams,
} from './types'

const http = axios.create({ baseURL: '/api', timeout: 15000 })

export async function getClusterStatus(): Promise<ClusterStatus> {
  const { data } = await http.get('/admin/cluster/status')
  return data
}

// fetchNodeWrites pulls one node's per-slot counters AND gauges over CORS —
// the light endpoint (no full per-slot table) the console polls every 2s:
// `writes` is diffed into per-slot write rates, `bytes`/`streams` are shown as
// columns. A node that has not loaded a slot reports 0 for it rather than
// opening it (opening scans the slot's WAL), so the caller takes the answer
// from whichever node holds the slot. Admin addresses from the status API
// carry an http:// scheme; older/bare host:port values are tolerated.
export async function fetchNodeWrites(addr: string): Promise<NodeWrites> {
  const base = addr.includes('://') ? addr : `http://${addr}`
  const { data } = await axios.get<NodeWrites>(`${base}/admin/writes`, { timeout: 5000 })
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
  const base = addr.includes('://') ? addr : `http://${addr}`
  const { data } = await axios.get<SlotStreams>(`${base}/admin/slots/${slot}/streams`, {
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
  const base = addr.includes('://') ? addr : `http://${addr}`
  const { data } = await axios.get<SlotDescribe>(`${base}/admin/slots/${slot}/describe`, { timeout: 8000 })
  return data
}

// ---- controller-addressed write commands -----------------------------------
//
// Every admin command that mutates Raft state (the replicated slot table) —
// migrate, remove-replica, plan — is controller-only: only the Raft leader can
// commit it. A follower REFUSES such a command (429, err_id 1005) instead of
// forwarding it: the admin plane is not a proxy for the controller, and
// node-to-node traffic lives on the peer plane. So the console reads the
// controller's admin address out of the same status table it already polls and
// posts the command there directly. Everything below is the single path for
// those writes — no write command may be addressed anywhere else.

// adminBase normalizes an admin address from the status table (it carries an
// http:// scheme; older/bare host:port values are tolerated).
function adminBase(addr: string): string {
  return addr.includes('://') ? addr : `http://${addr}`
}

// controllerAdminAddr is the shared "which node may I write to?" lookup: the
// controller (Raft leader) admin address from a status snapshot. It throws a
// readable error when the table has no elected (or not yet registered)
// controller — the case withController retries after a status refresh.
export function controllerAdminAddr(st: ClusterStatus | null): string {
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
// controller". We then re-read the status ONCE and retry against the new
// controller; if that also refuses, the caller gets a readable error
// ("控制器已切到 X，请重试") instead of a raw transport error.
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
      if (!isNotControllerError(e)) throw e
      if (attempt >= 1) {
        throw new Error(`控制器已切到 ${st?.raft?.leader || '(未知)'}，请重试`)
      }
      st = await refresh() // the leader moved between the status read and the request
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
      `${adminBase(addr)}/admin/slots/${slot}/migrate`,
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
      `${adminBase(addr)}/admin/slots/${slot}/remove-replica`,
      { node },
      { timeout: 30000 },
    ),
    status,
  )
}
