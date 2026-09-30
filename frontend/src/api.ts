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
// the light endpoint (no 4096-entry slot table) the console polls every 2s:
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

export async function describeSlot(slot: number, node: string): Promise<SlotDescribe> {
  const { data } = await http.get(`/admin/slots/${slot}/describe`, { baseURL: nodeApi(node) })
  return data
}

export async function migrateSlot(slot: number, toNode: string, node: string): Promise<void> {
  await http.post(`/admin/slots/${slot}/migrate`, { to_node: toNode }, { baseURL: nodeApi(node) })
}

// The console proxies /api/* to one node; node-scoped calls reuse that
// default when no explicit peer address is configured. nodeApi keeps the
// extension point for multi-node direct mode without touching call sites.
function nodeApi(_node: string): string {
  return '/api'
}
