import axios from 'axios'
import type {
  ClusterStatus, SlotDescribe,
} from './types'

const http = axios.create({ baseURL: '/api', timeout: 15000 })

export async function getClusterStatus(): Promise<ClusterStatus> {
  const { data } = await http.get('/admin/cluster/status')
  return data
}

// fetchNodeWrites pulls one node's per-slot durable counters over CORS —
// the light endpoint (counters only, no 4096-entry table) the console diffs
// every 2s to derive per-slot write rates. Admin addresses from the status
// API carry an http:// scheme; older/bare host:port values are tolerated.
export async function fetchNodeWrites(addr: string): Promise<{ writes: number[] }> {
  const base = addr.includes('://') ? addr : `http://${addr}`
  const { data } = await axios.get<{ writes: number[] }>(`${base}/admin/writes`, { timeout: 5000 })
  return data
}

export async function describeSlot(slot: number, node: string): Promise<SlotDescribe> {
  const { data } = await http.get(`/v1/slots/${slot}/describe`, { baseURL: nodeApi(node) })
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
