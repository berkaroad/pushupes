export interface Peer {
  id: string
  peer_addr: string
  admin_addr: string
  client_addr: string
  // The controller's replicated liveness verdict: consecutive peer-plane
  // probes failed. The directory entry (addresses included) survives a
  // down-mark — only leadership moves off it — so the console can show the
  // node as 离线 instead of watching it flicker in and out of the list.
  down?: boolean
}

export interface Placement {
  leader: string
  replicas: string[]
  epoch: number
  state: 'stable' | 'migrating_out' | 'importing_in' | 'backing_up'
  migrating_to?: string
}

export interface RaftStats {
  state: string
  leader: string
  applied_index?: string
  commit_index?: string
  [k: string]: unknown
}

export interface ClusterStatus {
  node: string
  raft: RaftStats
  peers: Record<string, Peer>
  slots: Record<string, Placement>
  slot_count: number
  replicas?: number
  writes?: number[]
}



// SlotDescribe is one node's view of a slot: every number is local, so the
// response says which node answered and what role it plays. hw/isr only exist
// on the leader (it tracks them from replica progress reports); a node that has
// not loaded the slot reports loaded:false with zeroed numbers instead of
// opening it.
export interface SlotDescribe {
  slot: number
  node?: string
  role?: 'leader' | 'replica' | 'none'
  loaded?: boolean
  placement: Placement | null
  hw: number
  last_seq: number
  isr: string[] | null
  segments: number
  total_bytes: number
  // Unix second at which THIS node automatically drops its local copy of the
  // slot after a migration (0 = nothing queued). The copy is surplus data —
  // the node is no longer in the slot's replica set.
  pending_drop_at?: number
}

export interface SlotStream {
  aggregate_id: string
  version: number
}

// Slot event-stream page: answered out of the node's in-memory slot index
// (aggregate id + latest version), so no WAL file is read. `loaded:false`
// means this node has not opened the slot — opening it would walk every
// segment of it — so the caller asks a node that holds it instead.
export interface SlotStreams {
  node?: string
  slot: number
  loaded: boolean
  total: number
  streams: SlotStream[]
  next_after: string
}

// One node's per-slot poll answer: durable counters (diffed into write
// rates) plus gauges — WAL bytes on disk and event-stream count per slot.
// A slot this node has not loaded reads as 0 in both gauge arrays.
export interface NodeWrites {
  node?: string
  slot_count?: number
  writes: number[]
  bytes?: number[]
  streams?: number[]
  // Per-slot post-migration cleanup queue of THAT node: 0, or the unix second
  // at which that node drops its local copy of the slot. Per-node by nature —
  // the console marks a replica from the node that reported it.
  dropping?: number[]
}
