export interface Peer {
  id: string
  peer_addr: string
  admin_addr: string
  client_addr: string
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
}
