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
  // replica_factor is the per-slot copy count in force; replica_policy is the
  // tier behind it (low/medium/high). The factor is derived from the policy
  // and the member count by the controller; the policy is set through
  // POST /admin/cluster/replica-policy (the startup flag only seeds a new
  // cluster), so this is the live cluster-wide value every node reads back.
  replica_factor?: number
  replica_policy?: string
  writes?: number[]
  // Cluster storage: the sum of every slot's LEADER copy (bytes on disk). It is
  // a background sample the backend refreshes every couple of seconds, so it
  // lags the write path slightly; storage_bytes_complete=false means a slot
  // leader did not answer and the number is a lower bound. Absent on older
  // nodes — the card renders '-' then.
  storage_bytes?: number
  storage_bytes_complete?: boolean
  // Machine-readable leader redirect (every node answers it): the Raft
  // controller's node id and its admin address from the replicated peer
  // directory. Empty while no leader is elected / registered yet. The console
  // pins all traffic here; older nodes without these fields fall back to
  // raft.leader + peers[leader].admin_addr.
  controller?: string
  controller_admin_addr?: string
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
  // unix_time of the aggregate's latest record (UTC seconds), 0 when the node
  // has no timestamp for it. Read out of the slot's in-memory directory along
  // with the version — the listing still reads no WAL file.
  unix_time: number
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
  // Durable record bytes per slot (frame bytes on disk), index-aligned with
  // `writes`: diffing two snapshots turns one window into a message rate and
  // a byte rate. The cluster page's write-speed cards require both.
  write_bytes?: number[]
  bytes?: number[]
  streams?: number[]
  // Per-slot post-migration cleanup queue of THAT node: 0, or the unix second
  // at which that node drops its local copy of the slot. Per-node by nature —
  // the console marks a replica from the node that reported it.
  dropping?: number[]
  // Node-level flow control (throttle of this node's slot leaders).
  // `flow_control` is the config in force (configured=true drives the node
  // card's 流控 tag); `flow_control_hits` is the per-slot cumulative
  // rejection count, index-aligned with `writes` — the slots page diffs
  // successive snapshots to mark the slots whose throttle FIRED recently.
  flow_control?: FlowControlView
  flow_control_hits?: number[]
}

// FlowControlView is the config shape /admin/writes reports per node.
export interface FlowControlView {
  tokens_per_slot: number
  period_ms: number
  configured: boolean
}

// FlowControlDetail is /admin/flow-control's answer: the same config view
// plus the duration string a POST body round-trips against and the hit
// tallies (node total + per-slot list).
export interface FlowControlDetail {
  node?: string
  tokens_per_slot: number
  period: string
  period_ms: number
  unlimited: boolean
  total_hits: number
  slots: { slot: number; hits: number }[]
}
