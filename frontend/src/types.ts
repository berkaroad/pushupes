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



export interface SlotDescribe {
  slot: number
  placement: Placement | null
  hw: number
  last_seq: number
  isr: string[] | null
  segments: number
  total_bytes: number
}
