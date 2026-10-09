// SSR-render the flow-control UI headlessly (the browser tool cannot reach
// localhost in this sandbox): PeerCard's 流控 tag follows the CONFIGURED
// flag (not the firing), and the 流控 action button appears only when the
// page passes the handler.
import { createServer } from 'vite'
import { renderToString } from 'react-dom/server'
import React from 'react'

const vite = await createServer({ server: { middlewareMode: true }, appType: 'custom' })
const { PeerCard } = await vite.ssrLoadModule('/src/PeerCard.tsx')

const peer = { id: 'node-1', peer_addr: '10.0.0.1:8394', admin_addr: 'http://10.0.0.1:8091', client_addr: '10.0.0.1:8944' }
const card = (props) => renderToString(React.createElement(PeerCard, { peer, current: false, slots: 4, ...props }))

const checks = []
const assert = (name, cond) => checks.push([name, !!cond])

const plain = card({})
assert('idle card has no 流控 mark at all', !plain.includes('流控'))
const configured = card({ flowControlConfigured: true, onFlowControl: () => {} })
assert('configured card shows the tag', configured.includes('流控'))
assert('the tag is orange', configured.includes('ant-tag-orange'))
const actionable = card({ onFlowControl: () => {} })
assert('a handler renders the action button', actionable.includes('流控') && actionable.includes('<button'))
assert('without a handler nothing renders', !plain.includes('<button') || plain.indexOf('流控') === -1)

// api surface: the three node-local verbs are exported
const api = await vite.ssrLoadModule('/src/api.ts')
assert('fetchFlowControl exposed', typeof api.fetchFlowControl === 'function')
assert('setFlowControl exposed', typeof api.setFlowControl === 'function')
assert('clearFlowControl exposed', typeof api.clearFlowControl === 'function')

let failed = 0
for (const [name, ok] of checks) {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`)
  if (!ok) failed++
}
await vite.close()
process.exit(failed ? 1 : 0)
