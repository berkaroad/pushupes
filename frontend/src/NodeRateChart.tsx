import { useState, type MouseEvent } from 'react'
import { Space, Typography, theme } from 'antd'
import { fmtRate, fmtTime } from './RateChart'

// A node-card sample: one 2s window of writes landing on the node, carried
// as FOUR numbers over two units. msg/kib are the CLIENT rate (batch appends
// the node served as the slot's leader); totalMsg/totalKib are the TOTAL
// rate — every durable write that landed on this node, client path AND the
// catch-up paths (replica fetch, migration copy). The gap between a line
// pair IS the node-to-node movement, which is why the operator gets both:
// the client lines answer "what is clients writing", the total lines answer
// "what is this disk taking". Both axes share the x (time); each scales
// against the peak of its OWN unit across both series — the units are not
// comparable on one scale, which is why the chart is dual-axis.
export type NodeRatePoint = { t: number; msg: number; kib: number; totalMsg: number; totalKib: number }

const W = 440
const H = 150
const PAD = { left: 42, right: 46, top: 16, bottom: 22 }

// NodeRateChart overlays four polylines on one time axis (plain SVG, the
// console's existing no-chart-dependency rule): the left ticks carry the
// msg/s pair (primary colour), the right ticks the KiB/s pair (orange).
// CLIENT lines are solid, TOTAL lines dashed — same colour per unit, so a
// pair reads as one story. Hovering the plot snaps a guide to the nearest
// sample and boxes every metric of THAT timestamp (the four rates plus the
// time), which is how the pairs get compared exactly. The caption names
// each line's latest value in its own colour.
export function NodeRateChart({ points }: { points: NodeRatePoint[] }) {
  const { token } = theme.useToken()
  const [hover, setHover] = useState<{ x: number; i: number } | null>(null)
  if (points.length < 2) {
    return (
      <Typography.Text type="secondary">
        采集中…（每 2s 采样一次：客户端写入按 leader 槽的 durable 计数、总写入按本节点全部落盘计数，两个样本后成图）
      </Typography.Text>
    )
  }
  const plotW = W - PAD.left - PAD.right
  const plotH = H - PAD.top - PAD.bottom
  const peakMsg = Math.max(1, ...points.map((p) => Math.max(p.msg, p.totalMsg)))
  const peakKib = Math.max(1, ...points.map((p) => Math.max(p.kib, p.totalKib)))
  const stepX = plotW / (points.length - 1)
  const xAt = (i: number) => PAD.left + i * stepX
  const yMsg = (v: number) => PAD.top + (1 - v / peakMsg) * plotH
  const yKib = (v: number) => PAD.top + (1 - v / peakKib) * plotH
  const polyAt = (key: 'msg' | 'kib' | 'totalMsg' | 'totalKib', yAt: (v: number) => number) =>
    points.map((p, i) => `${xAt(i).toFixed(1)},${yAt(p[key]).toFixed(1)}`).join(' ')
  const last = points[points.length - 1]

  const n = points.length - 1
  const xticks = [...new Set(n >= 30
    ? [0, Math.floor(n / 3), Math.floor((2 * n) / 3), n]
    : n >= 2 ? [0, Math.floor(n / 2), n] : [0, n])]
  const MSG = token.colorPrimary
  const KIB = '#fa8c16' // the 流控 tag's orange: the second unit, not the throttle

  // Client→viewBox mapping through the SVG's own transform: the svg fills
  // the card (width="100%") while the viewBox is 440 wide, and the default
  // preserveAspectRatio CENTERS the viewBox with letterbox margins whenever
  // the card is wider than the scaled viewBox — mapping the mouse by
  // rect.width alone then puts the guide tens of pixels off (narrow cards,
  // whose width lands near 440 with no margin, mapped right: that is this
  // bug's fingerprint). getScreenCTM().inverse() carries the real transform,
  // letterbox and scale included; the bounding-rect guess stays only as a
  // detached-node fallback.
  const hoverPos = (e: MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget
    const ctm = svg.getScreenCTM()
    let x: number
    if (ctm) {
      x = new DOMPoint(e.clientX, e.clientY).matrixTransform(ctm.inverse()).x
    } else {
      const rect = svg.getBoundingClientRect()
      x = ((e.clientX - rect.left) / rect.width) * W
    }
    x = Math.min(W - PAD.right, Math.max(PAD.left, x))
    setHover({ x, i: Math.min(points.length - 1, Math.max(0, Math.round((x - PAD.left) / stepX))) })
  }
  // The hover box: every metric of the NEAREST sample (the markers snap; the
  // guide itself tracks the mouse x — the line and the cursor are one line,
  // the readout names the sample it stands for). Box on the far side of the
  // marker near the plot's right edge so it never leaves the viewBox.
  const hv = hover === null ? null : points[hover.i]
  const boxW = 208
  const boxH = 76
  const boxX = hover === null ? 0 : (xAt(hover.i) + 10 + boxW > W - PAD.right ? xAt(hover.i) - 10 - boxW : xAt(hover.i) + 10)
  const boxY = PAD.top + 2
  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} role="img"
        aria-label="节点写入速度折线图（左轴 msg/s、右轴 KiB/s，实线为客户端写入、虚线为总写入，悬停显示同一时间点的各项数据）"
        onMouseMove={hoverPos} onMouseLeave={() => setHover(null)}>
        {[0, peakMsg / 2, peakMsg].map((v) => (
          <text key={v} x={PAD.left - 6} y={yMsg(v) + 3.5} fontSize={10} textAnchor="end"
            fill={MSG}>{fmtRate(v)}</text>
        ))}
        {[0, peakKib / 2, peakKib].map((v) => (
          <text key={v} x={W - PAD.right + 6} y={yKib(v) + 3.5} fontSize={10} textAnchor="start"
            fill={KIB}>{fmtRate(v)}</text>
        ))}
        {[0, peakMsg / 2, peakMsg].map((v) => (
          <line key={v} x1={PAD.left} y1={yMsg(v)} x2={W - PAD.right} y2={yMsg(v)}
            stroke={token.colorBorderSecondary} strokeDasharray={v === 0 ? undefined : '3 3'} />
        ))}
        <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={H - PAD.bottom} stroke={token.colorBorder} />
        <line x1={W - PAD.right} y1={PAD.top} x2={W - PAD.right} y2={H - PAD.bottom} stroke={KIB} />
        <line x1={PAD.left} y1={H - PAD.bottom} x2={W - PAD.right} y2={H - PAD.bottom} stroke={token.colorBorder} />
        <text x={PAD.left} y={10} fontSize={10} fill={MSG}>msg/s</text>
        <text x={W - PAD.right} y={10} fontSize={10} textAnchor="end" fill={KIB}>KiB/s</text>
        <polyline points={polyAt('totalKib', yKib)} fill="none" stroke={KIB} strokeWidth={1.25} strokeDasharray="5 4" />
        <polyline points={polyAt('kib', yKib)} fill="none" stroke={KIB} strokeWidth={1.5} />
        <polyline points={polyAt('totalMsg', yMsg)} fill="none" stroke={MSG} strokeWidth={1.25} strokeDasharray="5 4" />
        <polyline points={polyAt('msg', yMsg)} fill="none" stroke={MSG} strokeWidth={2} />
        <circle cx={xAt(n).toFixed(1)} cy={yMsg(last.msg).toFixed(1)} r={3} fill={MSG} />
        <circle cx={xAt(n).toFixed(1)} cy={yKib(last.kib).toFixed(1)} r={3} fill={KIB} />
        {hv !== null && hover !== null && (
          <g>
            <line x1={hover.x} y1={PAD.top} x2={hover.x} y2={H - PAD.bottom}
              stroke={token.colorTextSecondary} strokeDasharray="2 2" />
            <circle cx={xAt(hover.i)} cy={yMsg(hv.msg)} r={3} fill={MSG} />
            <circle cx={xAt(hover.i)} cy={yMsg(hv.totalMsg)} r={3} fill="none" stroke={MSG} />
            <circle cx={xAt(hover.i)} cy={yKib(hv.kib)} r={3} fill={KIB} />
            <circle cx={xAt(hover.i)} cy={yKib(hv.totalKib)} r={3} fill="none" stroke={KIB} />
            <rect x={boxX} y={boxY} width={boxW} height={boxH} rx={4}
              fill={token.colorBgElevated} stroke={token.colorBorder} />
            <text x={boxX + 8} y={boxY + 14} fontSize={10} fill={token.colorTextSecondary}>{fmtTime(hv.t)}</text>
            <text x={boxX + 8} y={boxY + 28} fontSize={10} fill={MSG}>{`客户端写入速度（数量） ${fmtRate(hv.msg)} msg/s`}</text>
            <text x={boxX + 8} y={boxY + 42} fontSize={10} fill={MSG}>{`总写入速度（数量） ${fmtRate(hv.totalMsg)} msg/s`}</text>
            <text x={boxX + 8} y={boxY + 56} fontSize={10} fill={KIB}>{`客户端写入速度（大小） ${fmtRate(hv.kib)} KiB/s`}</text>
            <text x={boxX + 8} y={boxY + 70} fontSize={10} fill={KIB}>{`总写入速度（大小） ${fmtRate(hv.totalKib)} KiB/s`}</text>
          </g>
        )}
        {xticks.map((i, k) => (
          <text key={i} x={xAt(i)} y={H - 8} fontSize={10} fill={token.colorTextSecondary}
            textAnchor={k === 0 ? 'start' : k === xticks.length - 1 ? 'end' : 'middle'}>
            {fmtTime(points[i].t)}
          </text>
        ))}
      </svg>
      {/* The legend: TWO columns (operator's call) — 数量 on the left, 大小 on
          the right, one metric per line inside each column: solid (client)
          above dashed (total), in the line's colour, mirroring the plot. */}
      <div style={{ display: 'flex', gap: 24, width: '100%' }}>
        <Space direction="vertical" size={2} style={{ flex: 1 }}>
          <Typography.Text style={{ color: MSG }}>{`客户端写入速度（数量）${fmtRate(last.msg)} msg/s`}</Typography.Text>
          <Typography.Text style={{ color: MSG, opacity: 0.75 }}>{`总写入速度（数量）${fmtRate(last.totalMsg)} msg/s`}</Typography.Text>
        </Space>
        <Space direction="vertical" size={2} style={{ flex: 1 }}>
          <Typography.Text style={{ color: KIB }}>{`客户端写入速度（大小）${fmtRate(last.kib)} KiB/s`}</Typography.Text>
          <Typography.Text style={{ color: KIB, opacity: 0.75 }}>{`总写入速度（大小）${fmtRate(last.totalKib)} KiB/s`}</Typography.Text>
        </Space>
      </div>
    </Space>
  )
}

export default NodeRateChart
