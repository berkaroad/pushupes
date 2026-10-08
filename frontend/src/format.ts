// humanBytesPerSec renders a byte RATE the same way humanBytes renders a
// size, with the "/s" suffix: the cluster page's write-size card shows it
// ("100 KiB/s") beside the message-rate card.
export function humanBytesPerSec(n: number): string {
  if (!Number.isFinite(n) || n < 0) return '-'
  return `${humanBytes(n)}/s`
}

// humanBytes renders a byte count for display (callers put the exact value in
// a title/tooltip). Shared by the cluster page's storage total and the slots
// page's per-slot bytes so both read the same way.
export function humanBytes(n: number): string {
  if (!Number.isFinite(n) || n < 0) return '-'
  if (n < 1024) return `${n} B`
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[i]}`
}
