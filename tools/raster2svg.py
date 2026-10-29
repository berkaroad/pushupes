#!/usr/bin/env python3
"""Raster -> vector for flat-colour marks (logos, icons).

    python3 raster2svg.py in.jpg out.svg [eps] [scale] [pad]

Pipeline: quantise onto the PALETTE below, clean each colour layer's mask
(median for JPEG specks, gaussian+threshold for the pixel staircase), extract
boundaries with marching squares at SCALE x (outer rings and holes in one pass),
simplify with Douglas-Peucker, emit one path per colour with fill-rule=evenodd,
and hug the mark with PAD units of white on every side.

Requires only Pillow. Fill PALETTE from a real measurement first:

    from PIL import Image
    im = Image.open(path).convert('RGB')
    for n, (r, g, b) in sorted(im.getcolors(1 << 22), reverse=True)[:14]:
        print('#%02x%02x%02x %5.2f%%' % (r, g, b, n * 100 / (im.size[0] * im.size[1])))
"""
import os
import sys

from PIL import Image, ImageFilter

PATH = sys.argv[1] if len(sys.argv) > 1 else "in.jpg"
OUT = sys.argv[2] if len(sys.argv) > 2 else "out.svg"
EPS = float(sys.argv[3]) if len(sys.argv) > 3 else 1.6
SCALE = int(sys.argv[4]) if len(sys.argv) > 4 else 2  # trace at SCALE x, emit at 1x
PAD = float(sys.argv[5]) if len(sys.argv) > 5 else 5.0  # white margin, viewBox units

# Ground first (it becomes the background rect), then darkest -> lightest, then
# the accent. Keep the list SHORT: every extra half-tone layer becomes fragments.
PALETTE = [
    (255, 255, 255),
    (38, 40, 44),
    (70, 84, 97),
    (110, 120, 132),
    (255, 115, 0),
]
# PALETTE_HEX overrides the defaults, ground colour first:
#   PALETTE_HEX="#ffffff,#35363a,#2c4d70,#3776b9,#ff7300" python3 raster2svg.py logo.png out.svg
# Measure the source before choosing (getcolors), never eyeball it.
if os.environ.get("PALETTE_HEX"):
    PALETTE = [
        tuple(int(h.strip().lstrip("#")[i:i + 2], 16) for i in (0, 2, 4))
        for h in os.environ["PALETTE_HEX"].split(",")
    ]


def quantize(im):
    pal = Image.new("P", (1, 1))
    flat = [c for col in PALETTE for c in col]
    pal.putpalette(flat + [0] * (768 - len(flat)))
    return im.quantize(palette=pal, dither=Image.Dither.NONE)


def marching_squares(rows, w, h, on_value):
    """Boundary segments of the pixels equal to on_value, on the half-pixel grid."""
    seg = {}
    for y in range(h - 1):
        r0, r1 = rows[y], rows[y + 1]
        for x in range(w - 1):
            a = r0[x] == on_value
            b = r0[x + 1] == on_value
            c = r1[x + 1] == on_value
            d = r1[x] == on_value
            case = (1 if a else 0) | (2 if b else 0) | (4 if c else 0) | (8 if d else 0)
            if case in (0, 15):
                continue
            top = (x + 0.5, y)
            right = (x + 1, y + 0.5)
            bottom = (x + 0.5, y + 1)
            left = (x, y + 0.5)
            pairs = {
                1: [(left, top)], 2: [(top, right)], 3: [(left, right)],
                4: [(right, bottom)], 5: [(left, top), (right, bottom)],
                6: [(top, bottom)], 7: [(left, bottom)], 8: [(bottom, left)],
                9: [(bottom, top)], 10: [(top, right), (bottom, left)],
                11: [(bottom, right)], 12: [(right, left)], 13: [(right, top)],
                14: [(top, left)],
            }[case]
            for p, q in pairs:
                seg.setdefault(p, []).append(q)
    return seg


def rings_from(seg):
    rings = []
    while seg:
        start = next(iter(seg))
        ring = [start]
        cur = start
        while True:
            nxts = seg.get(cur)
            if not nxts:
                break
            nxt = nxts.pop()
            if not nxts:
                del seg[cur]
            ring.append(nxt)
            cur = nxt
            if cur == start:
                break
        if len(ring) >= 4:
            rings.append(ring)
    return rings


def rdp(points, eps):
    """Douglas-Peucker on a closed ring, split at its two farthest points."""
    if len(points) < 3:
        return points
    ax, ay = points[0]
    bx, by = max(points, key=lambda p: (p[0] - ax) ** 2 + (p[1] - ay) ** 2)
    i = points.index((bx, by))
    return _rdp_open(points[:i + 1], eps)[:-1] + _rdp_open(points[i:] + [points[0]], eps)[:-1]


def _rdp_open(pts, eps):
    if len(pts) < 3:
        return pts
    (x1, y1), (x2, y2) = pts[0], pts[-1]
    dx, dy = x2 - x1, y2 - y1
    norm = (dx * dx + dy * dy) ** 0.5
    best, best_i = -1.0, 0
    for i in range(1, len(pts) - 1):
        px, py = pts[i]
        dist = abs(dy * (px - x1) - dx * (py - y1)) / norm if norm else ((px - x1) ** 2 + (py - y1) ** 2) ** 0.5
        if dist > best:
            best, best_i = dist, i
    if best <= eps:
        return [pts[0], pts[-1]]
    return _rdp_open(pts[:best_i + 1], eps)[:-1] + _rdp_open(pts[best_i:], eps)


def ring_area(r):
    s = 0.0
    for i in range(len(r)):
        x1, y1 = r[i]
        x2, y2 = r[(i + 1) % len(r)]
        s += x1 * y2 - x2 * y1
    return s / 2.0


def main():
    im = Image.open(PATH).convert("RGB")
    vw, vh = im.size
    if SCALE > 1:
        im = im.resize((vw * SCALE, vh * SCALE), Image.LANCZOS)
    w, h = im.size
    q = quantize(im)
    raw = q.tobytes()  # palette indices ARE the layer ids

    layers = []
    for idx in range(1, len(PALETTE)):
        layer = Image.frombytes("L", (w, h), raw).point(lambda v, i=idx: 255 if v == i else 0)
        layer = layer.filter(ImageFilter.MedianFilter(3))
        layer = layer.filter(ImageFilter.GaussianBlur(1.1)).point(lambda v: 255 if v >= 128 else 0)
        src = layer.tobytes()
        rings = rings_from(marching_squares([src[y * w:(y + 1) * w] for y in range(h)], w, h, 255))
        kept = []
        for r in rings:
            xs = [p[0] for p in r]
            ys = [p[1] for p in r]
            if abs(ring_area(r)) < 60 or (max(xs) - min(xs) < 8 and max(ys) - min(ys) < 8):
                continue
            kept.append(rdp(r, EPS))
        if kept:
            layers.append((idx, kept))
        print("  layer %d: rings=%d points=%d" % (idx, len(kept), sum(len(k) for k in kept)))

    xs = [p[0] / SCALE for _, kept in layers for r in kept for p in r]
    ys = [p[1] / SCALE for _, kept in layers for r in kept for p in r]
    x0, y0 = min(xs) - PAD, min(ys) - PAD
    bw, bh = (max(xs) + PAD) - x0, (max(ys) + PAD) - y0
    bg = PALETTE[0]
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="%.2f %.2f %.2f %.2f" width="%.0f" height="%.0f">' % (x0, y0, bw, bh, bw, bh),
        '  <rect x="%.2f" y="%.2f" width="%.2f" height="%.2f" fill="rgb(%d,%d,%d)"/>' % (x0, y0, bw, bh, *bg),
    ]
    for idx, kept in layers:
        c = PALETTE[idx]
        d = []
        for r in kept:
            d.append("M" + " L".join("%.2f,%.2f" % (x / SCALE, y / SCALE) for x, y in r) + " Z")
        parts.append('  <path fill="rgb(%d,%d,%d)" fill-rule="evenodd" d="%s"/>' % (*c, " ".join(d)))
    parts.append("</svg>")
    svg = "\n".join(parts) + "\n"
    open(OUT, "w").write(svg)
    print("wrote %s: %d bytes, %d layers, viewBox %.0fx%.0f" % (OUT, len(svg), len(layers), bw, bh))


main()
