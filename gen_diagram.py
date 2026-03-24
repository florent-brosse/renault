#!/usr/bin/env python3
"""
Generate Renault/Databricks architecture diagram as PNG using Python stdlib only.
"""

import struct
import zlib
import math

WIDTH  = 2800
HEIGHT = 1650
BG     = (245, 247, 250)

# Palette
DB_BLUE        = (28,  96, 181)
DB_BLUE_DARK   = (15,  60, 130)
DB_BLUE_LIGHT  = (210, 228, 255)
DB_ORANGE      = (210,  90,  20)
SILVER_BG      = (230, 240, 255)
GOLD_BG        = (255, 243, 200)
BRONZE_BG      = (248, 220, 180)
GREEN_DARK     = ( 30, 120,  30)
GREEN_BG       = (218, 245, 218)
PURPLE_BG      = (238, 222, 255)
PURPLE_DARK    = ( 95,  45, 175)
GREY_BORDER    = (155, 165, 180)
WHITE          = (255, 255, 255)
BLACK          = (  0,   0,   0)
ORANGE_LIGHT   = (255, 235, 190)

pixels = [[list(BG) for _ in range(WIDTH)] for _ in range(HEIGHT)]

def clamp(v): return max(0, min(255, int(v)))

def blend(dst, src, alpha):
    a = alpha / 255.0
    return [clamp(dst[i] * (1 - a) + src[i] * a) for i in range(3)]

def put(x, y, color, alpha=255):
    if 0 <= x < WIDTH and 0 <= y < HEIGHT:
        if alpha >= 255:
            pixels[y][x] = list(color)
        else:
            pixels[y][x] = blend(pixels[y][x], color, alpha)

def fill_rect(x1, y1, x2, y2, color):
    for y in range(max(0, y1), min(HEIGHT, y2)):
        for x in range(max(0, x1), min(WIDTH, x2)):
            pixels[y][x] = list(color)

def fill_rounded_rect(x1, y1, x2, y2, r, color):
    r = min(r, (x2-x1)//2, (y2-y1)//2)
    fill_rect(x1 + r, y1, x2 - r, y2, color)
    fill_rect(x1, y1 + r, x1 + r, y2 - r, color)
    fill_rect(x2 - r, y1 + r, x2, y2 - r, color)
    for dy in range(r + 1):
        for dx in range(r + 1):
            if math.sqrt(dx*dx + dy*dy) <= r:
                put(x1+r-dx, y1+r-dy, color)
                put(x2-r+dx, y1+r-dy, color)
                put(x1+r-dx, y2-r+dy, color)
                put(x2-r+dx, y2-r+dy, color)

def stroke_rounded_rect(x1, y1, x2, y2, r, color, thickness=2):
    r = min(r, (x2-x1)//2, (y2-y1)//2)
    for t in range(thickness):
        r2 = max(0, r - t)
        for x in range(x1+r, x2-r+1):
            put(x, y1+t, color)
            put(x, y2-t, color)
        for y in range(y1+r, y2-r+1):
            put(x1+t, y, color)
            put(x2-t, y, color)
        for angle_deg in range(0, 91):
            angle = math.radians(angle_deg)
            dx = int(r2 * math.cos(angle))
            dy = int(r2 * math.sin(angle))
            put(x2-r+dx, y1+r-dy, color)
            put(x1+r-dx, y1+r-dy, color)
            put(x2-r+dx, y2-r+dy, color)
            put(x1+r-dx, y2-r+dy, color)

def draw_line(x1, y1, x2, y2, color, thickness=2):
    dx = abs(x2-x1); dy = abs(y2-y1)
    sx = 1 if x1 < x2 else -1
    sy = 1 if y1 < y2 else -1
    err = dx - dy
    cx, cy = x1, y1
    while True:
        for t in range(-(thickness//2), thickness//2+1):
            if dx >= dy: put(cx, cy+t, color)
            else:        put(cx+t, cy, color)
        if cx == x2 and cy == y2: break
        e2 = 2*err
        if e2 > -dy: err -= dy; cx += sx
        if e2 < dx:  err += dx; cy += sy

def draw_dashed_line(x1, y1, x2, y2, color, thickness=2, dash=14, gap=8):
    length = math.sqrt((x2-x1)**2 + (y2-y1)**2)
    if length == 0: return
    steps = int(length)
    draw = True; count = 0
    for i in range(steps+1):
        t = i/steps
        x = int(x1 + t*(x2-x1))
        y = int(y1 + t*(y2-y1))
        if draw:
            dx = abs(x2-x1); dy2 = abs(y2-y1)
            for dt in range(-(thickness//2), thickness//2+1):
                if dx >= dy2: put(x, y+dt, color)
                else:         put(x+dt, y, color)
        count += 1
        if draw and count >= dash:   draw = False; count = 0
        elif not draw and count >= gap: draw = True;  count = 0

def draw_arrowhead(x2, y2, angle, color, thickness=3, head_size=16):
    for side in [-1, 1]:
        hx = int(x2 - head_size * math.cos(angle - side*math.pi/6))
        hy = int(y2 - head_size * math.sin(angle - side*math.pi/6))
        draw_line(x2, y2, hx, hy, color, thickness)

def draw_arrow(x1, y1, x2, y2, color, thickness=3, head_size=16):
    draw_line(x1, y1, x2, y2, color, thickness)
    angle = math.atan2(y2-y1, x2-x1)
    draw_arrowhead(x2, y2, angle, color, thickness, head_size)

def draw_dashed_arrow(x1, y1, x2, y2, color, thickness=2, head_size=14):
    draw_dashed_line(x1, y1, x2, y2, color, thickness)
    angle = math.atan2(y2-y1, x2-x1)
    draw_arrowhead(x2, y2, angle, color, thickness, head_size)

# Elbow arrow: goes horizontal then vertical (or vice versa)
def draw_elbow_arrow(x1, y1, x2, y2, color, thickness=3, head_size=16, horiz_first=True):
    if horiz_first:
        draw_line(x1, y1, x2, y1, color, thickness)
        draw_arrow(x2, y1, x2, y2, color, thickness, head_size)
    else:
        draw_line(x1, y1, x1, y2, color, thickness)
        draw_arrow(x1, y2, x2, y2, color, thickness, head_size)

def draw_route_arrow(points, color, thickness=3, head_size=16):
    """Draw a polyline arrow (last segment gets arrowhead)."""
    for i in range(len(points)-1):
        x1,y1 = points[i]
        x2,y2 = points[i+1]
        if i < len(points)-2:
            draw_line(x1,y1,x2,y2,color,thickness)
        else:
            draw_arrow(x1,y1,x2,y2,color,thickness,head_size)

# Bitmap font (5x7)
GLYPHS = {
    ' ': [0,0,0,0,0,0,0],
    'A': [0b01110,0b10001,0b10001,0b11111,0b10001,0b10001,0b10001],
    'B': [0b11110,0b10001,0b10001,0b11110,0b10001,0b10001,0b11110],
    'C': [0b01110,0b10001,0b10000,0b10000,0b10000,0b10001,0b01110],
    'D': [0b11100,0b10010,0b10001,0b10001,0b10001,0b10010,0b11100],
    'E': [0b11111,0b10000,0b10000,0b11110,0b10000,0b10000,0b11111],
    'F': [0b11111,0b10000,0b10000,0b11110,0b10000,0b10000,0b10000],
    'G': [0b01110,0b10001,0b10000,0b10111,0b10001,0b10001,0b01111],
    'H': [0b10001,0b10001,0b10001,0b11111,0b10001,0b10001,0b10001],
    'I': [0b11111,0b00100,0b00100,0b00100,0b00100,0b00100,0b11111],
    'J': [0b11111,0b00010,0b00010,0b00010,0b00010,0b10010,0b01100],
    'K': [0b10001,0b10010,0b10100,0b11000,0b10100,0b10010,0b10001],
    'L': [0b10000,0b10000,0b10000,0b10000,0b10000,0b10000,0b11111],
    'M': [0b10001,0b11011,0b10101,0b10001,0b10001,0b10001,0b10001],
    'N': [0b10001,0b11001,0b10101,0b10011,0b10001,0b10001,0b10001],
    'O': [0b01110,0b10001,0b10001,0b10001,0b10001,0b10001,0b01110],
    'P': [0b11110,0b10001,0b10001,0b11110,0b10000,0b10000,0b10000],
    'Q': [0b01110,0b10001,0b10001,0b10001,0b10101,0b10010,0b01101],
    'R': [0b11110,0b10001,0b10001,0b11110,0b10100,0b10010,0b10001],
    'S': [0b01111,0b10000,0b10000,0b01110,0b00001,0b00001,0b11110],
    'T': [0b11111,0b00100,0b00100,0b00100,0b00100,0b00100,0b00100],
    'U': [0b10001,0b10001,0b10001,0b10001,0b10001,0b10001,0b01110],
    'V': [0b10001,0b10001,0b10001,0b10001,0b10001,0b01010,0b00100],
    'W': [0b10001,0b10001,0b10001,0b10101,0b10101,0b11011,0b10001],
    'X': [0b10001,0b10001,0b01010,0b00100,0b01010,0b10001,0b10001],
    'Y': [0b10001,0b10001,0b01010,0b00100,0b00100,0b00100,0b00100],
    'Z': [0b11111,0b00001,0b00010,0b00100,0b01000,0b10000,0b11111],
    'a': [0,0,0b01110,0b00001,0b01111,0b10001,0b01111],
    'b': [0b10000,0b10000,0b11110,0b10001,0b10001,0b10001,0b11110],
    'c': [0,0,0b01110,0b10000,0b10000,0b10001,0b01110],
    'd': [0b00001,0b00001,0b01111,0b10001,0b10001,0b10001,0b01111],
    'e': [0,0,0b01110,0b10001,0b11111,0b10000,0b01110],
    'f': [0b00110,0b01001,0b01000,0b11100,0b01000,0b01000,0b01000],
    'g': [0,0,0b01111,0b10001,0b01111,0b00001,0b01110],
    'h': [0b10000,0b10000,0b11110,0b10001,0b10001,0b10001,0b10001],
    'i': [0,0b00100,0,0b00100,0b00100,0b00100,0b00110],
    'j': [0,0b00010,0,0b00010,0b00010,0b10010,0b01100],
    'k': [0b10000,0b10000,0b10010,0b10100,0b11000,0b10100,0b10010],
    'l': [0b01100,0b00100,0b00100,0b00100,0b00100,0b00100,0b01110],
    'm': [0,0,0b11010,0b10101,0b10101,0b10001,0b10001],
    'n': [0,0,0b11110,0b10001,0b10001,0b10001,0b10001],
    'o': [0,0,0b01110,0b10001,0b10001,0b10001,0b01110],
    'p': [0,0,0b11110,0b10001,0b11110,0b10000,0b10000],
    'q': [0,0,0b01111,0b10001,0b01111,0b00001,0b00001],
    'r': [0,0,0b10110,0b11001,0b10000,0b10000,0b10000],
    's': [0,0,0b01110,0b10000,0b01110,0b00001,0b11110],
    't': [0b01000,0b01000,0b11110,0b01000,0b01000,0b01001,0b00110],
    'u': [0,0,0b10001,0b10001,0b10001,0b10011,0b01101],
    'v': [0,0,0b10001,0b10001,0b10001,0b01010,0b00100],
    'w': [0,0,0b10001,0b10001,0b10101,0b10101,0b01010],
    'x': [0,0,0b10001,0b01010,0b00100,0b01010,0b10001],
    'y': [0,0,0b10001,0b10001,0b01111,0b00001,0b01110],
    'z': [0,0,0b11111,0b00010,0b00100,0b01000,0b11111],
    '0': [0b01110,0b10011,0b10101,0b10101,0b11001,0b10001,0b01110],
    '1': [0b00100,0b01100,0b00100,0b00100,0b00100,0b00100,0b01110],
    '2': [0b01110,0b10001,0b00001,0b00110,0b01000,0b10000,0b11111],
    '3': [0b11111,0b00010,0b00100,0b00110,0b00001,0b10001,0b01110],
    '4': [0b00010,0b00110,0b01010,0b10010,0b11111,0b00010,0b00010],
    '5': [0b11111,0b10000,0b11110,0b00001,0b00001,0b10001,0b01110],
    '6': [0b01110,0b10000,0b10000,0b11110,0b10001,0b10001,0b01110],
    '7': [0b11111,0b00001,0b00010,0b00100,0b01000,0b01000,0b01000],
    '8': [0b01110,0b10001,0b10001,0b01110,0b10001,0b10001,0b01110],
    '9': [0b01110,0b10001,0b10001,0b01111,0b00001,0b00001,0b01110],
    '+': [0,0b00100,0b00100,0b11111,0b00100,0b00100,0],
    '-': [0,0,0,0b11111,0,0,0],
    '/': [0b00001,0b00010,0b00100,0b01000,0b10000,0,0],
    '\\': [0b10000,0b01000,0b00100,0b00010,0b00001,0,0],
    '.': [0,0,0,0,0,0b00110,0b00110],
    ',': [0,0,0,0,0b00100,0b00100,0b01000],
    ':': [0,0b00110,0b00110,0,0b00110,0b00110,0],
    '(': [0b00010,0b00100,0b01000,0b01000,0b01000,0b00100,0b00010],
    ')': [0b01000,0b00100,0b00010,0b00010,0b00010,0b00100,0b01000],
    '!': [0b00100,0b00100,0b00100,0b00100,0b00100,0,0b00100],
    '?': [0b01110,0b10001,0b00001,0b00110,0b00100,0,0b00100],
    '_': [0,0,0,0,0,0,0b11111],
    '#': [0b01010,0b01010,0b11111,0b01010,0b11111,0b01010,0b01010],
    '>': [0b10000,0b01000,0b00100,0b00010,0b00100,0b01000,0b10000],
    '<': [0b00001,0b00010,0b00100,0b01000,0b00100,0b00010,0b00001],
    '&': [0b01100,0b10010,0b10100,0b01000,0b10100,0b10010,0b01101],
}
GLYPH_W = 5; GLYPH_H = 7; GLYPH_SP = 1

def draw_char(ch, cx, cy, color, scale=1):
    g = GLYPHS.get(ch, GLYPHS.get(ch.upper(), [0]*7))
    for row in range(GLYPH_H):
        bits = g[row]
        for col in range(GLYPH_W):
            if bits & (1 << (GLYPH_W-1-col)):
                for sy in range(scale):
                    for sx in range(scale):
                        put(cx+col*scale+sx, cy+row*scale+sy, color)

def text_width(text, scale=1):
    return len(text) * (GLYPH_W+GLYPH_SP) * scale

def draw_text(text, x, y, color, scale=1, center=False):
    w = text_width(text, scale)
    if center: x = x - w//2
    for i, ch in enumerate(text):
        draw_char(ch, x + i*(GLYPH_W+GLYPH_SP)*scale, y, color, scale)

def draw_text_c(text, cx, cy, color, scale=1):
    draw_text(text, cx, cy, color, scale=scale, center=True)

def draw_multiline(lines, cx, cy, color, scale=1, line_spacing=None):
    if line_spacing is None: line_spacing = (GLYPH_H+3)*scale
    total_h = len(lines)*line_spacing
    start_y = cy - total_h//2
    for i, line in enumerate(lines):
        draw_text_c(line, cx, start_y + i*line_spacing, color, scale)

def pill_label(text, cx, cy, color, bg, scale=1, pad_x=10, pad_y=5):
    w = text_width(text, scale) + pad_x*2
    h = GLYPH_H*scale + pad_y*2
    fill_rounded_rect(cx-w//2, cy-h//2, cx+w//2, cy+h//2, 6, bg)
    stroke_rounded_rect(cx-w//2, cy-h//2, cx+w//2, cy+h//2, 6, color, 1)
    draw_text_c(text, cx, cy - GLYPH_H*scale//2 + pad_y//2, color, scale)

def box(x1, y1, x2, y2, fill, border, lines=None,
        lcolor=WHITE, lscale=2, corner=10, bthick=2,
        sub=None, scolor=None, sscale=1):
    fill_rounded_rect(x1, y1, x2, y2, corner, fill)
    stroke_rounded_rect(x1, y1, x2, y2, corner, border, bthick)
    cx = (x1+x2)//2; cy = (y1+y2)//2
    if lines:
        draw_multiline(lines, cx, cy, lcolor, scale=lscale)
    if sub:
        sc = scolor or lcolor
        off = len(lines)*(GLYPH_H+3)*lscale//2 + 10 if lines else 0
        draw_text_c(sub, cx, cy+off, sc, scale=sscale)

def dashed_box(x1, y1, x2, y2, color, fill=None):
    if fill:
        fill_rounded_rect(x1, y1, x2, y2, 10, fill)
    draw_dashed_line(x1, y1, x2, y1, color, 2)
    draw_dashed_line(x2, y1, x2, y2, color, 2)
    draw_dashed_line(x1, y2, x2, y2, color, 2)
    draw_dashed_line(x1, y1, x1, y2, color, 2)

# ─────────────────────── Layout ───────────────────────────────────────────────
# Three columns
SRC_CX  = 175
MID_X1  = 350
MID_X2  = 1800
CON_X1  = 1860
CON_X2  = 2680

TITLE_H = 82
SECT_Y1 = TITLE_H + 10

# Title bar
fill_rect(0, 0, WIDTH, TITLE_H, DB_BLUE_DARK)
draw_text_c("Renault Car Sales  -  Databricks Lakehouse Architecture",
            WIDTH//2, 22, WHITE, scale=3)

# Section backgrounds
fill_rounded_rect(30, SECT_Y1, 310, HEIGHT-80, 12, (235,240,250))
stroke_rounded_rect(30, SECT_Y1, 310, HEIGHT-80, 12, GREY_BORDER, 1)

fill_rounded_rect(MID_X1, SECT_Y1, MID_X2, HEIGHT-80, 12, (228,238,255))
stroke_rounded_rect(MID_X1, SECT_Y1, MID_X2, HEIGHT-80, 12, DB_BLUE, 2)

fill_rounded_rect(CON_X1, SECT_Y1, CON_X2, HEIGHT-80, 12, (235,248,240))
stroke_rounded_rect(CON_X1, SECT_Y1, CON_X2, HEIGHT-80, 12, GREEN_DARK, 1)

draw_text_c("Data Sources",        SRC_CX,              SECT_Y1+18, DB_BLUE_DARK, scale=2)
draw_text_c("Databricks Lakehouse",(MID_X1+MID_X2)//2,  SECT_Y1+18, DB_BLUE_DARK, scale=2)
draw_text_c("Consumption",         (CON_X1+CON_X2)//2,  SECT_Y1+18, GREEN_DARK,   scale=2)

# ─────────── Unity Catalog wrapper ───────────────────────────────────────────
UC_X1 = MID_X1+16; UC_Y1 = SECT_Y1+40
UC_X2 = MID_X2-16; UC_Y2 = HEIGHT-150
fill_rounded_rect(UC_X1, UC_Y1, UC_X2, UC_Y2, 14, (218,232,255))
stroke_rounded_rect(UC_X1, UC_Y1, UC_X2, UC_Y2, 14, DB_BLUE, 3)
draw_text_c("Unity Catalog",        (UC_X1+UC_X2)//2, UC_Y1+22, DB_BLUE_DARK,  scale=2)
draw_text_c("Row-Level Security (RLS) by concession group",
            (UC_X1+UC_X2)//2, UC_Y1+46, DB_BLUE, scale=1)

# ─────────── Autoloader ──────────────────────────────────────────────────────
AL_X1 = UC_X1+30; AL_Y1 = UC_Y1+70
AL_X2 = AL_X1+230; AL_Y2 = AL_Y1+110
box(AL_X1, AL_Y1, AL_X2, AL_Y2, DB_BLUE, DB_BLUE_DARK,
    lines=["Autoloader"], lcolor=WHITE, lscale=2, corner=8, bthick=2,
    sub="(GCS ingest)", scolor=(190,215,255), sscale=1)

# ─────────── DP wrapper ───────────────────────────────────────────────────────
DP_X1 = UC_X1+20; DP_Y1 = AL_Y2+30
DP_X2 = UC_X2-20; DP_Y2 = UC_Y2-20
fill_rounded_rect(DP_X1, DP_Y1, DP_X2, DP_Y2, 12, (212,228,255))
stroke_rounded_rect(DP_X1, DP_Y1, DP_X2, DP_Y2, 12, DB_BLUE, 2)
draw_text_c("Declarative Pipeline (DP)", (DP_X1+DP_X2)//2, DP_Y1+22, DB_BLUE_DARK, scale=2)

# Layer geometry
LX1 = DP_X1+20; LX2 = DP_X2-20
available = DP_Y2 - DP_Y1 - 60
LAYER_H = (available - 40) // 3
PAD = 20
BR_Y1 = DP_Y1+46
BR_Y2 = BR_Y1+LAYER_H
SI_Y1 = BR_Y2+PAD
SI_Y2 = SI_Y1+LAYER_H
GO_Y1 = SI_Y2+PAD
GO_Y2 = GO_Y1+LAYER_H

# Bronze
box(LX1, BR_Y1, LX2, BR_Y2, BRONZE_BG, (175,110,45),
    lines=["Bronze"], lcolor=(90,50,10), lscale=3, corner=8,
    sub="Raw ingestion  -  Streaming Tables", scolor=(110,65,15), sscale=1)

# Silver
box(LX1, SI_Y1, LX2, SI_Y2, SILVER_BG, (70,115,180),
    lines=["Silver"], lcolor=(15,50,120), lscale=3, corner=8,
    sub="Cleansed, typed, validated  -  Streaming Tables", scolor=(20,60,140), sscale=1)

# Gold
box(LX1, GO_Y1, LX2, GO_Y2, GOLD_BG, (155,115,0),
    lines=["Gold"], lcolor=(90,65,0), lscale=3, corner=8,
    sub="Business aggregations  -  Materialized Views", scolor=(115,80,0), sscale=1)

# ─────────── Data Sources ────────────────────────────────────────────────────
GCS_X1 = 50; GCS_X2 = 300
GCS_Y1 = SECT_Y1+60; GCS_Y2 = GCS_Y1+250
box(GCS_X1, GCS_Y1, GCS_X2, GCS_Y2, DB_BLUE_LIGHT, DB_BLUE,
    lines=["GCS","Landing","Zone"], lcolor=DB_BLUE_DARK, lscale=2, corner=8,
    sub="(UC Volume)", scolor=DB_BLUE, sscale=1)
draw_text_c("CSV: history",          SRC_CX, GCS_Y2+20, (70,70,100), scale=1)
draw_text_c("+ daily incremental",   SRC_CX, GCS_Y2+36, (70,70,100), scale=1)

BQ_X1 = 50; BQ_X2 = 300
BQ_Y1 = GCS_Y2+110; BQ_Y2 = BQ_Y1+160
dashed_box(BQ_X1, BQ_Y1, BQ_X2, BQ_Y2, GREY_BORDER, fill=(245,245,255))
draw_text_c("BigQuery",  SRC_CX, (BQ_Y1+BQ_Y2)//2 - 12, (70,70,180), scale=2)
draw_text_c("(future)",  SRC_CX, (BQ_Y1+BQ_Y2)//2 + 14, GREY_BORDER, scale=1)

# ─────────── Consumption boxes ───────────────────────────────────────────────
CON_CX  = (CON_X1+CON_X2)//2
CON_PAD = 20

# AI/BI
AIBI_Y1 = SECT_Y1+60; AIBI_Y2 = AIBI_Y1+200
box(CON_X1+CON_PAD, AIBI_Y1, CON_X2-CON_PAD, AIBI_Y2,
    GREEN_BG, GREEN_DARK,
    lines=["AI/BI Dashboard"], lcolor=GREEN_DARK, lscale=2, corner=8,
    sub="No extra license  -  SSO", scolor=(40,120,40), sscale=1)

# Genie
GEN_Y1 = AIBI_Y2+30; GEN_Y2 = GEN_Y1+200
box(CON_X1+CON_PAD, GEN_Y1, CON_X2-CON_PAD, GEN_Y2,
    PURPLE_BG, PURPLE_DARK,
    lines=["Genie Space"], lcolor=PURPLE_DARK, lscale=2, corner=8,
    sub="Natural language analytics", scolor=PURPLE_DARK, sscale=1)

# Lakebase
LB_Y1 = GEN_Y2+30; LB_Y2 = LB_Y1+200
box(CON_X1+CON_PAD, LB_Y1, CON_X2-CON_PAD, LB_Y2,
    ORANGE_LIGHT, DB_ORANGE,
    lines=["Lakebase (Postgres)"], lcolor=(100,50,0), lscale=2, corner=8,
    sub="REST API exposure", scolor=DB_ORANGE, sscale=1)

# REST API label inside the Lakebase box (bottom right corner)
REST_Y = (LB_Y1+LB_Y2)//2 + 30

# Databricks App (optional, dashed)
APP_Y1 = LB_Y2+40; APP_Y2 = APP_Y1+180
dashed_box(CON_X1+CON_PAD, APP_Y1, CON_X2-CON_PAD, APP_Y2, PURPLE_DARK,
           fill=(248,240,255))
draw_text_c("Databricks App",   CON_CX, (APP_Y1+APP_Y2)//2-18, PURPLE_DARK, scale=2)
draw_text_c("(optional)",       CON_CX, (APP_Y1+APP_Y2)//2+4,  PURPLE_DARK, scale=1)
draw_text_c("Update reference tables", CON_CX, (APP_Y1+APP_Y2)//2+24, GREY_BORDER, scale=1)

# ─────────── Arrows ───────────────────────────────────────────────────────────
AL_MID_X = (AL_X1+AL_X2)//2
AL_MID_Y = (AL_Y1+AL_Y2)//2
AL_BOT   = AL_Y2
BR_TOP   = BR_Y1
LAYER_CX = (LX1+LX2)//2

# GCS → Autoloader (horizontal)
GCS_MID_Y = (GCS_Y1+GCS_Y2)//2
draw_arrow(GCS_X2, GCS_MID_Y, AL_X1, AL_MID_Y, DB_BLUE, thickness=3)
pill_label("CSV files", (GCS_X2+AL_X1)//2, GCS_MID_Y-22, DB_BLUE_DARK, DB_BLUE_LIGHT, scale=1)

# Autoloader → Bronze (vertical, centered)
draw_route_arrow([(AL_MID_X, AL_BOT), (AL_MID_X, BR_Y1-2)], DB_BLUE, thickness=3)

# Bronze → Silver
draw_arrow(LAYER_CX, BR_Y2, LAYER_CX, SI_Y1, DB_BLUE_DARK, thickness=3)

# Silver → Gold
draw_arrow(LAYER_CX, SI_Y2, LAYER_CX, GO_Y1, DB_BLUE_DARK, thickness=3)

# Gold outputs: vertical trunk between Lakehouse and Consumption columns
GOLD_MID_Y = (GO_Y1+GO_Y2)//2
TRUNK_X    = CON_X1 - 35   # vertical trunk channel

AIBI_MID_Y = (AIBI_Y1+AIBI_Y2)//2
GEN_MID_Y  = (GEN_Y1+GEN_Y2)//2
LB_MID_Y   = (LB_Y1+LB_Y2)//2

# Horizontal line from Gold right edge to trunk
draw_line(LX2, GOLD_MID_Y, TRUNK_X, GOLD_MID_Y, DB_BLUE_DARK, 3)

# Vertical trunk covering all consumption targets
draw_line(TRUNK_X, AIBI_MID_Y, TRUNK_X, LB_MID_Y, DB_BLUE_DARK, 3)

# Arrows from trunk to each consumption box
draw_arrow(TRUNK_X, AIBI_MID_Y, CON_X1+CON_PAD, AIBI_MID_Y, GREEN_DARK, 3)
draw_arrow(TRUNK_X, GEN_MID_Y,  CON_X1+CON_PAD, GEN_MID_Y,  PURPLE_DARK, 3)
draw_arrow(TRUNK_X, LB_MID_Y,   CON_X1+CON_PAD, LB_MID_Y,   DB_ORANGE, 3)

# pill labels on each branch
pill_label("Gold", TRUNK_X+38, AIBI_MID_Y-22, GREEN_DARK,   GREEN_BG,     scale=1)
pill_label("Gold", TRUNK_X+38, GEN_MID_Y -22, PURPLE_DARK,  PURPLE_BG,    scale=1)
pill_label("Gold", TRUNK_X+38, LB_MID_Y  -22, DB_ORANGE,    ORANGE_LIGHT, scale=1)

# Sync back: Lakebase → sync to Delta → Unity Catalog
# Route: down from Lakebase bottom, left at SYNC_Y level, up into the DP Gold right edge
SYNC_Y   = GO_Y2 + 60
SYNC_X_R = (CON_X1 + CON_X2) // 2   # center of consumption column
SYNC_X_L = LX2 - 80                  # arrive at right side of Gold layer

draw_line(SYNC_X_R, LB_Y2, SYNC_X_R, SYNC_Y, (50,100,200), 2)
draw_line(SYNC_X_R, SYNC_Y, SYNC_X_L, SYNC_Y, (50,100,200), 2)
draw_arrow(SYNC_X_L, SYNC_Y, SYNC_X_L, GO_Y2+2, (50,100,200), 3)
pill_label("sync to Delta", (SYNC_X_R+SYNC_X_L)//2, SYNC_Y-20, DB_BLUE_DARK, DB_BLUE_LIGHT, scale=1)

# App → Lakebase (CRUD)
APP_MID_Y = (APP_Y1+APP_Y2)//2
draw_arrow(CON_CX, APP_Y1, CON_CX, LB_Y2, PURPLE_DARK, 2)
pill_label("CRUD ref tables", CON_CX+90, (APP_Y1+LB_Y2)//2, PURPLE_DARK, PURPLE_BG, scale=1)

# BigQuery → UC (dashed future)
BQ_MID_Y = (BQ_Y1+BQ_Y2)//2
draw_dashed_arrow(BQ_X2, BQ_MID_Y, UC_X1+20, BQ_MID_Y, GREY_BORDER, thickness=2)
pill_label("Federation - later", (BQ_X2+UC_X1)//2, BQ_MID_Y-20, GREY_BORDER, (245,245,252), scale=1)

# ─────────── Bottom annotation ────────────────────────────────────────────────
A_Y1 = HEIGHT-78; A_Y2 = HEIGHT-10
fill_rounded_rect(40, A_Y1, WIDTH-40, A_Y2, 12, (255,245,210))
stroke_rounded_rect(40, A_Y1, WIDTH-40, A_Y2, 12, DB_ORANGE, 2)
CY_A = (A_Y1+A_Y2)//2
draw_text_c("Replaces:  6 GCP Projects  +  NiFi  +  Cloud SQL  +  K8s App",
            WIDTH//2, CY_A-16, (110,55,0), scale=2)
# arrow then text
ANN_CX = WIDTH//2
atext = "1 Databricks Workspace"
atw   = text_width(atext, 2)
ax1   = ANN_CX - atw//2 - 40
ax2   = ax1 + 28
draw_arrow(ax1, CY_A+16, ax2, CY_A+16, DB_ORANGE, 3, 10)
draw_text(atext, ax2+10, CY_A+9, DB_ORANGE, scale=2)

# ─────────── PNG output ───────────────────────────────────────────────────────
def write_png(filepath, width, height, pixel_rows):
    def chunk(ctype, data):
        raw = ctype + data
        return struct.pack('>I', len(data)) + raw + struct.pack('>I', zlib.crc32(raw)&0xffffffff)
    ihdr = struct.pack('>IIBBBBB', width, height, 8, 2, 0, 0, 0)
    raw  = b''
    for row in pixel_rows:
        raw += b'\x00'
        for p in row:
            raw += bytes([clamp(p[0]), clamp(p[1]), clamp(p[2])])
    with open(filepath, 'wb') as f:
        f.write(b'\x89PNG\r\n\x1a\n')
        f.write(chunk(b'IHDR', ihdr))
        f.write(chunk(b'IDAT', zlib.compress(raw, 9)))
        f.write(chunk(b'IEND', b''))

OUT = '/Users/florent.brosse/demo/renault/architecture.png'
write_png(OUT, WIDTH, HEIGHT, pixels)
print(f"Saved: {OUT}  ({WIDTH}x{HEIGHT})")
