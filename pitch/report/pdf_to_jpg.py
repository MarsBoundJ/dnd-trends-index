#!/usr/bin/env python
"""Convert a PDF to per-page JPGs using PyMuPDF. Replaces pdftoppm on Windows."""
import sys
import os
import fitz

if len(sys.argv) < 2:
    print("Usage: python pdf_to_jpg.py <pdf> [dpi=150] [prefix=slide]")
    sys.exit(1)

pdf_path = sys.argv[1]
dpi = int(sys.argv[2]) if len(sys.argv) > 2 else 150
prefix = sys.argv[3] if len(sys.argv) > 3 else "slide"

doc = fitz.open(pdf_path)
n = len(doc)
# Match pdftoppm zero-padding behavior
if n < 10:
    pad = 1
elif n < 100:
    pad = 2
else:
    pad = 3

zoom = dpi / 72.0
mat = fitz.Matrix(zoom, zoom)

for i, page in enumerate(doc, start=1):
    pix = page.get_pixmap(matrix=mat)
    out = f"{prefix}-{str(i).zfill(pad)}.jpg"
    pix.save(out)
    print(os.path.abspath(out))
