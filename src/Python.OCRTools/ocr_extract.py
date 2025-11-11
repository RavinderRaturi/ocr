#!/usr/bin/env python3
# ocr_extract.py
# Rasterize PDF -> run Tesseract (eng+hin) -> emit JSONL with blocks
# Usage example:
#   python ocr_extract.py --pdf ../../sample_data/sample1.pdf --out ../../sample_data/sample1_blocks.jsonl

import os
import sys
import json
import argparse
from pathlib import Path
from pdf2image import convert_from_path
from PIL import Image, ImageOps, ImageFilter
import pytesseract

def preprocess_image(pil_img, deskew=False, denoise=False, binarize=False):
    img = pil_img.convert("L")  # grayscale
    if denoise:
        img = img.filter(ImageFilter.MedianFilter(size=3))
    if binarize:
        # adaptive-ish binarization with ImageOps
        img = ImageOps.autocontrast(img)
        img = img.point(lambda p: 255 if p > 180 else 0)
    # deskew placeholder. Tesseract's page segmentation often handles mild skew.
    # For heavy skew consider OpenCV based deskewing later.
    return img

def page_to_blocks(pil_img, page_no, tesseract_lang="eng+hin", psm=1, oem=3):
    # Use pytesseract.image_to_data for block/line/word metadata
    config = f"--oem {oem} --psm {psm}"
    data = pytesseract.image_to_data(pil_img, lang=tesseract_lang, config=config, output_type=pytesseract.Output.DICT)
    blocks = []
    n = len(data['level'])
    for i in range(n):
        text = (data['text'][i] or "").strip()
        if text == "":
            continue
        try:
            conf = int(data['conf'][i])
        except:
            conf = None
        bbox = {
            "left": int(data['left'][i]),
            "top": int(data['top'][i]),
            "width": int(data['width'][i]),
            "height": int(data['height'][i])
        }
        blocks.append({
            "page": int(page_no),
            "block_id": i,
            "bbox": bbox,
            "conf": conf,
            "text": text
        })
    return blocks

def pdf_to_jsonl(pdf_path, out_jsonl, dpi=300, poppler_path=None, temp_dir=None,
                 preprocess_opts=None, tesseract_lang="eng+hin"):
    pdf_path = Path(pdf_path)
    out_jsonl = Path(out_jsonl)
    temp_dir = Path(temp_dir) if temp_dir else Path.cwd() / "ocr_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    # convert PDF to images
    convert_kwargs = {"dpi": dpi}
    if poppler_path:
        convert_kwargs["poppler_path"] = poppler_path

    pages = convert_from_path(str(pdf_path), **convert_kwargs)
    all_blocks = []
    for idx, page in enumerate(pages, start=1):
        png_name = temp_dir / f"{pdf_path.stem}_page_{idx}.png"
        page.save(png_name, format="PNG")
        # preprocessing
        img = Image.open(png_name)
        img_proc = preprocess_image(img,
                                    deskew=bool(preprocess_opts.get("deskew", False)),
                                    denoise=bool(preprocess_opts.get("denoise", False)),
                                    binarize=bool(preprocess_opts.get("binarize", False)))
        # run OCR
        blocks = page_to_blocks(img_proc, idx, tesseract_lang=tesseract_lang,
                                 psm=preprocess_opts.get("psm", 1),
                                 oem=preprocess_opts.get("oem", 3))
        all_blocks.extend(blocks)
        # optionally save processed image for inspection
        if preprocess_opts.get("save_processed", False):
            img_proc.save(temp_dir / f"{pdf_path.stem}_page_{idx}_proc.png")

    # write JSONL
    with out_jsonl.open("w", encoding="utf-8") as f:
        for b in all_blocks:
            f.write(json.dumps(b, ensure_ascii=False) + "\n")

    return out_jsonl, temp_dir

def main():
    parser = argparse.ArgumentParser(description="PDF -> OCR JSONL using Tesseract (eng+hin)")
    parser.add_argument("--pdf", required=True, help="Input PDF path")
    parser.add_argument("--out", required=True, help="Output JSONL path")
    parser.add_argument("--dpi", type=int, default=300, help="Render DPI")
    parser.add_argument("--poppler-path", default=None, help="Optional Poppler bin path")
    parser.add_argument("--temp-dir", default=None, help="Temporary output directory for PNGs")
    parser.add_argument("--deskew", action="store_true", help="Apply deskew preprocessing (simple)")
    parser.add_argument("--denoise", action="store_true", help="Apply median denoise")
    parser.add_argument("--binarize", action="store_true", help="Apply naive binarization")
    parser.add_argument("--save-processed", action="store_true", help="Save processed page PNGs")
    parser.add_argument("--psm", type=int, default=1, help="Tesseract Page Segmentation Mode")
    parser.add_argument("--oem", type=int, default=3, help="Tesseract OCR Engine Mode")
    args = parser.parse_args()

    preprocess_opts = {
        "deskew": args.deskew,
        "denoise": args.denoise,
        "binarize": args.binarize,
        "save_processed": args.save_processed,
        "psm": args.psm,
        "oem": args.oem
    }

    out_jsonl_path, tmp = pdf_to_jsonl(
        args.pdf,
        args.out,
        dpi=args.dpi,
        poppler_path=args.poppler_path,
        temp_dir=args.temp_dir,
        preprocess_opts=preprocess_opts,
        tesseract_lang="eng+hin"
    )

    print(f"Wrote OCR blocks to: {out_jsonl_path}")
    print(f"Temp images in: {tmp}")

if __name__ == "__main__":
    main()
