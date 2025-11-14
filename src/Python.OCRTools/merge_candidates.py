# merge_candidates.py
# Robust merger: accepts rows where bbox is nested or where top/left are top-level.
# Produces candidates.jsonl for LLM postprocessing.

import sys
import json
from pathlib import Path
from collections import defaultdict
import math
import re

DEVANAGARI_RANGE = (0x0900, 0x097F)

def is_devanagari_char(ch):
    return DEVANAGARI_RANGE[0] <= ord(ch) <= DEVANAGARI_RANGE[1]

def script_ratio(text):
    if not text:
        return 0.0
    dev = sum(1 for c in text if is_devanagari_char(c))
    total = sum(1 for c in text if c.isalpha())
    return (dev / total) if total > 0 else 0.0

def normalize_row(obj):
    """
    Ensure each row has: page, text, left, top, width, height, conf, block_num, par_num, line_num
    Accepts rows with nested 'bbox' or with direct fields.
    """
    out = {}
    out['page'] = int(obj.get('page', 1))
    out['text'] = (obj.get('text') or obj.get('ocr_text') or "").strip()
    # confidence
    conf = obj.get('conf')
    try:
        out['conf'] = int(conf) if conf is not None and str(conf).lstrip('-').isdigit() else None
    except:
        out['conf'] = None

    # topology: either direct fields or nested bbox
    if 'left' in obj and 'top' in obj and 'width' in obj and 'height' in obj:
        out['left'] = int(obj.get('left') or 0)
        out['top'] = int(obj.get('top') or 0)
        out['width'] = int(obj.get('width') or 0)
        out['height'] = int(obj.get('height') or 0)
    else:
        bbox = obj.get('bbox') or {}
        # bbox may be dict with keys left/top/width/height
        out['left'] = int(bbox.get('left') or bbox.get('x') or obj.get('left', 0) or 0)
        out['top'] = int(bbox.get('top') or bbox.get('y') or obj.get('top', 0) or 0)
        out['width'] = int(bbox.get('width') or bbox.get('w') or obj.get('width', 0) or 0)
        out['height'] = int(bbox.get('height') or bbox.get('h') or obj.get('height', 0) or 0)

    # optional tesseract fields (may not exist)
    out['block_num'] = int(obj.get('block_num') or obj.get('block_id') or 0)
    out['par_num'] = int(obj.get('par_num') or 0)
    out['line_num'] = int(obj.get('line_num') or 0)

    return out

def load_blocks(jsonl_path):
    blocks_by_page = defaultdict(list)
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            norm = normalize_row(obj)
            blocks_by_page[norm['page']].append(norm)
    return blocks_by_page

def _median(arr):
    arr = sorted([a for a in arr if a is not None])
    if not arr:
        return None
    n = len(arr)
    if n%2==1:
        return arr[n//2]
    return (arr[n//2 -1] + arr[n//2]) / 2.0

def group_words_to_lines(page_words, y_tol=0.5):
    # If Tesseract produced line_num fields, prefer grouping by that
    has_line_num = any(w.get('line_num', 0) != 0 for w in page_words)
    lines = []
    if has_line_num:
        by_line = {}
        for w in page_words:
            key = (w.get('block_num',0), w.get('par_num',0), w.get('line_num',0))
            by_line.setdefault(key, []).append(w)
        # sort keys in reading order
        for k in sorted(by_line.keys(), key=lambda t: (t[0], t[1], t[2])):
            words = by_line[k]
            words_sorted = sorted(words, key=lambda w: w.get('left',0))
            text = " ".join((w.get('text') or "").strip() for w in words_sorted).strip()
            left = min(w.get('left',0) for w in words_sorted)
            top = min(w.get('top',0) for w in words_sorted)
            right = max(w.get('left',0)+w.get('width',0) for w in words_sorted)
            bottom = max(w.get('top',0)+w.get('height',0) for w in words_sorted)
            confs = [w.get('conf') for w in words_sorted if w.get('conf') is not None]
            conf_avg = int(sum(confs)/len(confs)) if confs else None
            lines.append({"text": text, "left": left, "top": top, "right": right, "bottom": bottom, "conf": conf_avg, "words": words_sorted})
        return lines

    # fallback clustering by 'top' coordinate
    page_words_sorted = sorted(page_words, key=lambda w: w.get('top',0))
    median_h = _median([w.get('height',20) or 20 for w in page_words_sorted]) or 20
    groups = []
    for w in page_words_sorted:
        placed = False
        for g in groups:
            if abs(w.get('top',0) - g['top']) <= median_h * y_tol:
                g['words'].append(w)
                # update average top
                g['top'] = int(sum(x['top'] for x in g['words'])/len(g['words']))
                placed = True
                break
        if not placed:
            groups.append({'top': w.get('top',0), 'words':[w]})
    for g in groups:
        words = sorted(g['words'], key=lambda w:w.get('left',0))
        text = " ".join((w.get('text') or "").strip() for w in words).strip()
        left = min(w.get('left',0) for w in words)
        top = min(w.get('top',0) for w in words)
        right = max(w.get('left',0)+w.get('width',0) for w in words)
        bottom = max(w.get('top',0)+w.get('height',0) for w in words)
        confs = [w.get('conf') for w in words if w.get('conf') is not None]
        conf_avg = int(sum(confs)/len(confs)) if confs else None
        lines.append({"text": text, "left": left, "top": top, "right": right, "bottom": bottom, "conf": conf_avg, "words": words})
    return lines

QUESTION_NUM_REGEX = re.compile(r'^\s*(?:Q(?:uestion)?\s*)?(\d{1,4})\s*[\).\:-]?\s*(.*)', re.UNICODE)

def detect_question_number(line_text):
    m = QUESTION_NUM_REGEX.match(line_text)
    if m:
        qnum = m.group(1)
        rest = m.group(2).strip()
        return qnum, rest
    return None, line_text

def guess_lang(text):
    r = script_ratio(text)
    if r > 0.3:
        return "hindi"
    if r < 0.05:
        return "english"
    return "mixed"

def _bbox(line):
    return {"left": line['left'], "top": line['top'], "width": line['right']-line['left'], "height": line['bottom']-line['top']}

def _avg_conf(blocks):
    confs = [b.get('conf') for b in blocks if b.get('conf') is not None]
    if not confs:
        return None
    return int(sum(confs)/len(confs))

def group_lines_to_candidates(lines):
    candidates = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        qnum, rest = detect_question_number(line['text'])
        if qnum:
            blocks = []
            blocks.append({"lang": guess_lang(line['text']), "text": rest or line['text'], "bbox": _bbox(line), "conf": line.get('conf')})
            i += 1
            while i < n:
                nxt = lines[i]
                nxt_qnum, _ = detect_question_number(nxt['text'])
                if nxt_qnum:
                    break
                # break if huge vertical gap relative to line height
                line_height = line.get('bottom',0)-line.get('top',0) or 1
                if nxt['top'] - line['bottom'] > line_height * 6:
                    break
                blocks.append({"lang": guess_lang(nxt['text']), "text": nxt['text'], "bbox": _bbox(nxt), "conf": nxt.get('conf')})
                line = nxt
                i += 1
            candidates.append({"qnum": qnum, "blocks": blocks, "conf_avg": _avg_conf(blocks)})
        else:
            blocks = []
            start = i
            j = i
            # collect up to 6 lines heuristically
            while j < min(n, i+6):
                blocks.append({"lang": guess_lang(lines[j]['text']), "text": lines[j]['text'], "bbox": _bbox(lines[j]), "conf": lines[j].get('conf')})
                # if we detect option marker on a later line, we break to include options
                if re.match(r'^\s*[A-D]\s*[\.\)]\s*', lines[j]['text']):
                    j += 1
                    while j < n and len([1 for _ in range(3) if re.match(r'^\s*[A-D]\s*[\.\)]', lines[j]['text'])])==0 and j < i+8:
                        blocks.append({"lang": guess_lang(lines[j]['text']), "text": lines[j]['text'], "bbox": _bbox(lines[j]), "conf": lines[j].get('conf')})
                        j += 1
                    break
                j += 1
            if len(" ".join(b['text'] for b in blocks).strip()) > 10:
                candidates.append({"qnum": None, "blocks": blocks, "conf_avg": _avg_conf(blocks)})
                i = j
            else:
                i += 1
    return candidates

def main(in_path, out_path):
    pages = load_blocks(in_path)
    all_candidates = []
    for page, words in sorted(pages.items()):
        lines = group_words_to_lines(words)
        lines_sorted = sorted(lines, key=lambda l: (l['top'], l['left']))
        candidates = group_lines_to_candidates(lines_sorted)
        for c in candidates:
            c['page'] = page
            all_candidates.append(c)
    with open(out_path, 'w', encoding='utf-8') as f:
        for c in all_candidates:
            f.write(json.dumps(c, ensure_ascii=False) + '\n')
    print(f"Wrote {len(all_candidates)} candidates to {out_path}")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python merge_candidates.py <input_blocks.jsonl> <out_candidates.jsonl>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
