"""
generate_dashboard.py
=====================
Reads RRU_BXP_RL50_Health_Result.csv from each dated scan folder,
processes the data, and produces a single self-contained dashboard.html
with all data embedded — no CSV files or web server needed.

Usage:
    python generate_dashboard.py

    By default, scans all subfolders named YYYYMMDD in the same directory
    as this script. You can also specify folders explicitly:

    python generate_dashboard.py --scans 20260212 20260306 20260401

    Custom output path:
    python generate_dashboard.py --output my_dashboard.html

Output:
    dashboard.html  (self-contained, ready to upload to SharePoint)
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime


# ── Constants matching the dashboard JS ──────────────────────────────────────

BRANCHES = ["A", "B", "C", "D"]

RL_ORDER = [
    "Healthy",
    "Early Degradation",
    "Middle Degradation",
    "Late Degradation",
    "Critical/Failed",
    "No Data",
]
RL_ALERT_KEYS = {"Early Degradation", "Middle Degradation", "Late Degradation", "Critical/Failed"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def pf(v):
    """Parse float, return None on failure."""
    try:
        n = float(v)
        return None if (n != n) else round(n, 2)  # NaN check
    except (ValueError, TypeError):
        return None


def node_name(src):
    """Strip path and .log extension from source_file column."""
    if not src:
        return ""
    base = src.replace("\\", "/").split("/")[-1]
    return re.sub(r"\.log$", "", base, flags=re.IGNORECASE)


def normalize_rl(raw):
    """Normalise RL status string — strips suffix like '(1/4 branches)'."""
    if not raw:
        return "No Data"
    s = raw.strip().lower()
    if s.startswith("healthy"):           return "Healthy"
    if s.startswith("early"):             return "Early Degradation"
    if s.startswith("middle"):            return "Middle Degradation"
    if s.startswith("late"):              return "Late Degradation"
    if s.startswith("critical"):          return "Critical/Failed"
    if "healthy"  in s:                   return "Healthy"
    if "early"    in s:                   return "Early Degradation"
    if "middle"   in s:                   return "Middle Degradation"
    if "late"     in s:                   return "Late Degradation"
    if "critical" in s:                   return "Critical/Failed"
    return "No Data"


def vswr_category(row):
    """
    Determine VSWR risk category for one unit.
    Priority: RL < 10 dB > No Output > Non-risk
    """
    risk_flag = (row.get("VSWR_risk_assessment") or "").lower()
    is_risk = "risk" in risk_flag and "non" not in risk_flag
    if not is_risk:
        return "Non-risk"

    has_no_output = False
    min_db = None
    for i in range(1, 5):
        val = (row.get(f"VSWR_{i}_dB") or "").strip()
        if not val or val.upper() == "N/A":
            continue
        if re.search(r"not enough output|no output", val, re.IGNORECASE):
            has_no_output = True
            continue
        n = pf(val)
        if n is not None and (min_db is None or n < min_db):
            min_db = n

    if min_db is not None and min_db < 10:
        return "Risk — RL < 10 dB"
    if has_no_output:
        return "Risk — No Output"
    return "Risk — RL < 10 dB"   # risk flagged but sub-type unclear


def get_worst_branch(row):
    """Find worst RL branch and return its metrics."""
    best = {"rank": -1, "br": None, "fv": None, "btr": None,
            "szr": None, "cog": None, "score": None}
    for br in BRANCHES:
        hs   = normalize_rl(row.get(f"RL_{br}_Health") or "")
        rank = -1 if hs == "No Data" else RL_ORDER.index(hs)
        if rank > best["rank"]:
            best = {
                "rank":  rank,
                "br":    f"Branch {br}",
                "fv":    pf(row.get(f"RL_{br}_FV_Ratio_Pct")),
                "btr":   pf(row.get(f"RL_{br}_BTR_Pct")),
                "szr":   pf(row.get(f"RL_{br}_SZR_Pct")),
                "cog":   pf(row.get(f"RL_{br}_CoG")),
                "score": pf(row.get(f"RL_{br}_Score")),
            }
    return best


def get_risk_branch(row):
    """Find VSWR risk branch and worst dB value."""
    risk_br = None
    min_db  = None
    no_out  = False
    for i, br in enumerate(BRANCHES, 1):
        val = (row.get(f"VSWR_{i}_dB") or "").strip()
        if not val or val.upper() == "N/A":
            continue
        if re.search(r"not enough output|no output", val, re.IGNORECASE):
            no_out = True
            if risk_br is None:
                risk_br = f"Branch {br}"
            continue
        n = pf(val)
        if n is not None and (min_db is None or n < min_db):
            min_db  = n
            risk_br = f"Branch {br}"
    return {"branch": risk_br, "db": min_db, "noOut": no_out}


def cog_override_any(row):
    """True if any branch has CoG Override set (handles '⚠ Yes' etc.)."""
    for br in BRANCHES:
        v = (row.get(f"RL_{br}_CoG_Override") or "").strip().lower()
        if "yes" in v or "true" in v or v == "1":
            return True
    return False


def scan_label_from_id(scan_id):
    """Convert YYYYMMDD to 'Mon YYYY' label."""
    try:
        return datetime.strptime(scan_id, "%Y%m%d").strftime("%b %Y")
    except ValueError:
        return scan_id


# ── CSV → scan data ───────────────────────────────────────────────────────────

def process_csv(csv_path, scan_id):
    """Read CSV and return processed scan data dict."""
    label = scan_label_from_id(scan_id)
    rows  = []

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    if not rows:
        raise ValueError(f"No data rows in {csv_path}")

    units = []
    rl_counts   = {k: 0 for k in RL_ORDER}
    vswr_counts = {"Risk — No Output": 0, "Risk — RL < 10 dB": 0, "Non-risk": 0}

    for row in rows:
        rl_status = normalize_rl(row.get("Unit_Worst_Status") or "")
        wb        = get_worst_branch(row)
        vswr_cat  = vswr_category(row)
        rb        = get_risk_branch(row)
        cog_ov    = cog_override_any(row)

        rl_counts[rl_status]    = rl_counts.get(rl_status, 0) + 1
        vswr_counts[vswr_cat]   = vswr_counts.get(vswr_cat, 0) + 1

        # Build compact unit dict — only include non-None values to save space
        unit = {
            "n":  node_name(row.get("source_file") or ""),
            "s":  (row.get("serial_number") or "").strip(),
            "rs": rl_status,
            "sc": wb["score"],
            "wb": wb["br"],
            "fv": wb["fv"],
            "bt": wb["btr"],
            "sz": wb["szr"],
            "cg": wb["cog"],
            "co": "Yes" if cog_ov else "No",
            "vc": vswr_cat,
            "vb": rb["branch"],
            "vd": rb["db"],
            "vn": rb["noOut"],
        }
        units.append(unit)

    rl_risk   = sum(rl_counts[k] for k in RL_ALERT_KEYS)
    vswr_risk = vswr_counts["Risk — No Output"] + vswr_counts["Risk — RL < 10 dB"]
    both      = sum(
        1 for u in units
        if u["rs"] in RL_ALERT_KEYS and u["vc"] != "Non-risk"
    )

    return {
        "id":         scan_id,
        "label":      label,
        "rlCounts":   rl_counts,
        "vswrCounts": vswr_counts,
        "rlRisk":     rl_risk,
        "vswrRisk":   vswr_risk,
        "both":       both,
        "units":      units,
    }


# ── Discover scan folders ──────────────────────────────────────────────────────

def discover_scans(base_dir):
    """Find all YYYYMMDD subfolders containing a Health_Result CSV."""
    found = []
    for name in sorted(os.listdir(base_dir)):
        if not re.fullmatch(r"\d{8}", name):
            continue
        csv_path = os.path.join(base_dir, name, "RRU_BXP_RL50_Health_Result.csv")
        if os.path.isfile(csv_path):
            found.append((name, csv_path))
    return found


# ── Generate HTML ──────────────────────────────────────────────────────────────

def generate_html(scan_data_list, output_path):
    """
    Read dashboard_v2.html (template), replace the SCANS/Papa Parse section
    with embedded data, and write the self-contained output.
    """
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_v2.html")
    if not os.path.isfile(template_path):
        print(f"ERROR: dashboard_v2.html not found at {template_path}")
        print("Make sure generate_dashboard.py is in the same folder as dashboard_v2.html")
        sys.exit(1)

    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    # ── Build embedded data block ──────────────────────────────────────────
    # Compact JSON — units use short keys to keep file size down
    embedded_json = json.dumps(
        {s["id"]: s for s in scan_data_list},
        ensure_ascii=False,
        separators=(",", ":")
    )

    # Scan list for dropdowns (id + label only)
    scans_list_json = json.dumps(
        [{"id": s["id"], "label": s["label"]} for s in scan_data_list],
        ensure_ascii=False,
        separators=(",", ":")
    )

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    embedded_block = f"""/* ══════════════════════════════════════════
   EMBEDDED DATA — generated {generated_at}
   Do not edit manually. Re-run generate_dashboard.py to update.
══════════════════════════════════════════ */
const EMBEDDED_DATA = {embedded_json};
const SCANS = {scans_list_json};
"""

    # ── Replace SCANS config block ─────────────────────────────────────────
    old_scans = """const SCANS = [
  { id: "20260212", label: "Feb 2026", path: "20260212/RRU_BXP_RL50_Health_Result.csv" },
  { id: "20260306", label: "Mar 2026", path: "20260306/RRU_BXP_RL50_Health_Result.csv" },
  /* Add new scans here:
  { id: "20260401", label: "Apr 2026", path: "20260401/RRU_BXP_RL50_Health_Result.csv" },
  */
];"""
    if old_scans not in html:
        print("WARNING: Could not find SCANS block to replace. Check dashboard_v2.html is unmodified.")
    else:
        html = html.replace(old_scans, embedded_block)

    # ── Remove Papa Parse CDN script tag ──────────────────────────────────
    html = html.replace(
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/PapaParse/5.4.1/papaparse.min.js"></script>',
        "<!-- Papa Parse removed: data embedded directly -->"
    )

    # ── Replace queryScan (CSV fetch) with embedded data lookup ───────────
    # Match using regex to avoid unicode encoding mismatches
    import re as _re
    new_query = (
        "function queryScan() {\n"
        "  const id = document.getElementById(\"sel-month\").value;\n"
        "  if (!id) { setStatus(\"Please select a month first.\", \"error\"); return; }\n"
        "  const embedded = EMBEDDED_DATA[id];\n"
        "  if (!embedded) { setStatus(\"Scan data not found.\", \"error\"); return; }\n"
        "  try {\n"
        "    scanData = expandEmbedded(embedded);\n"
        "    setStatus(\"Loaded: \" + scanData.meta.label + \" \u2014 \" + scanData.units.length.toLocaleString() + \" units\", \"\");\n"
        "    renderAll();\n"
        "  } catch(e) {\n"
        "    setStatus(\"Processing error: \" + e.message, \"error\");\n"
        "    console.error(e);\n"
        "  }\n"
        "}"
    )
    # Regex: match function queryScan() { ... } including nested braces
    depth, start_idx, end_idx = 0, -1, -1
    fn_start = html.find("function queryScan()")
    if fn_start == -1:
        print("WARNING: Could not find queryScan function to replace.")
    else:
        i = fn_start
        while i < len(html):
            if html[i] == "{":
                if depth == 0: start_idx = i
                depth += 1
            elif html[i] == "}":
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break
            i += 1
        if start_idx != -1 and end_idx != -1:
            html = html[:fn_start] + new_query + html[end_idx:]
        else:
            print("WARNING: Could not parse queryScan function braces.")

        # ── Remove onBothLoaded and loaded counter (no longer needed) ─────────
    old_both = """  let loaded = 0;
  let baseRows = null, targetRows = null;

  function onBothLoaded() {
    document.getElementById("btn-compare").disabled = false;
    try {
      cmpBaseData   = buildScanData(baseRows,   baseScan);
      cmpTargetData = buildScanData(targetRows, targetScan);
      setCmpStatus("Comparing " + baseScan.label + " vs " + targetScan.label, "");
      renderComparison();
    } catch(e) {
      setCmpStatus("Processing error: " + e.message, "error");
      console.error(e);
    }
  }

"""
    if old_both in html:
        html = html.replace(old_both, "\n")

    # ── Replace Papa.parse calls in runComparison with embedded data ───────
    old_cmp = (
        "  Papa.parse(baseScan.path, {\n"
        "    download: true, header: true, skipEmptyLines: true,\n"
        "    complete(r) { baseRows = r.data; loaded++; if (loaded === 2) onBothLoaded(); },\n"
        "    error(e)    { document.getElementById(\"btn-compare\").disabled = false; "
        "setCmpStatus(\"Failed to load \" + baseScan.label + \". Run via: python -m http.server 8080\", \"error\"); }\n"
        "  });\n"
        "\n"
        "  Papa.parse(targetScan.path, {\n"
        "    download: true, header: true, skipEmptyLines: true,\n"
        "    complete(r) { targetRows = r.data; loaded++; if (loaded === 2) onBothLoaded(); },\n"
        "    error(e)    { document.getElementById(\"btn-compare\").disabled = false; "
        "setCmpStatus(\"Failed to load \" + targetScan.label + \". Run via: python -m http.server 8080\", \"error\"); }\n"
        "  });"
    )
    new_cmp = (
        "  const baseEmb   = EMBEDDED_DATA[baseId];\n"
        "  const targetEmb = EMBEDDED_DATA[targetId];\n"
        "  if (!baseEmb || !targetEmb) {\n"
        "    document.getElementById(\"btn-compare\").disabled = false;\n"
        "    setCmpStatus(\"Scan data not found for one or both selections.\", \"error\");\n"
        "    return;\n"
        "  }\n"
        "  try {\n"
        "    cmpBaseData   = expandEmbedded(baseEmb);\n"
        "    cmpTargetData = expandEmbedded(targetEmb);\n"
        "    setCmpStatus(\"Comparing \" + baseScan.label + \" vs \" + targetScan.label, \"\");\n"
        "    renderComparison();\n"
        "  } catch(e) {\n"
        "    setCmpStatus(\"Processing error: \" + e.message, \"error\");\n"
        "    console.error(e);\n"
        "  }\n"
        "  document.getElementById(\"btn-compare\").disabled = false;"
    )
    if old_cmp in html:
        html = html.replace(old_cmp, new_cmp)
    else:
        # Fallback: use brace-matching on Papa.parse(baseScan.path
        base_start = html.find("  Papa.parse(baseScan.path")
        target_end_marker = "  });\n\n  // ── Replace"
        # Find end of the second Papa.parse block
        second_start = html.find("  Papa.parse(targetScan.path", base_start)
        if base_start != -1 and second_start != -1:
            # Find closing }); of second block
            close = html.find("  });", second_start) + 5
            html = html[:base_start] + new_cmp + html[close:]
        else:
            print("WARNING: Could not find Papa.parse comparison block to replace.")

    # ── Inject expandEmbedded() function before buildScanData ─────────────
    # This function converts compact embedded format back to the structure
    # that all render functions expect (same shape as buildScanData output)
    expand_fn = """/* ══════════════════════════════════════════
   EXPAND EMBEDDED DATA
   Converts compact embedded unit objects (short keys)
   back to the full structure render functions expect.
══════════════════════════════════════════ */
function expandEmbedded(emb) {
  const units = (emb.units || []).map(u => ({
    node:        u.n  || "",
    serial:      u.s  || "",
    rl_status:   u.rs || "No Data",
    score:       u.sc,
    worst_br:    u.wb,
    fv:          u.fv,
    btr:         u.bt,
    szr:         u.sz,
    cog:         u.cg,
    cog_override:u.co || "No",
    vswr_cat:    u.vc || "Non-risk",
    vswr_branch: u.vb,
    vswr_db:     u.vd,
    vswr_noout:  u.vn || false,
  }));
  return {
    meta:       { id: emb.id, label: emb.label },
    units,
    rlCounts:   emb.rlCounts   || {},
    vswrCounts: emb.vswrCounts || {},
    rlRisk:     emb.rlRisk     || 0,
    vswrRisk:   emb.vswrRisk   || 0,
    both:       emb.both       || 0,
  };
}

"""

    # Insert before buildScanData
    marker = "/* ══════════════════════════════════════════\n   BUILD SCAN DATA FROM CSV ROWS"
    if marker in html:
        html = html.replace(marker, expand_fn + marker)
    else:
        print("WARNING: Could not find buildScanData marker to inject expandEmbedded().")

    # ── Write output ───────────────────────────────────────────────────────
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate self-contained dashboard.html with embedded scan data."
    )
    parser.add_argument(
        "--scans", nargs="+", metavar="YYYYMMDD",
        help="Scan folder IDs to include (e.g. 20260212 20260306). "
             "If omitted, all YYYYMMDD subfolders are auto-discovered."
    )
    parser.add_argument(
        "--output", default="dashboard.html",
        help="Output HTML filename (default: dashboard.html)"
    )
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Resolve scan list
    if args.scans:
        scan_list = []
        for sid in args.scans:
            csv_path = os.path.join(base_dir, sid, "RRU_BXP_RL50_Health_Result.csv")
            if not os.path.isfile(csv_path):
                print(f"WARNING: CSV not found for {sid} at {csv_path} — skipping")
                continue
            scan_list.append((sid, csv_path))
    else:
        scan_list = discover_scans(base_dir)
        if not scan_list:
            print("ERROR: No YYYYMMDD scan folders found. Run from WorkFolder or use --scans.")
            sys.exit(1)

    if not scan_list:
        print("ERROR: No valid scans found to process.")
        sys.exit(1)

    print(f"Processing {len(scan_list)} scan(s):")
    scan_data_list = []
    for sid, csv_path in scan_list:
        print(f"  {sid} — {csv_path} ...", end=" ", flush=True)
        try:
            data = process_csv(csv_path, sid)
            scan_data_list.append(data)
            print(f"{len(data['units']):,} units, {data['rlRisk']} RL risk, {data['vswrRisk']} VSWR risk")
        except Exception as e:
            print(f"ERROR: {e}")

    if not scan_data_list:
        print("ERROR: No scans processed successfully.")
        sys.exit(1)

    output_path = os.path.join(base_dir, args.output)
    print(f"\nGenerating {args.output} ...", end=" ", flush=True)
    generate_html(scan_data_list, output_path)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"done ({size_kb:.0f} KB)")
    print(f"\nOutput: {output_path}")
    print("Upload this file to SharePoint — no CSV folders needed.")


if __name__ == "__main__":
    main()
