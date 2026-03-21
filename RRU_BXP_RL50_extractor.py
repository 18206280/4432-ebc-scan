# python RRU_BXP_RL50_extractor.py --input 20260212
# python RRU_BXP_RL50_extractor.py --input 20260306
#
import re
import os
import csv
import argparse
from collections import defaultdict
import time
import datetime  # Added for deployment date parsing

# ============================================================
# RL HEALTH CLASSIFICATION ALGORITHM
# ============================================================
# Classification is based on three metrics per branch:
#   1. FV Ratio (%)  = FV_1 (TOO_LOW_POWER) / Total Activity × 100   [Weight: 60%]
#   2. BTR (%)       = sum(bins 1-10) / TSM × 100                     [Weight: 25%]
#   3. SZR (%)       = sum(bins 13-21) / TSM × 100                    [Weight: 15%]
#
# Total Activity    = TSM + FV_1 + FV_2 + FV_3  (dynamic denominator)
# FV Noise Gate     = 120 counts (aligned with VSWR alarm trigger threshold)
#                     FV_1 <= 120 is treated as noise, scored as 100
#                     FV_1 >  120 applies ratio-based scoring
#
# Safe Zone         = Bins 13-21 (>=12dB), aligned with production test limit (>12dB)
# Marginal Zone     = Bins 11-12 (10-12dB), below production limit but above VSWR alarm
# Fail Zone         = Bins 1-10  (0-10dB),  below VSWR alarm threshold (10dB)
#
# MIN_ACTIVITY      = 17,280 (~1 day), flags insufficient data if below
#
# CoG Early Warning  = Center of Gravity threshold = 14 dB
#                     Derived from production CPK: mean=18.6dB, std=1.47dB
#                     14dB = ~3.1σ below mean — confirmed degradation beyond production spread
#                     If SZR=100% AND CoG < 14 -> override to Early Degradation
#                     CoG override only applies when SZR=100% (safe zone not yet breached)
#
# Degradation Phases:
#   Healthy           : FV_1<=120 OR FV=0%,  BTR=0%,   SZR>95%,  CoG>=14
#   Early Degradation : FV ratio<=1%,        BTR=0%,   SZR 70-95% OR CoG<14 with SZR=100%
#   Middle Degradation: FV ratio 1-10%,      BTR<50%,  SZR<70%
#   Late Degradation  : FV ratio 10-80%,     BTR>50%,  SZR~0%
#   Critical/Failed   : FV ratio>80% OR TSM=0 with FV_1>0
#   No Data           : Total Activity=0
#
# Last_RL_ABCD_4Ports:
#   Shows the 4 most recent Event 50 RL entries per branch independently.
#   Each branch (A/B/C/D) finds its own 4 most recent entries by timestamp.
#   Entries within each branch are sorted newest-first (Latest, -W1, -W2, -W3).
#   Classification still uses only the most recent entry per branch (unchanged).
#
# Active_VSWR_Alarm:
#   Active VSWR alarms extracted from the 'alt' (alarm list) section of the log.
#   Format in log:  <node>> alt
#                   YYYY-MM-DD HH:MM:SS <sev> VSWR Over Threshold
#                   FieldReplaceableUnit=RRU-X,RfPort=<port> (<detail>)
#   Matched to unit by RRU number (FieldReplaceableUnit=RRU-X).
#   Multiple active alarms per unit are newline-separated.
#   "N/A (No active VSWR alarm)" if none found.
# ============================================================

TOTAL_MEASUREMENTS = 120960  # theoretical max measurements per week per branch (every 5s x 7 days)
MIN_ACTIVITY = 17280         # minimum activity threshold (~1 day of measurements)
FV_NOISE_GATE = 120          # minimum FV_1 count before penalizing (aligned with VSWR alarm trigger: 120 consecutive failures)
COG_EARLY_WARNING = 14       # CoG threshold for early warning
                             # Derived from production CPK data: mean=18.6dB, std=1.47dB
                             # 14dB ≈ 3.1σ below production mean — a new unit should never
                             # reach this level; confirmed degradation beyond production spread
LAST_RL_HISTORY = 4          # number of most recent Event 50 entries to show per branch

def safe_int(val, default=0):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def classify_rl_health(bins, fv_1, fv_2, fv_3):
    tsm = sum(bins)
    total_activity = tsm + fv_1 + fv_2 + fv_3

    if total_activity == 0:
        return {
            'health_status': '⚪ No Data', 'health_score': 'N/A',
            'fv_ratio_pct': 0.0, 'btr_pct': 0.0, 'szr_pct': 0.0, 'cog': 0.0,
            'cog_override': False, 'tsm': 0, 'total_activity': 0,
            'fv_1': 0, 'insufficient_data': False
        }

    if tsm == 0 and fv_1 > 0:
        fv_ratio = round(fv_1 / total_activity * 100, 2)
        insufficient = total_activity < MIN_ACTIVITY
        return {
            'health_status': '🚨 Critical/Failed', 'health_score': 0,
            'fv_ratio_pct': fv_ratio, 'btr_pct': 0.0, 'szr_pct': 0.0, 'cog': 0.0,
            'cog_override': False, 'tsm': 0, 'total_activity': total_activity,
            'fv_1': fv_1, 'insufficient_data': insufficient
        }

    fv_ratio = round(fv_1 / total_activity * 100, 2)
    btr = round(sum(bins[:10]) / tsm * 100, 2)
    szr = round(sum(bins[12:21]) / tsm * 100, 2)
    cog = round(sum((i + 1) * bins[i] for i in range(21)) / tsm, 2)
    insufficient = total_activity < MIN_ACTIVITY

    if fv_1 <= FV_NOISE_GATE:      fv_score = 100
    elif fv_ratio <= 1:             fv_score = 75
    elif fv_ratio <= 10:            fv_score = 50
    elif fv_ratio <= 80:            fv_score = 25
    else:                           fv_score = 0

    if btr == 0:    bt_score = 100
    elif btr <= 5:  bt_score = 75
    elif btr <= 30: bt_score = 50
    elif btr <= 50: bt_score = 25
    else:           bt_score = 0

    if szr > 95:    sz_score = 100
    elif szr >= 70: sz_score = 75
    elif szr >= 30: sz_score = 50
    elif szr > 0:   sz_score = 25
    else:           sz_score = 0

    health_score = round((fv_score * 0.60) + (bt_score * 0.25) + (sz_score * 0.15), 2)

    if health_score >= 90:   health_status = '✅ Healthy'
    elif health_score >= 70: health_status = '⚠️ Early Degradation'
    elif health_score >= 40: health_status = '🔶 Middle Degradation'
    elif health_score >= 10: health_status = '🔴 Late Degradation'
    else:                    health_status = '🚨 Critical/Failed'

    cog_override = False
    if szr == 100.0 and cog < COG_EARLY_WARNING and health_status == '✅ Healthy':
        health_status = '⚠️ Early Degradation'
        cog_override = True

    return {
        'health_status': health_status, 'health_score': health_score,
        'fv_ratio_pct': fv_ratio, 'btr_pct': btr, 'szr_pct': szr, 'cog': cog,
        'cog_override': cog_override, 'tsm': tsm, 'total_activity': total_activity,
        'fv_1': fv_1, 'insufficient_data': insufficient
    }

def classify_unit_rl(record):
    results = {}
    for branch in ['A', 'B', 'C', 'D']:
        bins = [safe_int(record.get(f'RL_{branch}_field_{i}', 0)) for i in range(1, 22)]
        fv_1 = safe_int(record.get(f'RL_{branch}_FV_1', 0))
        fv_2 = safe_int(record.get(f'RL_{branch}_FV_2', 0))
        fv_3 = safe_int(record.get(f'RL_{branch}_FV_3', 0))
        time_val = record.get(f'RL_{branch}_time', 'N/A')
        if time_val == 'N/A' and sum(bins) == 0 and fv_1 == 0 and fv_2 == 0 and fv_3 == 0:
            results[branch] = None
            continue
        results[branch] = classify_rl_health(bins, fv_1, fv_2, fv_3)
    return results

def get_unit_worst_status(branch_results):
    priority = {
        '🚨 Critical/Failed': 5, '🔴 Late Degradation': 4,
        '🔶 Middle Degradation': 3, '⚠️ Early Degradation': 2,
        '✅ Healthy': 1, '⚪ No Data': 0
    }
    worst = '⚪ No Data'
    total_branches = 0
    degraded_branches = 0
    for branch, result in branch_results.items():
        if result is None:
            continue
        status = result['health_status']
        if status == '⚪ No Data':
            continue
        total_branches += 1
        if priority.get(status, 0) > priority.get(worst, 0):
            worst = status
        if priority.get(status, 0) >= priority['⚠️ Early Degradation']:
            degraded_branches += 1
    if worst == '⚪ No Data' or total_branches == 0:
        return '⚪ No Data'
    return f'{worst} ({degraded_branches}/{total_branches} branches)'

def extract_deployment_date(lines, bxp_num):
    hwlog_date_pattern = re.compile(
        rf'{re.escape(bxp_num)}:\s+\d+\s+\d+\s+(\d{{2}}-\d{{2}}-\d{{2}})\s+\d{{2}}:\d{{2}}:\d{{2}}'
    )
    deployment_dates = []
    for line in lines:
        match = hwlog_date_pattern.search(line)
        if match:
            deployment_dates.append(match.group(1))
    if deployment_dates:
        def parse_yy_mm_dd(date_str):
            yy, mm, dd = date_str.split('-')
            return datetime.datetime(year=2000 + int(yy), month=int(mm), day=int(dd))
        sorted_dates = sorted(deployment_dates, key=parse_yy_mm_dd)
        return sorted_dates[0]
    return "N/A"

def assess_vswr_risk(record):
    is_risk_vswr = False
    for col in ['VSWR_1_dB', 'VSWR_2_dB', 'VSWR_3_dB', 'VSWR_4_dB']:
        val = record.get(col, "N/A")
        if isinstance(val, str):
            if 'not enough output' in val.lower():
                is_risk_vswr = True; break
            try:
                if float(val) < 10:
                    is_risk_vswr = True; break
            except ValueError:
                pass
        elif isinstance(val, (int, float)):
            if val < 10:
                is_risk_vswr = True; break
    return 'Risk unit' if is_risk_vswr else 'Non-risk unit'

# ============================================================
# EXTRACTION LOGIC
# ============================================================

def extract_core_info(file_path):
    """
    Extract RRU/BXP identity, VSWR, and RL Event 50 data from one log file.

    Change vs original:
        Last_RL_ABCD_4Ports now stores the 4 most recent Event 50 RL
        entries per branch INDEPENDENTLY. Each branch (A/B/C/D) collects
        all its own entries across the full log, sorts by timestamp
        descending, and shows the top 4. Branches are not required to
        share the same timestamp group.

        Classification fields (RL_{branch}_field_*, FV_*, time) are still
        populated from the single most recent entry per branch — unchanged.
    """
    rru_pattern = re.compile(
        r'(RRU-\d+)\s+(BXP_\d+)\s+(RRU4432B28)\*?\s+.+?\s+([A-Z0-9]{10})\s+'
    )
    vswr_branch_pattern = re.compile(r'(BXP_\d+)\s+.*?(fui|fdf)\s+get\s+vswr\s+(\d)')
    vswr_value_pattern  = re.compile(r'(BXP_\d+):\s*VSWR:\s*(\d+\.\d+)\s*dB')
    vswr_error_pattern  = re.compile(r'(BXP_\d+):\s*(.+?)$')
    rl_detail_pattern   = re.compile(
        r'(BXP_\d+):\s+\[(\d{6} \d{6})\]\s+50:\s+RL Statistic Counters:\s+'
        r'([A-D]);RL:21:([0-9,]+);FV:([0-9,]+);1:([0-9,]+);2:([0-9,]+);3:([0-9,]+)'
    )
    rl_pattern = re.compile(
        r'(BXP_\d+):\s+\[.+?\]\s+50:\s+RL Statistic Counters:\s+([A-D]);(.+)'
    )

    rru_records    = []
    seen_rru       = set()
    bxp_vswr       = defaultdict(lambda: {"1": "N/A", "2": "N/A", "3": "N/A", "4": "N/A"})

    # Most recent Event 50 entry per branch — for classification (unchanged)
    bxp_rl_detail  = defaultdict(dict)

    # All Event 50 entries per branch per BXP — for Last_RL_ABCD_4Ports
    # {bxp_num: {branch: [(timestamp_str, entry_dict), ...]}}
    # Timestamp 'YYMMDD HHMMSS' sorts correctly as a plain string.
    bxp_rl_history = defaultdict(lambda: defaultdict(list))

    # Active VSWR alarms from 'alt' alarm table — keyed by RRU number
    node_prompt_pattern    = re.compile(r'^\S+>\s+alt\b')
    rru_from_alarm_pattern = re.compile(r'FieldReplaceableUnit=(RRU-\d+)')
    rru_active_vswr        = defaultdict(list)   # {rru_num: [alarm_line, ...]}

    # Historic VSWR alarms from 'lgar' command block
    lgar_prompt_pattern   = re.compile(r'^\S+>\s+lgar\b')
    any_prompt_pattern    = re.compile(r'^\S+>\s+\S+')
    rru_historic_vswr     = defaultdict(list)   # {rru_num: [alarm_line, ...]}

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = [line.strip() for line in f.readlines()]
        file_name = os.path.basename(file_path)
        full_path = os.path.abspath(file_path)

        bxp_deployment_dates = {}
        all_bxp_nums = re.findall(r'(BXP_\d+)', '\n'.join(lines))
        for bxp_num in set(all_bxp_nums):
            bxp_deployment_dates[bxp_num] = extract_deployment_date(lines, bxp_num)

        current_bxp    = None
        current_branch = None
        bxp_rl_all     = defaultdict(list)
        in_alt_block   = False   # True while inside 'alt' alarm table
        in_lgar_block  = False   # True while inside 'lgar' alarm block

        for line_num, line in enumerate(lines, 1):
            if not line:
                continue
            try:
                # 1. RRU/BXP/Serial identity
                rru_match = rru_pattern.search(line)
                if rru_match:
                    try:
                        rru_num    = rru_match.group(1) or "N/A"
                        bxp_num    = rru_match.group(2) or "N/A"
                        rru_model  = rru_match.group(3) or "N/A"
                        serial_num = rru_match.group(4) or "N/A"
                        key = (rru_num, bxp_num, serial_num)
                        if key not in seen_rru and rru_num != "N/A" and bxp_num != "N/A":
                            seen_rru.add(key)
                            deployment_date = bxp_deployment_dates.get(bxp_num, "N/A")
                            record = {
                                'source_file': file_name, 'full_file_path': full_path,
                                'RU_number': rru_num, 'BXP_number': bxp_num,
                                'RU_model': rru_model, 'serial_number': serial_num,
                                'Unit_Field_Deployment_Date': deployment_date,
                                'VSWR_1_dB': "N/A", 'VSWR_2_dB': "N/A",
                                'VSWR_3_dB': "N/A", 'VSWR_4_dB': "N/A",
                                'Last_RL_ABCD_4Ports': "N/A",
                                # REMOVED: 'VSWR_Alarm_Above_Last_RL' (col M)
                                # REMOVED: 'VSWR_Alarm_Count'          (col N)
                                'RL_A_time': "N/A",
                                **{f'RL_A_field_{i}': "N/A" for i in range(1, 22)},
                                'RL_A_FV': "N/A", 'RL_A_FV_1': "N/A",
                                'RL_A_FV_2': "N/A", 'RL_A_FV_3': "N/A",
                                'RL_B_time': "N/A",
                                **{f'RL_B_field_{i}': "N/A" for i in range(1, 22)},
                                'RL_B_FV': "N/A", 'RL_B_FV_1': "N/A",
                                'RL_B_FV_2': "N/A", 'RL_B_FV_3': "N/A",
                                'RL_C_time': "N/A",
                                **{f'RL_C_field_{i}': "N/A" for i in range(1, 22)},
                                'RL_C_FV': "N/A", 'RL_C_FV_1': "N/A",
                                'RL_C_FV_2': "N/A", 'RL_C_FV_3': "N/A",
                                'RL_D_time': "N/A",
                                **{f'RL_D_field_{i}': "N/A" for i in range(1, 22)},
                                'RL_D_FV': "N/A", 'RL_D_FV_1': "N/A",
                                'RL_D_FV_2': "N/A", 'RL_D_FV_3': "N/A",
                                'Unit_Worst_Status': "N/A",
                                'RL_A_Health': "N/A", 'RL_A_Score': "N/A",
                                'RL_A_FV_Ratio_Pct': "N/A", 'RL_A_BTR_Pct': "N/A",
                                'RL_A_SZR_Pct': "N/A", 'RL_A_CoG': "N/A",
                                'RL_A_CoG_Override': "N/A", 'RL_A_TSM': "N/A",
                                'RL_A_Total_Activity': "N/A", 'RL_A_Insufficient_Data': "N/A",
                                'RL_B_Health': "N/A", 'RL_B_Score': "N/A",
                                'RL_B_FV_Ratio_Pct': "N/A", 'RL_B_BTR_Pct': "N/A",
                                'RL_B_SZR_Pct': "N/A", 'RL_B_CoG': "N/A",
                                'RL_B_CoG_Override': "N/A", 'RL_B_TSM': "N/A",
                                'RL_B_Total_Activity': "N/A", 'RL_B_Insufficient_Data': "N/A",
                                'RL_C_Health': "N/A", 'RL_C_Score': "N/A",
                                'RL_C_FV_Ratio_Pct': "N/A", 'RL_C_BTR_Pct': "N/A",
                                'RL_C_SZR_Pct': "N/A", 'RL_C_CoG': "N/A",
                                'RL_C_CoG_Override': "N/A", 'RL_C_TSM': "N/A",
                                'RL_C_Total_Activity': "N/A", 'RL_C_Insufficient_Data': "N/A",
                                'RL_D_Health': "N/A", 'RL_D_Score': "N/A",
                                'RL_D_FV_Ratio_Pct': "N/A", 'RL_D_BTR_Pct': "N/A",
                                'RL_D_SZR_Pct': "N/A", 'RL_D_CoG': "N/A",
                                'RL_D_CoG_Override': "N/A", 'RL_D_TSM': "N/A",
                                'RL_D_Total_Activity': "N/A", 'RL_D_Insufficient_Data': "N/A",
                                'VSWR_risk_assessment': "N/A",
                                'Active_VSWR_Alarm': "N/A (No active VSWR alarm)",
                                'Historic_VSWR_Alarm': "N/A (No historic VSWR alarm)"
                            }
                            rru_records.append(record)
                    except:
                        continue

                # 2. VSWR branch
                branch_match = vswr_branch_pattern.search(line)
                if branch_match:
                    try:
                        current_bxp    = branch_match.group(1) or None
                        current_branch = branch_match.group(3) or None
                        if current_branch not in ["1", "2", "3", "4"]:
                            current_bxp = None; current_branch = None
                    except:
                        current_bxp = None; current_branch = None
                    continue

                # 3. VSWR value
                if current_bxp and current_branch in ["1", "2", "3", "4"]:
                    try:
                        val_match = vswr_value_pattern.search(line)
                        if val_match and val_match.group(1) == current_bxp:
                            bxp_vswr[current_bxp][current_branch] = str(val_match.group(2)) or "N/A"
                            current_bxp = None; current_branch = None; continue
                        err_match = vswr_error_pattern.search(line)
                        if err_match and err_match.group(1) == current_bxp:
                            bxp_vswr[current_bxp][current_branch] = err_match.group(2).strip() or "N/A"
                            current_bxp = None; current_branch = None; continue
                    except:
                        current_bxp = None; current_branch = None; continue

                # 4. Active VSWR alarms from 'alt' alarm table only.
                # The alt block is bounded:
                #   Start : line matching "<node>> alt"
                #   End   : line matching "Total:" (e.g. ">>> Total: 4 Alarms")
                # Only VSWR Over Threshold lines inside this window are collected.
                try:
                    if node_prompt_pattern.search(line):
                        in_alt_block = True
                    elif in_alt_block and 'Total:' in line and 'Alarms' in line:
                        in_alt_block = False
                    elif in_alt_block and "VSWR Over Threshold" in line and "FieldReplaceableUnit=RRU-" in line:
                        m_rru = rru_from_alarm_pattern.search(line)
                        if m_rru:
                            rru_active_vswr[m_rru.group(1)].append(line.strip())
                except:
                    pass

                # 5. Historic VSWR alarms from 'lgar' command block
                try:
                    if lgar_prompt_pattern.search(line):
                        in_lgar_block = True
                    elif in_lgar_block and any_prompt_pattern.search(line) and not lgar_prompt_pattern.search(line):
                        in_lgar_block = False
                    elif in_lgar_block and "VSWR Over Threshold" in line and "FieldReplaceableUnit=RRU-" in line:
                        m_rru = rru_from_alarm_pattern.search(line)
                        if m_rru:
                            rru_historic_vswr[m_rru.group(1)].append(line.strip())
                except:
                    pass

                # 6. Event 50 RL detail — primary pattern
                rl_detail_match = rl_detail_pattern.search(line)
                if rl_detail_match:
                    try:
                        bxp_num  = rl_detail_match.group(1)
                        time_str = rl_detail_match.group(2)
                        rl_group = rl_detail_match.group(3)

                        def fv_val(s):
                            parts = s.split(',')
                            return parts[1] if len(parts) > 1 else "N/A"

                        rl_21_list = rl_detail_match.group(4).split(',')
                        rl_21_list = (rl_21_list + ['0'] * (21 - len(rl_21_list)))[:21]

                        entry = {
                            'time':   time_str,
                            'fields': rl_21_list,
                            'fv':     fv_val(rl_detail_match.group(5)),
                            'fv_1':   fv_val(rl_detail_match.group(6)),
                            'fv_2':   fv_val(rl_detail_match.group(7)),
                            'fv_3':   fv_val(rl_detail_match.group(8)),
                        }

                        # Most recent per branch — for classification.
                        # File read top-to-bottom: last assignment = most recent.
                        bxp_rl_detail[bxp_num][rl_group] = entry

                        # ALL entries per branch — for Last_RL_ABCD_4Ports.
                        bxp_rl_history[bxp_num][rl_group].append((time_str, entry))

                    except:
                        continue

                # 7. RL fallback pattern
                try:
                    rl_match = rl_pattern.match(line)
                    if rl_match:
                        bxp_num = rl_match.group(1) or None
                        port    = rl_match.group(2) or None
                        if bxp_num and port in ['A', 'B', 'C', 'D']:
                            time_part_match = re.search(r'\[(.+?)\]', line)
                            time_part = time_part_match.group(1) if time_part_match else ""
                            rl_full_line = (
                                f"{bxp_num}: [{time_part}] 50: RL Statistic Counters: "
                                f"{port};{rl_match.group(3) or ''}"
                            )
                            bxp_rl_all[bxp_num].append((line_num, port, rl_full_line))
                except:
                    continue

            except:
                continue

        # ── Build Last_RL_ABCD_4Ports — 4 most recent per branch independently ──
        bxp_last_rl_4 = {}
        for bxp_num, branches in bxp_rl_history.items():
            branch_blocks = []
            for branch in ['A', 'B', 'C', 'D']:
                entries = branches.get(branch, [])
                if not entries:
                    branch_blocks.append(f"Branch {branch}: N/A (no RL record)")
                    continue

                # Deduplicate by timestamp
                seen_ts = {}
                for ts, entry in entries:
                    if ts not in seen_ts:
                        seen_ts[ts] = entry

                # Sort descending, take top 4
                recent = sorted(seen_ts.items(), key=lambda x: x[0], reverse=True)[:LAST_RL_HISTORY]

                labels    = ['Latest', '-W1', '-W2', '-W3']
                out_lines = [f"Branch {branch} ({len(recent)} reports):"]
                for i, (ts, e) in enumerate(recent):
                    label  = labels[i] if i < len(labels) else f'-W{i}'
                    rl_str = ','.join(str(v) for v in e['fields'])
                    out_lines.append(
                        f"  [{label}][{ts}] RL:21:{rl_str} | "
                        f"FV:{e['fv']} FV1:{e['fv_1']} FV2:{e['fv_2']} FV3:{e['fv_3']}"
                    )
                branch_blocks.append('\n'.join(out_lines))

            bxp_last_rl_4[bxp_num] = '\n\n'.join(branch_blocks)

        # Fill records and run classification
        for record in rru_records:
            try:
                bxp_num   = record['BXP_number']
                vswr_dict = bxp_vswr.get(bxp_num, {})

                record['VSWR_1_dB'] = vswr_dict.get("1", "N/A")
                record['VSWR_2_dB'] = vswr_dict.get("2", "N/A")
                record['VSWR_3_dB'] = vswr_dict.get("3", "N/A")
                record['VSWR_4_dB'] = vswr_dict.get("4", "N/A")

                # 4 most recent Event 50 entries per branch independently
                record['Last_RL_ABCD_4Ports'] = bxp_last_rl_4.get(bxp_num, "N/A (no RL record)")

                # REMOVED: record['VSWR_Alarm_Above_Last_RL'] (col M)
                # REMOVED: record['VSWR_Alarm_Count']         (col N)

                # Classification fields — most recent entry per branch (unchanged)
                rl_detail = bxp_rl_detail.get(bxp_num, {})
                for rl_group in ['A', 'B', 'C', 'D']:
                    group_data = rl_detail.get(rl_group, {})
                    if not group_data:
                        continue
                    record[f'RL_{rl_group}_time'] = group_data.get('time', "N/A")
                    for i in range(21):
                        record[f'RL_{rl_group}_field_{i+1}'] = group_data.get('fields', ["N/A"]*21)[i]
                    record[f'RL_{rl_group}_FV']   = group_data.get('fv',  "N/A")
                    record[f'RL_{rl_group}_FV_1'] = group_data.get('fv_1',"N/A")
                    record[f'RL_{rl_group}_FV_2'] = group_data.get('fv_2',"N/A")
                    record[f'RL_{rl_group}_FV_3'] = group_data.get('fv_3',"N/A")

                branch_results = classify_unit_rl(record)
                record['Unit_Worst_Status'] = get_unit_worst_status(branch_results)

                for branch, result in branch_results.items():
                    if result is None:
                        continue
                    record[f'RL_{branch}_Health']             = result['health_status']
                    record[f'RL_{branch}_Score']              = result['health_score']
                    record[f'RL_{branch}_FV_Ratio_Pct']      = result['fv_ratio_pct']
                    record[f'RL_{branch}_BTR_Pct']           = result['btr_pct']
                    record[f'RL_{branch}_SZR_Pct']           = result['szr_pct']
                    record[f'RL_{branch}_CoG']               = result['cog']
                    record[f'RL_{branch}_CoG_Override']      = '⚠️ Yes' if result['cog_override'] else 'No'
                    record[f'RL_{branch}_TSM']               = result['tsm']
                    record[f'RL_{branch}_Total_Activity']    = result['total_activity']
                    record[f'RL_{branch}_Insufficient_Data'] = '⚠️ Yes (<1 day)' if result['insufficient_data'] else 'No'

                record['VSWR_risk_assessment'] = assess_vswr_risk(record)

                # Fill Active_VSWR_Alarm — matched by RU_number (RRU-X)
                rru_num = record.get('RU_number', 'N/A')
                active_alarms = rru_active_vswr.get(rru_num, [])
                record['Active_VSWR_Alarm'] = (
                    '\n'.join(active_alarms)
                    if active_alarms
                    else "N/A (No active VSWR alarm)"
                )

                # Fill Historic_VSWR_Alarm — matched by RU_number (RRU-X)
                historic_alarms = rru_historic_vswr.get(rru_num, [])
                record['Historic_VSWR_Alarm'] = (
                    '\n'.join(historic_alarms)
                    if historic_alarms
                    else "N/A (No historic VSWR alarm)"
                )

            except:
                continue

    except Exception as e:
        print(f"⚠️  File processing warning {file_path}: {str(e)}")
        return []

    return rru_records


def batch_extract():
    start_time = time.time()
    all_records = []
    file_list   = []

    parser = argparse.ArgumentParser(description="RRU BXP RL50 Health Extractor")
    parser.add_argument("--input", default=None,
                        help="Folder containing .log files (default: script's own folder)")
    args = parser.parse_args()

    if args.input:
        SCRIPT_FOLDER = os.path.abspath(args.input)
    else:
        SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

    if not os.path.isdir(SCRIPT_FOLDER):
        print(f"❌ Input folder not found: {SCRIPT_FOLDER}")
        return

    print(f"🔍 Auto-scanning folder: {SCRIPT_FOLDER}")
    print("=" * 60)

    for root, dirs, files in os.walk(SCRIPT_FOLDER):
        for filename in files:
            if filename.lower().endswith(".log") and not filename.startswith('.'):
                file_path = os.path.join(root, filename)
                file_list.append(file_path)
                print(f"✅ Found log file: {os.path.basename(file_path)}")

    total_files = len(file_list)
    print("=" * 60)
    print(f"📊 Found {total_files} log files. Starting extraction...")
    print("=" * 60)

    success_count = 0
    fail_count    = 0
    for idx, file_path in enumerate(file_list, 1):
        try:
            if idx % 5 == 0:
                print(f"[{idx}/{total_files}] Processing: {os.path.basename(file_path)}")
            rru_records = extract_core_info(file_path)
            all_records.extend(rru_records)
            success_count += 1
        except Exception as e:
            fail_count += 1
            print(f"⚠️  Skipping file: {os.path.basename(file_path)} (reason: {str(e)[:50]}...)")
            continue

    seen_global    = set()
    unique_records = []
    for record in all_records:
        try:
            key = (record['RU_number'], record['BXP_number'], record['serial_number'])
            if key not in seen_global and record['RU_number'] != "N/A" and record['BXP_number'] != "N/A":
                seen_global.add(key)
                unique_records.append(record)
        except:
            continue

    def get_base_status(status_str):
        for s in ['🚨 Critical/Failed', '🔴 Late Degradation', '🔶 Middle Degradation',
                  '⚠️ Early Degradation', '✅ Healthy', '⚪ No Data']:
            if status_str.startswith(s):
                return s
        return status_str

    status_counts = defaultdict(int)
    for record in unique_records:
        base = get_base_status(record.get('Unit_Worst_Status', 'N/A'))
        status_counts[base] += 1

    vswr_risk_count     = sum(1 for r in unique_records if r.get('VSWR_risk_assessment') == 'Risk unit')
    vswr_non_risk_count = len(unique_records) - vswr_risk_count

    print("\n" + "=" * 60)
    print("📋 NETWORK-WIDE RL HEALTH SUMMARY")
    print("=" * 60)
    for status in ['🚨 Critical/Failed', '🔴 Late Degradation', '🔶 Middle Degradation',
                   '⚠️ Early Degradation', '✅ Healthy', '⚪ No Data', 'N/A']:
        count = status_counts.get(status, 0)
        if count > 0:
            print(f"   {status}: {count} units")
    print(f"   VSWR Risk unit: {vswr_risk_count} units")
    print(f"   VSWR Non-risk unit: {vswr_non_risk_count} units")
    print(f"   Total units: {len(unique_records)}")
    print("=" * 60)

    def branch_fields(b):
        return [
            f'RL_{b}_time',
            *[f'RL_{b}_field_{i}' for i in range(1, 22)],
            f'RL_{b}_FV', f'RL_{b}_FV_1', f'RL_{b}_FV_2', f'RL_{b}_FV_3',
            f'RL_{b}_Health', f'RL_{b}_Score',
            f'RL_{b}_FV_Ratio_Pct', f'RL_{b}_BTR_Pct', f'RL_{b}_SZR_Pct',
            f'RL_{b}_CoG', f'RL_{b}_CoG_Override',
            f'RL_{b}_TSM', f'RL_{b}_Total_Activity', f'RL_{b}_Insufficient_Data'
        ]

    # REMOVED: 'VSWR_Alarm_Above_Last_RL' and 'VSWR_Alarm_Count' from fieldnames
    fieldnames = [
        'source_file', 'full_file_path',
        'RU_number', 'BXP_number', 'RU_model', 'serial_number',
        'Unit_Field_Deployment_Date',
        'VSWR_1_dB', 'VSWR_2_dB', 'VSWR_3_dB', 'VSWR_4_dB',
        'Last_RL_ABCD_4Ports',
        'VSWR_risk_assessment',
        'Active_VSWR_Alarm',
        'Historic_VSWR_Alarm',
        'Unit_Worst_Status',
        *branch_fields('A'), *branch_fields('B'),
        *branch_fields('C'), *branch_fields('D'),
    ]

    output_file = os.path.join(SCRIPT_FOLDER, "RRU_BXP_RL50_Health_Result.csv")
    try:
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(unique_records)
        print(f"\n📄 Full result exported: {output_file}")
    except Exception as e:
        print(f"⚠️  Export failed: {str(e)}")

    alert_statuses = {'🚨 Critical/Failed', '🔴 Late Degradation',
                      '🔶 Middle Degradation', '⚠️ Early Degradation'}
    alert_records = [
        r for r in unique_records
        if get_base_status(r.get('Unit_Worst_Status', '')) in alert_statuses
    ]
    alert_output_file = os.path.join(SCRIPT_FOLDER, "RRU_BXP_RL50_ALERT_Units.csv")
    try:
        with open(alert_output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(alert_records)
        print(f"🚨 Alert units exported: {alert_output_file} ({len(alert_records)} units)")
    except Exception as e:
        print(f"⚠️  Alert export failed: {str(e)}")

    run_time = round(time.time() - start_time, 2)
    print("=" * 60)
    print(f"✅ Extraction & Classification Complete!")
    print(f"   📁 Scanned folder        : {SCRIPT_FOLDER}")
    print(f"   📋 Log files found       : {total_files}")
    print(f"   ✅ Successfully processed: {success_count}")
    print(f"   ❌ Skipped files         : {fail_count}")
    print(f"   📊 Valid records         : {len(unique_records)}")
    print(f"   🚨 Alert units           : {len(alert_records)}")
    print(f"   📡 VSWR Risk units       : {vswr_risk_count}")
    print(f"   🕒 Total time            : {run_time} seconds")
    print(f"   📄 Full result           : {output_file}")
    print(f"   🚨 Alert result          : {alert_output_file}")
    print("=" * 60)

# The algorithm in this program is from Claude.
if __name__ == "__main__":
    batch_extract()
