import time
import re
import argparse
from collections import defaultdict
from pathlib import Path

import pandas as pd

# Попытка импортировать gspread/Google, но не падаем, если нет (для локального fallback)
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.utils import rowcol_to_a1
    HAS_GOOGLE = True
except ImportError:
    HAS_GOOGLE = False

# === Параметры расписания ===
RAND_START_H = 0.3
WORK_H = 5
RAND_DUR_H = 1.5

rand_start = RAND_START_H * 60
work = WORK_H * 60
rand_dur = RAND_DUR_H * 60
DAY = 24 * 60

# Ограничения
SAME_ID_GAP = 96  # одинаковые ID: минимум 96 минут между базовыми стартами
DIFF_ID_GAP = 1   # разные ID: минимум 1 минута между разными ID
MIN_BASE = int(rand_start)
MAX_BASE = int(DAY - (rand_start + work + rand_dur))

# === Вспомогательные ===
def worst_interval(base):
    earliest = max(0, base - rand_start)
    latest = min(DAY, base + rand_start + work + rand_dur)
    return earliest, latest

def max_overlap(intervals):
    events = []
    for s, e in intervals:
        if e <= s:
            continue
        events.append((s, 1))
        events.append((e, -1))
    events.sort(key=lambda x: (x[0], -x[1]))
    curr = peak = 0
    for _, delta in events:
        curr += delta
        if curr > peak:
            peak = curr
    return peak

def minute_to_hhmm(m):
    h = int(m // 60)
    mi = int(m % 60)
    return f"{h:02d}:{mi:02d}"

def extract_identifier(raw):
    if isinstance(raw, str):
        if "ac=*-" in raw:
            return raw.split("ac=*-")[-1].strip()
        return raw.strip()
    return raw

def ci(name, columns):
    for c in columns:
        if c.strip().lower() == name.strip().lower():
            return c
    return None

# === CLI ===
parser = argparse.ArgumentParser(description="Подбор базовых стартов и экспорт результата.")
parser.add_argument("--local", "-l", help="Локальный входной CSV (dry-run), например sample_input.csv", type=str)
parser.add_argument("--output", "-o", help="Куда экспортировать результат CSV", default="result.csv")
parser.add_argument("--sheet", "-s", help="URL или ключ Google Sheets (если не указан, берётся встроенный)", default=None)
args = parser.parse_args()
output_csv = args.output
local_csv = args.local

# === Загрузка данных ===
df = None
using_google = False
ws = None

def load_from_csv(path):
    df_local = pd.read_csv(path)
    return df_local

def try_load_google(sheet_url_or_key):
    if not HAS_GOOGLE:
        raise RuntimeError("Модули gspread/google-auth не установлены.")
    SERVICE_ACCOUNT_FILE = "creds.json"
    SPREADSHEET = sheet_url_or_key or "https://docs.google.com/spreadsheets/d/1KZxEHdnZ4vj_2IXOS0ABuxwT3hMyNKqsym1I-Zm1oUM/edit"
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    if SPREADSHEET.startswith("http"):
        sh = gc.open_by_url(SPREADSHEET)
    else:
        sh = gc.open_by_key(SPREADSHEET)
    worksheet = sh.sheet1
    return worksheet

# 1. Попробуем Google, если не принудительно локальный
if local_csv is None:
    try:
        ws = try_load_google(args.sheet)
        using_google = True
        # обеспечить заголовки (если нужно)
        headers = ws.row_values(1)
        def ensure_column(name):
            global headers
            if not any(h.strip().lower() == name.strip().lower() for h in headers):
                ws.update_cell(1, len(headers) + 1, name)
                headers = ws.row_values(1)
        ensure_column("NAME")
        ensure_column("IDENTIFICATOR")
        ensure_column("TIME START")
        ensure_column("earliest_start")
        ensure_column("latest_end")
        records = ws.get_all_records()
        df = pd.DataFrame(records)
        print("Данные загружены из Google Sheets.")
    except Exception as e:
        print(f"[!] Не удалось подключиться к Google Sheets ({e}), переключаемся на локальный input.")
        if local_csv:
            df = load_from_csv(local_csv)
        else:
            fallback = Path("sample_input.csv")
            if fallback.exists():
                df = load_from_csv(fallback)
                print(f"Использую {fallback} как локальный вход.")
            else:
                raise RuntimeError("Нет локального CSV для fallback (sample_input.csv не найден и --local не передан).")
else:
    # принудительно локальный режим
    if not Path(local_csv).exists():
        raise RuntimeError(f"Файл {local_csv} не найден.")
    df = load_from_csv(local_csv)
    print(f"Данные загружены из локального файла {local_csv}.")

# Проверим, что df загружен
if df is None:
    raise RuntimeError("Не удалось загрузить входные данные ни откуда.")

# === Подготовка списка программ ===
col_NAME = ci("NAME", df.columns)
col_IDENT = ci("IDENTIFICATOR", df.columns) or ci("identificator", df.columns)
# TIME START и окна могут быть созданы позже, не нужны для расчетов
if not col_NAME or not col_IDENT:
    raise RuntimeError(f"Не найдены колонки NAME/IDENTIFICATOR. Есть: {list(df.columns)}")

# Отбираем строки, где NAME начинается с program
is_program_mask = df[col_NAME].astype(str).str.match(r'^\s*program\d+', flags=re.IGNORECASE)
program_df = df[is_program_mask].copy()
if program_df.empty:
    raise RuntimeError("Не найдено ни одной строки с NAME, начинающимся на 'program'.")

programs = []
program_indices = []
for idx, row in program_df.iterrows():
    name = row[col_NAME]
    ident_raw = row[col_IDENT]
    ident = extract_identifier(ident_raw)
    programs.append((name, ident))
    program_indices.append(idx)

# Сортировка по частоте ID
id_count = defaultdict(int)
for _, pid in programs:
    id_count[pid] += 1
order = sorted(range(len(programs)), key=lambda i: -id_count[programs[i][1]])
ordered_programs = [programs[i] for i in order]

solution = None

# === Жадный подбор ===
print("Жадный подбор базовых стартов по отфильтрованным программам...")
assigned_ordered = []
for idx, (name, pid) in enumerate(ordered_programs):
    found = False
    for base in range(MIN_BASE, MAX_BASE + 1):
        ok = True
        for prev_idx, (prev_name, prev_pid) in enumerate(ordered_programs[:idx]):
            prev_base = assigned_ordered[prev_idx]
            if prev_pid == pid:
                if abs(prev_base - base) < SAME_ID_GAP:
                    ok = False; break
            else:
                if abs(prev_base - base) < DIFF_ID_GAP:
                    ok = False; break
            if abs(prev_base - base) <= (rand_start + rand_start):
                ok = False; break
        if not ok:
            continue
        partial = [worst_interval(b) for b in assigned_ordered] + [worst_interval(base)]
        if max_overlap(partial) > 4:
            continue
        assigned_ordered.append(base)
        print(f"  {name} (ID={pid}) -> базовый старт {minute_to_hhmm(base)}")
        found = True
        break
    if not found:
        print(f"Жадный не смог для {name} (ID={pid}), переключаюсь на backtracking.")
        break

# Привязка к исходному порядку
if len(assigned_ordered) == len(ordered_programs):
    solution = [None] * len(programs)
    for ord_pos, prog_index in enumerate(order):
        solution[prog_index] = assigned_ordered[ord_pos]
else:
    print("Ограниченный backtracking...")
    start_time = time.time()
    TIME_LIMIT = 30

    def backtrack(assigned, idx):
        global solution
        if solution is not None:
            return
        if time.time() - start_time > TIME_LIMIT:
            return
        if idx == len(ordered_programs):
            base_list = [None] * len(programs)
            for ord_pos, prog_index in enumerate(order):
                base_list[prog_index] = assigned[ord_pos]
            for i in range(len(programs)):
                for j in range(i + 1, len(programs)):
                    if programs[i][1] == programs[j][1] and abs(base_list[i] - base_list[j]) < SAME_ID_GAP:
                        return
            if max_overlap([worst_interval(b) for b in base_list]) > 4:
                return
            solution = base_list
            return
        name, pid = ordered_programs[idx]
        for base in range(MIN_BASE, MAX_BASE + 1):
            ok = True
            for prev in range(idx):
                prev_name, prev_pid = ordered_programs[prev]
                prev_base = assigned[prev]
                if prev_pid == pid:
                    if abs(prev_base - base) < SAME_ID_GAP:
                        ok = False; break
                else:
                    if abs(prev_base - base) < DIFF_ID_GAP:
                        ok = False; break
                if abs(prev_base - base) <= (rand_start + rand_start):
                    ok = False; break
            if not ok:
                continue
            partial = [worst_interval(assigned[j]) for j in range(idx)] + [worst_interval(base)]
            if max_overlap(partial) > 4:
                continue
            assigned.append(base)
            backtrack(assigned, idx + 1)
            assigned.pop()
            if solution is not None:
                return

    backtrack([], 0)

if solution is None:
    raise RuntimeError("Не удалось подобрать допустимое расписание ни жадно, ни backtracking-ом.")

# === Подготовка экспортов ===
times = [minute_to_hhmm(b) for b in solution]
earliest_list = []
latest_list = []
for b in solution:
    e, l = worst_interval(b)
    earliest_list.append(minute_to_hhmm(e))
    latest_list.append(minute_to_hhmm(l))

# Заполняем результат в DataFrame (в исходную df)
# создаём/обновляем колонки если нет
if "TIME START" not in df.columns:
    df["TIME START"] = ""
if "earliest_start" not in df.columns:
    df["earliest_start"] = ""
if "latest_end" not in df.columns:
    df["latest_end"] = ""

for sol_idx, df_row_idx in enumerate(program_indices):
    df.at[df_row_idx, "TIME START"] = times[sol_idx]
    df.at[df_row_idx, "earliest_start"] = earliest_list[sol_idx]
    df.at[df_row_idx, "latest_end"] = latest_list[sol_idx]

# === Обновление Google Sheets, если применимо ===
if using_google and HAS_GOOGLE and ws is not None:
    # Обновить только соответствующие ячейки через batch_update
    headers = ws.row_values(1)
    def find_col(name):
        for i, h in enumerate(headers, start=1):
            if h.strip().lower() == name.strip().lower():
                return i
        return None

    col_time_idx = find_col("TIME START")
    col_earliest_idx = find_col("earliest_start")
    col_latest_idx = find_col("latest_end")

    if not col_time_idx or not col_earliest_idx or not col_latest_idx:
        print("[!] Не удалось найти нужные колонки в Google Sheets для записи.")
    else:
        batch_data = []
        for sol_idx, df_row_idx in enumerate(program_indices):
            sheet_row = df_row_idx + 2  # get_all_records пропускает заголовок
            base = solution[sol_idx]
            hhmm = minute_to_hhmm(base)
            e, l = worst_interval(base)
            earliest_str = minute_to_hhmm(e)
            latest_str = minute_to_hhmm(l)
            batch_data.append({
                "range": rowcol_to_a1(sheet_row, col_time_idx),
                "values": [[hhmm]]
            })
            batch_data.append({
                "range": rowcol_to_a1(sheet_row, col_earliest_idx),
                "values": [[earliest_str]]
            })
            batch_data.append({
                "range": rowcol_to_a1(sheet_row, col_latest_idx),
                "values": [[latest_str]]
            })
        try:
            ws.batch_update(batch_data, value_input_option="USER_ENTERED")
            print("Готово: TIME START, earliest_start и latest_end обновлены в Google Sheets.")
        except Exception as e:
            print(f"[!] Ошибка при записи в Google Sheets: {e}")

# === Локальный экспорт в CSV ===
export_rows = []
for i, (name, ident) in enumerate(programs):
    base = solution[i]
    if base is None:
        continue
    export_rows.append({
        "NAME": name,
        "IDENTIFICATOR": ident,
        "TIME START": minute_to_hhmm(base),
        "earliest_start": earliest_list[i],
        "latest_end": latest_list[i],
    })

export_df = pd.DataFrame(export_rows)
out_path = Path(output_csv)
export_df.to_csv(out_path, index=False, encoding="utf-8")
print(f"Готово: локальный файл с расписанием сохранён в {out_path.resolve()}")
