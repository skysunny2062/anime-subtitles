import os
import sys
try:
    from tqdm import tqdm
except ImportError:
    print("正在安裝pip:tqdm")
    import subprocess
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "tqdm", 
        "--quiet", 
        "--no-warn-script-location", 
        "--disable-pip-version-check"
    ])
    os.execv(sys.executable, [sys.executable] + sys.argv)
import argparse
import csv
import ctypes
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

PROGRAM_NAME = "clean_ass Zack Ver260322"
BACKUP_DIR = "original"
ENCODINGS  = ["utf-8-sig", "utf-8", "utf-16", "utf-16-le", "utf-16-be", "big5", "gbk"]
EXTENSIONS = {".ass", ".ssa"}
TARGET_SECTIONS = {"[Aegisub Extradata]", "[Aegisub Project Garbage]"}
INFO_GARBAGE_KEYS = {
    "aegisub video aspect ratio", "audio uri", "comment",
    "original editing", "original effect", "original ripper",
    "original timing", "original traditional chinese", "original translation",
    "original revising", "playdepth", "script updated by", "sub by",
    "synch point", "timer", "update details", "video aspect ratio",
    "video color matrix", "video zoom",
}
WORKERS    = 8


class QueueWriter:
    def __init__(self, path: str):
        self._q = Queue()
        self._thread = threading.Thread(target=self._worker, args=(path,), daemon=True)
        self._thread.start()

    def write(self, content: str):
        self._q.put(content)

    def close(self):
        self._q.put(None)
        self._thread.join()

    def _worker(self, path: str):

        first_item = self._q.get()
        if first_item is None:
            return
            
        with open(path, "a", encoding="utf-8") as f:
            f.write(first_item)
            f.flush()
            for item in iter(self._q.get, None):
                f.write(item)
                f.flush()


def read_file_auto_encoding(path: str):
    for enc in ENCODINGS:
        try:
            with open(path, "r", encoding=enc) as f:
                lines = f.readlines()
            if any("\x00" in l for l in lines):
                raise UnicodeDecodeError(enc, b"", 0, 1, "null byte")
            return lines, enc
        except UnicodeDecodeError:
            continue
    raise ValueError(f"無法辨識編碼：{path}")


def remove_garbage(lines: list[str]):
    new_lines, removed_lines = [], []
    current_section = ""
    skip_section = False

    for line in lines:
        stripped = line.strip()
        is_header = stripped.startswith("[") and stripped.endswith("]")

        if is_header:
            current_section = stripped
            skip_section = stripped in TARGET_SECTIONS

        if skip_section:
            removed_lines.append(line)
            continue

        if current_section == "[Script Info]" and not is_header and stripped:
            if stripped.startswith(";"):
                removed_lines.append(line)
                continue
            if ":" in stripped:
                key = stripped.split(":", 1)[0].strip().lower()
                if key in INFO_GARBAGE_KEYS:
                    removed_lines.append(line)
                    continue

        new_lines.append(line)

    return new_lines, removed_lines


def atomic_write(path: str, lines: list[str], encoding: str):
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding=encoding) as f:
            f.writelines(lines)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def backup_file(backup_dir: str, root_dir: str, file_path: str):
    dest = os.path.join(backup_dir, os.path.relpath(file_path, root_dir))
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(file_path, dest)


def process_file(root_dir: str, file_path: str, backup_dir: str, log_writer: QueueWriter):
    relative = os.path.relpath(file_path, root_dir)
    try:
        lines, encoding = read_file_auto_encoding(file_path)
        cleaned, removed = remove_garbage(lines)

        if removed:
            backup_file(backup_dir, root_dir, file_path)
            atomic_write(file_path, cleaned, encoding)
            log_writer.write(f"\n{'='*60}\nFILE: {relative}\n{'='*60}\n{''.join(removed)}\n")

        return len(removed), sum(len(l.encode()) for l in removed), len(lines), None

    except Exception as e:
        log_writer.write(f"\n[ERROR] {relative}: {e}\n")
        return 0, 0, 0, str(e)


def scan_files(root_dir: str, backup_dir: str):
    backup_abs = os.path.abspath(backup_dir)
    files = []
    for root, dirs, filenames in os.walk(root_dir):
        root_abs = os.path.abspath(root)
        if root_abs == backup_abs or root_abs.startswith(backup_abs + os.sep):
            dirs.clear()
            continue
        files.extend(
            os.path.join(root, f) for f in filenames
            if os.path.splitext(f)[1].lower() in EXTENSIONS
        )
    return files


def main():
    try:
        if len(sys.argv) > 1:
            root_dir = os.path.abspath(sys.argv[1])
        else:
            root_dir = os.path.abspath(os.getcwd())

        backup_dir = os.path.join(root_dir, "original")
        log_path   = os.path.join(root_dir, "clean_ass_log.txt")
        stats_csv  = os.path.join(root_dir, "clean_ass_statistics.csv")

        if not os.path.exists(root_dir):
            print(f"找不到指定的路徑：{root_dir}")
            return
        print(f"目標目錄：{root_dir}") 

        for p in (log_path, stats_csv):
            if os.path.exists(p):
                os.remove(p)

        files = scan_files(root_dir, backup_dir)
        if not files:
            print("找不到字幕檔案。")
            return
        print(f"找到 {len(files)} 個字幕檔案，開始處理...\n")

        log_writer = QueueWriter(log_path)
        total_removed_lines = total_removed_bytes = total_original_lines = error_count = 0
        per_file_stats = []

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            results = executor.map(
                lambda f: process_file(root_dir, f, backup_dir, log_writer), files
            )
            for (r_lines, r_bytes, orig_lines, err), f in zip(
                tqdm(results, total=len(files), desc="Processing", unit="file"), files
            ):
                total_removed_lines  += r_lines
                total_removed_bytes  += r_bytes
                total_original_lines += orig_lines
                error_count          += bool(err)

                if r_lines > 0 or err:
                    per_file_stats.append({
                        "file": os.path.relpath(f, root_dir),
                        "removed_lines": r_lines,
                        "removed_bytes": r_bytes,
                        "total_lines":   orig_lines,
                        "error":         err or "",
                    })

        log_writer.close()

        print(f"\n  已移除行數：{total_removed_lines}")
        print(f"  已移除大小：{total_removed_bytes / 1024:.2f} KB")
        print(f"  掃描總行數：{total_original_lines}")
        if error_count:
            print(f"  錯誤檔案數：{error_count}（詳見 {log_path}）")

        if per_file_stats:
            with open(stats_csv, "w", newline="", encoding="utf-8") as f:
                fieldnames = ["file", "removed_lines", "removed_bytes", "total_lines", "error"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow({
                    "file":          "檔案路徑",
                    "removed_lines": "刪除行數",
                    "removed_bytes": "刪除大小(bytes)",
                    "total_lines":   "原始總行數",
                    "error":         "錯誤訊息",
                })
                writer.writerows(per_file_stats)

            print(f"\n統計Log → {stats_csv}")
            print(f"刪除Log → {log_path}")
        else:
            print("\n✔️ 所有字幕檔皆已是乾淨狀態，無任何更動")

    except Exception as e:
        print(f"\n[發生嚴重錯誤]: {e}")
    finally:
        print("\n按任意鍵退出...")
        os.system("pause >nul")

if __name__ == "__main__":
    ctypes.windll.kernel32.SetConsoleTitleW(f'{PROGRAM_NAME}')
    os.system("cls")
    os.system("COLOR 0B")
    main()