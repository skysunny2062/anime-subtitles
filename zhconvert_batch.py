import argparse
import concurrent.futures
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterable


DEFAULT_ENDPOINT = "https://api.zhconvert.org/convert"
DEFAULT_EXTENSIONS = (".ass", ".ssa", ".srt")
ENCODINGS = ("utf-8-sig", "utf-8", "utf-16", "utf-16-le", "utf-16-be", "big5", "gbk")
# ASS_EVENT_TYPES = ("Dialogue", "Comment")
# ASS_TEXT_SENTINEL = "\n<<<ZHCONVERT_ASS_TEXT_SPLIT>>>\n"
# ASS_BATCH_SIZE = 150


@dataclass
class FileText:
    text: str
    encoding: str
    newline: str
    had_final_newline: bool


# @dataclass
# class AssEventText:
#     line_index: int
#     fields: list[str]
#     text_index: int


class RateLimiter:
    def __init__(self, max_requests_per_second: float):
        self.interval = 0.0 if max_requests_per_second <= 0 else 1.0 / max_requests_per_second
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait(self) -> None:
        if self.interval <= 0:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                if now >= self.next_allowed:
                    self.next_allowed = now + self.interval
                    return
                sleep_for = self.next_allowed - now
            time.sleep(sleep_for)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="批次將字幕檔送到繁化姬 API 做台灣化或其他轉換。"
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="要處理的資料夾或單一字幕檔，預設為目前目錄",
    )
    parser.add_argument(
        "--extensions",
        nargs="+",
        default=list(DEFAULT_EXTENSIONS),
        help="要處理的副檔名，預設為 .ass .ssa .srt",
    )
    parser.add_argument(
        "--converter",
        default="Taiwan",
        help="繁化姬 converter，預設 Taiwan",
    )
    parser.add_argument(
        "--api-endpoint",
        default=DEFAULT_ENDPOINT,
        help=f"繁化姬 API 端點，預設 {DEFAULT_ENDPOINT}",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="同時處理的檔案數，免費 API 建議先從 1 開始",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="單次 API 請求逾時秒數，預設 60",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="API 失敗時重試次數，預設 2",
    )
    parser.add_argument(
        "--max-requests-per-second",
        type=float,
        default=10,
        help="每秒最大 API 請求數，預設 10；設 0 代表不限制",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只檢查有哪些檔案會改變，不回寫檔案",
    )
    parser.add_argument(
        "--backup-dir",
        default="",
        help="回寫前先備份原檔到指定資料夾；留空則不另存備份",
    )
    parser.add_argument(
        "--ignore-text-styles",
        default="",
        help='忽略的 ASS 樣式名，例如 "OPJP,OPCN,EDJP"',
    )
    parser.add_argument(
        "--jp-text-styles",
        default="",
        help='指定視為日文的 ASS 樣式名，例如 "OPJP,EDJP,*noAutoJpTextStyles"',
    )
    parser.add_argument(
        "--jp-style-conversion-strategy",
        default="protectOnlySameOrigin",
        choices=("none", "protect", "protectOnlySameOrigin", "fix"),
        help="日文樣式處理策略",
    )
    parser.add_argument(
        "--jp-text-conversion-strategy",
        default="protectOnlySameOrigin",
        choices=("none", "protect", "protectOnlySameOrigin", "fix"),
        help="自動偵測日文區段的處理策略",
    )
    parser.add_argument(
        "--user-protect-replace-file",
        default="",
        help="保護字詞清單檔案，每行一個詞",
    )
    parser.add_argument(
        "--user-pre-replace-file",
        default="",
        help='前置取代規則檔，格式為 "原文=替換"',
    )
    parser.add_argument(
        "--user-post-replace-file",
        default="",
        help='後置取代規則檔，格式為 "原文=替換"',
    )
    parser.add_argument(
        "--diff-enable",
        action="store_true",
        help="向 API 要求差異資料，方便之後除錯",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="顯示每個檔案的處理結果",
    )
    return parser.parse_args()


def normalize_extensions(values: Iterable[str]) -> tuple[str, ...]:
    normalized = []
    for value in values:
        ext = value if value.startswith(".") else f".{value}"
        normalized.append(ext.lower())
    return tuple(dict.fromkeys(normalized))


def read_optional_text(path: str) -> str:
    if not path:
        return ""
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def read_text_file(path: str) -> FileText:
    with open(path, "rb") as handle:
        raw = handle.read()
    for encoding in ENCODINGS:
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"無法辨識編碼：{path}")

    newline = "\r\n" if "\r\n" in text else "\n"
    had_final_newline = text.endswith(("\r\n", "\n", "\r"))
    return FileText(
        text=text,
        encoding=encoding,
        newline=newline,
        had_final_newline=had_final_newline,
    )


def restore_newlines(text: str, newline: str, had_final_newline: bool) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    restored = normalized.replace("\n", newline)
    if not had_final_newline:
        restored = restored.rstrip("\r\n")
    return restored


# def extract_ass_event_texts(text: str) -> tuple[list[str], list[AssEventText]]:
#     lines = text.splitlines(keepends=True)
#     events = []
#     in_events = False
#     format_fields: list[str] | None = None
#
#     for index, line in enumerate(lines):
#         stripped = line.strip()
#         is_header = stripped.startswith("[") and stripped.endswith("]")
#         if is_header:
#             in_events = stripped.lower() == "[events]"
#             format_fields = None
#             continue
#
#         if not in_events:
#             continue
#
#         if stripped.startswith("Format:"):
#             format_fields = [item.strip() for item in stripped[7:].split(",")]
#             continue
#
#         if not format_fields:
#             continue
#
#         if ":" not in line:
#             continue
#
#         kind, remainder = line.split(":", 1)
#         kind = kind.strip()
#         if kind not in ASS_EVENT_TYPES:
#             continue
#
#         parts = remainder.lstrip().rstrip("\r\n").split(",", len(format_fields) - 1)
#         if len(parts) != len(format_fields):
#             continue
#
#         try:
#             text_index = format_fields.index("Text")
#         except ValueError:
#             continue
#
#         events.append(
#             AssEventText(
#                 line_index=index,
#                 fields=parts,
#                 text_index=text_index,
#             )
#         )
#
#     texts = [event.fields[event.text_index] for event in events]
#     return texts, events
#
#
# def replace_ass_event_texts(original_text: str, converted_texts: list[str]) -> str:
#     lines = original_text.splitlines(keepends=True)
#     _, events = extract_ass_event_texts(original_text)
#     if not events:
#         return original_text
#
#     if len(converted_texts) != len(events):
#         raise RuntimeError("繁化姬回傳的事件數量和原始字幕不一致")
#
#     event_iter = iter(zip(events, converted_texts))
#     current = next(event_iter, None)
#     in_events = False
#     format_fields: list[str] | None = None
#
#     for index, line in enumerate(lines):
#         stripped = line.strip()
#         is_header = stripped.startswith("[") and stripped.endswith("]")
#         if is_header:
#             in_events = stripped.lower() == "[events]"
#             format_fields = None
#             continue
#
#         if not in_events:
#             continue
#
#         if stripped.startswith("Format:"):
#             format_fields = [item.strip() for item in stripped[7:].split(",")]
#             continue
#
#         if not current or current[0].line_index != index or not format_fields:
#             continue
#
#         event, converted = current
#         prefix, _ = line.split(":", 1)
#         fields = event.fields[:]
#         fields[event.text_index] = converted
#         line_ending = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
#         lines[index] = f"{prefix}: {','.join(fields)}{line_ending}"
#         current = next(event_iter, None)
#
#     return "".join(lines)
#
#
# def chunked(items: list[str], size: int) -> Iterable[list[str]]:
#     for index in range(0, len(items), size):
#         yield items[index:index + size]


def collect_targets(path: str, extensions: tuple[str, ...]) -> list[str]:
    if os.path.isfile(path):
        if os.path.splitext(path)[1].lower() not in extensions:
            return []
        return [os.path.abspath(path)]

    collected = []
    for root, _, files in os.walk(path):
        for name in files:
            if os.path.splitext(name)[1].lower() in extensions:
                collected.append(os.path.abspath(os.path.join(root, name)))
    return sorted(collected)


def backup_file(src: str, root_dir: str, backup_dir: str) -> None:
    if not backup_dir:
        return
    dest = os.path.join(backup_dir, os.path.relpath(src, root_dir))
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(src, dest)


def atomic_write(path: str, content: str, encoding: str) -> None:
    fd, temp_path = tempfile.mkstemp(prefix=".zhconvert-", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as handle:
            handle.write(content)
        os.replace(temp_path, path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def call_api(
    payload: dict[str, object],
    endpoint: str,
    timeout: int,
    retries: int,
    rate_limiter: RateLimiter,
) -> dict:
    body = urllib.parse.urlencode(payload).encode("utf-8")
    last_error = None
    for attempt in range(retries + 1):
        try:
            rate_limiter.wait()
            request = urllib.request.Request(
                endpoint,
                data=body,
                method="POST",
                headers={
                    "User-Agent": "Mozilla/5.0 zhconvert-batch/1.0",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 3))
    raise RuntimeError(f"API 呼叫失敗：{last_error}")


def build_payload(args: argparse.Namespace, text: str, extra: dict[str, str]) -> dict[str, object]:
    payload: dict[str, object] = {
        "text": text,
        "converter": args.converter,
        "ignoreTextStyles": args.ignore_text_styles,
        "jpTextStyles": args.jp_text_styles,
        "jpStyleConversionStrategy": args.jp_style_conversion_strategy,
        "jpTextConversionStrategy": args.jp_text_conversion_strategy,
        "userProtectReplace": extra["protect"],
        "userPreReplace": extra["pre"],
        "userPostReplace": extra["post"],
        "diffEnable": "true" if args.diff_enable else "false",
        # 先保守處理，避免 API 順手整理字幕格式。
        "cleanUpText": "false",
        "ensureNewlineAtEof": "false",
        "trimTrailingWhiteSpaces": "false",
        "translateTabsToSpaces": "-1",
        "outputFormat": "json",
    }
    return payload


def convert_text(
    source_text: str,
    args: argparse.Namespace,
    extra: dict[str, str],
    rate_limiter: RateLimiter,
) -> tuple[str, str]:
    payload = build_payload(args, source_text, extra)
    response = call_api(payload, args.api_endpoint, args.timeout, args.retries, rate_limiter)

    if response.get("code") != 0:
        raise RuntimeError(response.get("msg") or "繁化姬回傳未知錯誤")

    data = response.get("data") or {}
    converted = data.get("text")
    if not isinstance(converted, str):
        raise RuntimeError("繁化姬回傳格式不完整，缺少 data.text")

    modules = ",".join(data.get("usedModules") or [])
    return converted, modules


# def convert_ass_texts(
#     texts: list[str],
#     args: argparse.Namespace,
#     extra: dict[str, str],
#     rate_limiter: RateLimiter,
# ) -> tuple[list[str], str]:
#     if not texts:
#         return [], ""
#
#     converted_texts: list[str] = []
#     modules_seen: list[str] = []
#
#     for group in chunked(texts, ASS_BATCH_SIZE):
#         converted_blob, modules = convert_text(
#             ASS_TEXT_SENTINEL.join(group),
#             args,
#             extra,
#             rate_limiter,
#         )
#         split_group = converted_blob.split(ASS_TEXT_SENTINEL)
#
#         if len(split_group) != len(group):
#             split_group = []
#             for text in group:
#                 converted_line, line_modules = convert_text(text, args, extra, rate_limiter)
#                 split_group.append(converted_line)
#                 if line_modules:
#                     for module in line_modules.split(","):
#                         if module and module not in modules_seen:
#                             modules_seen.append(module)
#
#         else:
#             if modules:
#                 for module in modules.split(","):
#                     if module and module not in modules_seen:
#                         modules_seen.append(module)
#
#         converted_texts.extend(split_group)
#
#     return converted_texts, ",".join(modules_seen)


def process_file(
    file_path: str,
    root_dir: str,
    args: argparse.Namespace,
    extra: dict[str, str],
    rate_limiter: RateLimiter,
) -> tuple[str, bool, str]:
    original = read_text_file(file_path)
    extension = os.path.splitext(file_path)[1].lower()

    updated_text, modules = convert_text(original.text, args, extra, rate_limiter)

    restored = restore_newlines(updated_text, original.newline, original.had_final_newline)
    changed = restored != original.text

    if changed and not args.dry_run:
        if args.backup_dir:
            backup_file(file_path, root_dir, args.backup_dir)
        atomic_write(file_path, restored, original.encoding)

    return os.path.relpath(file_path, root_dir), changed, modules


def main() -> int:
    args = parse_args()
    target_path = os.path.abspath(args.path)
    extensions = normalize_extensions(args.extensions)

    if not os.path.exists(target_path):
        print(f"找不到目標：{target_path}", file=sys.stderr)
        return 1

    backup_dir = os.path.abspath(args.backup_dir) if args.backup_dir else ""
    targets = collect_targets(target_path, extensions)
    if not targets:
        print("找不到符合副檔名的字幕檔。")
        return 1

    root_dir = target_path if os.path.isdir(target_path) else os.path.dirname(target_path)
    extra = {
        "protect": read_optional_text(args.user_protect_replace_file),
        "pre": read_optional_text(args.user_pre_replace_file),
        "post": read_optional_text(args.user_post_replace_file),
    }

    print(f"目標：{target_path}")
    print(f"檔案數：{len(targets)}")
    print(f"副檔名：{' '.join(extensions)}")
    print(f"Converter：{args.converter}")
    print(f"Dry-run：{'yes' if args.dry_run else 'no'}")
    print(f"每秒請求上限：{args.max_requests_per_second}")
    if backup_dir:
        print(f"備份目錄：{backup_dir}")

    changed_count = 0
    failed_count = 0
    started_at = time.time()
    rate_limiter = RateLimiter(args.max_requests_per_second)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {
            executor.submit(process_file, file_path, root_dir, args, extra, rate_limiter): file_path
            for file_path in targets
        }
        for future in concurrent.futures.as_completed(future_map):
            file_path = future_map[future]
            rel_path = os.path.relpath(file_path, root_dir)
            try:
                rel_path, changed, modules = future.result()
                changed_count += int(changed)
                if args.verbose or changed:
                    status = "CHANGED" if changed else "UNCHANGED"
                    detail = f" [{modules}]" if modules else ""
                    print(f"{status:9} {rel_path}{detail}")
            except Exception as exc:
                failed_count += 1
                print(f"FAILED    {rel_path}: {exc}", file=sys.stderr)

    elapsed = time.time() - started_at
    print("")
    print(f"完成，耗時 {elapsed:.1f} 秒")
    print(f"已變更：{changed_count}")
    print(f"失敗：{failed_count}")
    print(f"未變更：{len(targets) - changed_count - failed_count}")
    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
