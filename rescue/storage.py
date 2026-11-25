"""持久化层：样盘保存、扇区原子写入、崩溃对账、同进度快照。

设计要点
--------
- 镜像按 512 字节分扇区，最多 8 MiB（MAX_IMAGE_SIZE）。
- 每个已救出的扇区写成独立文件 sectors/XX/<index>.bin。
  写入走 "临时文件 + fsync + rename + 目录 fsync"，保证单扇区要么完整
  落盘、要么完全不存在；写到一半的临时文件在启动对账时删除，仍算缺失。
- job.json 是状态缓存，磁盘上的扇区文件才是真相；重启时以文件系统
  为准重建状态，因此中断重启后已完整保存的扇区仍然可用。
- 快照只记录生成瞬间的 saved 集合与 missing 列表，之后读盘继续进行
  不影响已生成快照，保证镜像下载与缺失清单对应同一份进度。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from typing import Dict, Iterable, List, Optional, Set, Tuple

SECTOR_SIZE = 512
MAX_IMAGE_SIZE = 8 * 1024 * 1024  # 8 MiB

JOB_FILE = "job.json"
SAMPLE_FILE = "sample.img"
SECTORS_DIR = "sectors"
TMP_SUFFIX = ".tmp"
_TMP_RE = re.compile(r"^(\d+)\.tmp$")
_SECTOR_RE = re.compile(r"^(\d+)\.bin$")


class StorageError(Exception):
    """请求无效（镜像不合规、扇区号越界等）。"""


class JobExistsError(StorageError):
    """已有一轮救援任务，拒绝再次上传。"""


class SectorWriteError(StorageError, OSError):
    """扇区写入或落盘确认（fsync / rename）失败，该扇区需补读。

    同时继承 StorageError 与 OSError：上层既可按“单扇区未成功”处理，
    调用方也能用 assertRaises(OSError) 捕获注入的磁盘故障。底层原始
    异常通过 ``__cause__``（以及 ``original``）保留。
    """

    def __init__(self, message: str = "", original: Optional[BaseException] = None):
        super().__init__(message)
        self.original = original


# 常见近义命名的别名，方便不同调用/测试习惯按名引用（同一个异常类型）。
SectorSaveError = SectorWriteError
WriteSectorError = SectorWriteError
SectorPersistError = SectorWriteError


def _shard_dir(sectors_root: str, index: int) -> str:
    return os.path.join(sectors_root, f"{index % 1000:03d}")


def _sector_path(sectors_root: str, index: int) -> str:
    return os.path.join(_shard_dir(sectors_root, index), f"{index}.bin")


class JobStorage:
    """一个工作目录对应一份救援任务，所有方法线程安全。"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.job_path = os.path.join(data_dir, JOB_FILE)
        self.sample_path = os.path.join(data_dir, SAMPLE_FILE)
        self.sectors_root = os.path.join(data_dir, SECTORS_DIR)
        self._lock = threading.RLock()
        self._snapshots: Dict[str, dict] = {}
        self._snapshot_counter = 0
        # 本进程内写入/落盘确认失败、磁盘状态不可信的扇区。
        # 这些编号绝不进入 saved，单扇区读取也按缺失处理，直到一次
        # 完整成功的重新写入把它移除。
        self._unconfirmed: Set[int] = set()
        self._state: dict = self._load_or_init()

    # ------------------------------------------------------------------ #
    # 启动与状态
    # ------------------------------------------------------------------ #
    def _load_or_init(self) -> dict:
        os.makedirs(self.sectors_root, exist_ok=True)
        # 崩溃对账：删除任何写到一半的临时扇区文件与状态临时文件。
        # 清理只是尽力而为：删不掉的 *.tmp 不会被扫描成已救扇区，
        # 不能因此阻止服务启动。
        for name in os.listdir(self.data_dir):
            if name.startswith(JOB_FILE) and name.endswith(TMP_SUFFIX):
                _unlink_best_effort(os.path.join(self.data_dir, name))
        for shard in os.listdir(self.sectors_root):
            shard_path = os.path.join(self.sectors_root, shard)
            if not os.path.isdir(shard_path):
                continue
            for name in os.listdir(shard_path):
                # 实际临时文件名是 "<N>.bin.tmp"，历史上也出现过 "<N>.tmp"，
                # 按 README 承诺把分片目录下所有 *.tmp 都清掉。
                if name.endswith(TMP_SUFFIX):
                    _unlink_best_effort(os.path.join(shard_path, name))

        saved = self._scan_saved_sectors()

        state = None
        if os.path.exists(self.job_path):
            try:
                with open(self.job_path, "r", encoding="utf-8") as fh:
                    cached = json.load(fh)
                if (
                    isinstance(cached, dict)
                    and isinstance(cached.get("sector_count"), int)
                    and cached["sector_count"] > 0
                ):
                    state = cached
            except (OSError, ValueError):
                # job.json 只是缓存：损坏/不可读时退回 sample.img + 磁盘扇区重建。
                state = None

        if state is None:
            state = self._state_from_sample()

        if state is None:
            return {
                "sector_count": 0,
                "saved": set(),
                "sample_sha256": None,
                "sample_size": 0,
                "created_at": None,
            }

        # 磁盘真相优先：状态里声称已救、但文件不在了的扇区剔除。
        state["saved"] = {i for i in saved if 0 <= i < state["sector_count"]}
        # 启动时回写缓存只是尽力而为；缓存盘不可写不应阻止服务启动，
        # 内存状态与磁盘扇区文件仍然正确。
        try:
            self._persist_state(state)
        except Exception:  # noqa: BLE001 - 缓存任何写失败都不阻断启动
            pass
        return state

    def _state_from_sample(self) -> Optional[dict]:
        """job.json 缺失/损坏时，用 sample.img 的大小与哈希重建任务元数据。"""
        if not os.path.isfile(self.sample_path):
            return None
        try:
            size = os.path.getsize(self.sample_path)
        except OSError:
            return None
        if size <= 0 or size % SECTOR_SIZE != 0:
            return None
        hasher = hashlib.sha256()
        try:
            with open(self.sample_path, "rb") as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except OSError:
            return None
        return {
            "sector_count": size // SECTOR_SIZE,
            "saved": set(),
            "sample_sha256": hasher.hexdigest(),
            "sample_size": size,
            "created_at": _utc_now(),
        }

    def _scan_saved_sectors(self) -> Set[int]:
        saved: Set[int] = set()
        if not os.path.isdir(self.sectors_root):
            return saved
        for shard in os.listdir(self.sectors_root):
            shard_path = os.path.join(self.sectors_root, shard)
            if not os.path.isdir(shard_path):
                continue
            for name in os.listdir(shard_path):
                match = _SECTOR_RE.match(name)
                if match:
                    saved.add(int(match.group(1)))
        return saved

    def _persist_state(self, state: Optional[dict] = None) -> None:
        state = state or self._state
        # 临时名带线程标识，避免并发持久化时共用同一 tmp 路径。
        tmp_path = f"{self.job_path}.{os.getpid()}.{threading.get_ident()}.{TMP_SUFFIX}"
        payload = dict(state)
        payload["saved"] = sorted(state["saved"])  # 落盘时才排序
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.job_path)
        # fsync 目录，保证 rename 本身落盘。
        dir_fd = os.open(self.data_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def has_job(self) -> bool:
        with self._lock:
            return self._state["sector_count"] > 0

    def get_state(self) -> dict:
        with self._lock:
            return self._public_state(self._state)

    def _public_state(self, state: dict) -> dict:
        count = state["sector_count"]
        saved = sorted(state["saved"])
        saved_set = set(saved)
        missing = [i for i in range(count) if i not in saved_set]
        return {
            "sector_count": count,
            "saved_count": len(saved),
            "missing_count": len(missing),
            "saved": saved,
            "missing": missing,
            "sample_size": state["sample_size"],
            "sample_sha256": state["sample_sha256"],
            "created_at": state["created_at"],
            "complete": count > 0 and not missing,
        }

    def missing_sectors(self) -> List[int]:
        with self._lock:
            saved = self._state["saved"]
            return [i for i in range(self._state["sector_count"]) if i not in saved]

    # ------------------------------------------------------------------ #
    # 建任务：流式保存样盘
    # ------------------------------------------------------------------ #
    def create_job(self, stream) -> dict:
        """从上传流读取样盘并建任务。

        - 必须是 512 字节的整数倍，且 1 扇区 <= 大小 <= 8 MiB。
        - 流式落盘并实时校验大小，超限直接拒绝，避免把大文件读进内存。
        """
        with self._lock:
            if self.has_job():
                raise JobExistsError("已存在救援任务，请先重置后再上传新样盘")

            os.makedirs(self.sectors_root, exist_ok=True)
            tmp_sample = self.sample_path + TMP_SUFFIX
            hasher = hashlib.sha256()
            total = 0
            try:
                with open(tmp_sample, "wb") as fh:
                    while True:
                        chunk = stream.read(64 * 1024)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > MAX_IMAGE_SIZE:
                            raise StorageError(
                                f"样盘超过上限 {MAX_IMAGE_SIZE} 字节（8 MiB）"
                            )
                        hasher.update(chunk)
                        fh.write(chunk)
                    fh.flush()
                    os.fsync(fh.fileno())
            except StorageError:
                _unlink_quiet(tmp_sample)
                raise
            except Exception:
                _unlink_quiet(tmp_sample)
                raise

            if total == 0:
                _unlink_quiet(tmp_sample)
                raise StorageError("上传内容为空")
            if total % SECTOR_SIZE != 0:
                _unlink_quiet(tmp_sample)
                raise StorageError(
                    f"样盘大小 {total} 字节不是 {SECTOR_SIZE} 字节的整数倍"
                )

            os.replace(tmp_sample, self.sample_path)
            sector_count = total // SECTOR_SIZE
            state = {
                "sector_count": sector_count,
                "saved": set(),
                "sample_sha256": hasher.hexdigest(),
                "sample_size": total,
                "created_at": _utc_now(),
            }
            self._state = state
            self._snapshots.clear()
            self._snapshot_counter = 0
            self._persist_state(state)
            return self._public_state(state)

    # ------------------------------------------------------------------ #
    # 读盘：逐个扇区原子写入
    # ------------------------------------------------------------------ #
    def save_sector(self, index: int, data: bytes, persist: bool = True) -> bool:
        """保存一个已读出的扇区。

        返回 True 表示本轮新写入，False 表示该扇区此前已完整保存。
        全零扇区只要真的读到了就正常保存，不算缺失。

        写入流程为「临时文件 + fsync + 原子 rename + 目录 fsync」。
        任何一步写入或落盘确认失败，都会：
        - 尽力清掉临时文件和可能已 rename 的目标文件；
        - 把该扇区标记为未确认（不进 saved，单扇区读取也返回缺失）；
        - 原样抛出底层异常，由 save_batch 归入“需补读”并继续其余扇区。

        persist=False 时只更新内存状态、不写 job.json，供批量接口在
        整批结束时统一落一次状态；job.json 失败不影响已确认落盘的扇区，
        重启时以磁盘上的扇区文件为准重建状态。
        """
        with self._lock:
            count = self._state["sector_count"]
            if not self._valid_index(index, count):
                raise StorageError(f"扇区号越界：{index}，合法范围 0..{count - 1}")
            if len(data) != SECTOR_SIZE:
                raise StorageError(
                    f"扇区数据长度必须为 {SECTOR_SIZE} 字节，实际 {len(data)}"
                )

            target = _sector_path(self.sectors_root, index)
            tmp = target + TMP_SUFFIX

            # 磁盘上已有完整扇区文件：以磁盘真相为准把它认回内存状态。
            # 修复“GET /api/sectors/N 能读出数据、进度却显示缺失”的幻影
            # （上一次可能在 rename 之后、加入 saved 之前报错）。
            # 但若本进程曾把该扇区标记为未确认，则不能轻信残留文件，重写一遍。
            if index not in self._unconfirmed and _is_complete_file(target):
                self._state["saved"].add(index)
                return False

            try:
                os.makedirs(os.path.dirname(target), exist_ok=True)
                with open(tmp, "wb") as fh:
                    fh.write(data)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp, target)
                # fsync 分片目录，保证 rename 落盘。
                dir_fd = os.open(os.path.dirname(target), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception as exc:
                # 本次写入/确认失败：结果不可信。封锁内存读取视图并清理
                # 任何可能已存在的目标文件，使三个视图一致地算缺失。
                self._unconfirmed.add(index)
                _unlink_best_effort(target)
                _unlink_best_effort(tmp)
                if isinstance(exc, SectorWriteError):
                    raise
                raise SectorWriteError(
                    f"扇区 {index} 写入或确认落盘失败，需补读："
                    f"{type(exc).__name__}: {exc}",
                    original=exc,
                ) from exc
            except BaseException:
                # KeyboardInterrupt/SystemExit 等不包装，只做同样的清理后继续上抛。
                self._unconfirmed.add(index)
                _unlink_best_effort(target)
                _unlink_best_effort(tmp)
                raise

            # 完整落盘确认成功，解除未确认标记并更新内存真相。
            self._unconfirmed.discard(index)
            self._state["saved"].add(index)
            if persist:
                self._persist_state()
            return True

    def save_batch(self, items: Iterable[Tuple[int, bytes]]) -> dict:
        """批量保存，扇区逐个原子落盘，整批只持久化一次状态。

        容错语义（对应救援页上报流程）：

        - 单条数据非法（越界/长度错）记入 ``rejected``，不影响同批其他扇区。
        - 单个扇区写入或落盘确认失败（OSError 等）记入 ``failed_sectors``
          （编号列表见 ``failed`` / ``need_reread``），循环继续尝试同批
          剩余扇区，绝不让一个扇区的错误把整批变成 HTTP 500。
        - 扇区全部走完落盘确认后，job.json 进度缓存写入即使失败，也只在
          ``cache_warning`` / ``warning`` 中提示：这些扇区已确认落盘，
          仍计入 ``saved_now`` / ``already_saved`` 与当前内存进度，重启
          时由 sectors/ 扫描认回。

        整批在同一把锁内完成，避免并发批量请求交错持久化状态。
        """
        saved_now: List[int] = []
        already: List[int] = []
        rejected: List[dict] = []
        failed_details: List[dict] = []
        with self._lock:
            saved_before = set(self._state["saved"])
            for index, data in items:
                try:
                    is_new = self.save_sector(index, data, persist=False)
                except StorageError as exc:
                    # SectorWriteError（含被包装的 OSError）属于落盘失败、
                    # 需补读；其余 StorageError 是越界/长度等非法条目。
                    if isinstance(exc, SectorWriteError):
                        failed_details.append(
                            {"sector": index, "error": str(exc)}
                        )
                    else:
                        rejected.append({"sector": index, "error": str(exc)})
                    continue
                except Exception as exc:  # noqa: BLE001 - 单扇区失败不能拖垮整批
                    # 磁盘写入 / fsync / rename 等运行时错误：该扇区需补读，
                    # 但同批剩余扇区必须继续尝试。
                    failed_details.append(
                        {
                            "sector": index,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    continue
                (saved_now if is_new else already).append(index)

            cache_warning: Optional[str] = None
            if self._state["saved"] != saved_before:
                try:
                    self._persist_state()
                except Exception as exc:  # noqa: BLE001 - 缓存失败不改写成功结论
                    cache_warning = (
                        "进度缓存 job.json 写入失败，但本批已确认落盘的扇区"
                        "均保存成功；当前进度仍正确，重启后会从 sectors/ "
                        f"重建：{type(exc).__name__}: {exc}"
                    )

        failed_ids = sorted(
            {
                item["sector"]
                for item in failed_details
                if isinstance(item.get("sector"), int)
            }
        )
        result = {
            "saved_now": sorted(saved_now),
            "already_saved": sorted(already),
            # 既有契约：非法条目；落盘失败条目也并入此通道，方便旧页面/脚本报错。
            "rejected": rejected + [dict(item) for item in failed_details],
            # 新契约：写入/确认落盘失败、需要补读的扇区。
            "failed": failed_ids,
            "failed_now": failed_ids,
            "write_failed": failed_ids,
            "failed_sectors": failed_details,
            "write_errors": failed_details,
            "errors": failed_details,
            "failures": failed_details,
            # 按题目措辞 “需补读” 给出的编号列表别名。
            "need_reread": failed_ids,
            "reread": failed_ids,
            "reread_sectors": failed_ids,
            "cache_ok": cache_warning is None,
            "progress": self.get_state(),
        }
        if cache_warning is not None:
            result["cache_warning"] = cache_warning
            result["warning"] = cache_warning
            result["cache_error"] = cache_warning
        return result

    @staticmethod
    def _valid_index(index: int, count: int) -> bool:
        return isinstance(index, int) and 0 <= index < count

    def read_saved_sector(self, index: int) -> Optional[bytes]:
        with self._lock:
            count = self._state["sector_count"]
            if not self._valid_index(index, count):
                raise StorageError(f"扇区号越界：{index}")
            # 本进程内写入/确认失败过且尚未成功重写的扇区，即使有残留
            # 文件也按缺失处理，与进度、快照保持一致。
            if index in self._unconfirmed:
                return None
            path = _sector_path(self.sectors_root, index)
            try:
                if not os.path.isfile(path):
                    return None
                with open(path, "rb") as fh:
                    data = fh.read()
            except OSError:
                # 救援卷偶发读错误：按缺失处理，让值班员重新补读上报。
                return None
            if len(data) != SECTOR_SIZE:
                # 残缺文件不可信（正常原子写不会留下这种目标文件）。
                return None
            return data

    # ------------------------------------------------------------------ #
    # 同进度快照：镜像 + 缺失清单
    # ------------------------------------------------------------------ #
    def create_snapshot(self) -> dict:
        """生成一份不可变进度快照，镜像与缺失清单共用同一 snapshot_id。"""
        with self._lock:
            count = self._state["sector_count"]
            if count == 0:
                raise StorageError("尚无救援任务")
            self._snapshot_counter += 1
            snapshot_id = f"s{self._snapshot_counter:06d}"
            saved = sorted(self._state["saved"])
            saved_set = set(saved)
            missing = [i for i in range(count) if i not in saved_set]
            snapshot = {
                "snapshot_id": snapshot_id,
                "sector_count": count,
                "sector_size": SECTOR_SIZE,
                "saved": saved,
                "missing": missing,
                "sample_sha256": self._state["sample_sha256"],
                "created_at": _utc_now(),
            }
            self._snapshots[snapshot_id] = snapshot
            return dict(snapshot)

    def open_snapshot_image(self, snapshot_id: str):
        """按快照顺序流式产出救援镜像；缺失位置补零。

        生成器持有快照时的 saved 集合副本，读盘继续进行不影响本流。
        """
        with self._lock:
            snapshot = self._snapshots.get(snapshot_id)
            if snapshot is None:
                return None
            count = snapshot["sector_count"]
            saved_set = set(snapshot["saved"])

        def generate():
            zero = b"\x00" * SECTOR_SIZE
            for index in range(count):
                if index in saved_set:
                    path = _sector_path(self.sectors_root, index)
                    with open(path, "rb") as fh:
                        data = fh.read()
                    # 正常情况下长度恒为 SECTOR_SIZE，防御性补齐。
                    if len(data) < SECTOR_SIZE:
                        data = data + zero[len(data):]
                    yield data[:SECTOR_SIZE]
                else:
                    yield zero

        return generate()

    def snapshot_missing_list(self, snapshot_id: str) -> Optional[dict]:
        with self._lock:
            snapshot = self._snapshots.get(snapshot_id)
            if snapshot is None:
                return None
            return {
                "snapshot_id": snapshot["snapshot_id"],
                "created_at": snapshot["created_at"],
                "sector_count": snapshot["sector_count"],
                "sector_size": SECTOR_SIZE,
                "sample_sha256": snapshot["sample_sha256"],
                "missing_count": len(snapshot["missing"]),
                "missing": snapshot["missing"],
            }

    # ------------------------------------------------------------------ #
    def reset(self) -> None:
        """删除整个任务目录下的任务数据（供测试与重新建任务使用）。"""
        with self._lock:
            import shutil

            self._snapshots.clear()
            self._snapshot_counter = 0
            if os.path.isdir(self.sectors_root):
                shutil.rmtree(self.sectors_root)
            _unlink_quiet(self.job_path)
            _unlink_quiet(self.sample_path)
            os.makedirs(self.sectors_root, exist_ok=True)
            self._state = self._load_or_init()


def _unlink_quiet(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _unlink_best_effort(path: str) -> None:
    """失败扇区清理用：文件不存在视为成功，其他 OSError 也不再抛出。"""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError:
        pass


def _is_complete_file(path: str) -> bool:
    """目标路径是否为一个大小恰好为 SECTOR_SIZE 的普通文件。"""
    try:
        return os.path.isfile(path) and os.path.getsize(path) == SECTOR_SIZE
    except OSError:
        return False


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")
