"""故障注入自测：扇区落盘报错、进度缓存报错、重启核对。

这些用例对应任务要求的关键语义：

1. 某个扇区写入/确认落盘失败后，同批剩余扇区继续尝试，HTTP 不返回 500；
2. 失败扇区在单扇区读取(409)、进度(missing)、新快照(missing/补零)中都算缺失；
3. 重启（重新构造 JobStorage / 重启 HTTP 服务）后仍算缺失、仍可补读成功；
4. 已确认落盘的扇区，即使 job.json 进度缓存写入失败，响应仍报保存成功，
   内存进度正确，重启后由 sectors/ 扫描认回；
5. 全零扇区正常落盘并算已救（既有测试也覆盖，这里在故障批次旁再断言一次）。

直接运行：python3 -I tests/test_fault_recovery.py
"""

from __future__ import annotations

import base64
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

# 支持直接运行：python3 -I selftest/test_faults.py
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_ROOT, os.path.join(_ROOT, ".pylibs")):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from rescue.app import create_app
from rescue.storage import SECTOR_SIZE, JobStorage


def make_image(sectors: int) -> bytes:
    buf = bytearray((i * 13 + 5) % 256 for i in range(sectors * SECTOR_SIZE))
    # 放一个真实全零扇区（4），确认它不会被当成缺失补零位。
    buf[4 * SECTOR_SIZE : 5 * SECTOR_SIZE] = b"\x00" * SECTOR_SIZE
    return bytes(buf)


class FaultApiTestBase(unittest.TestCase):
    sectors = 8

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="rescue-fault-")
        self.app = create_app(self.tmp)
        self.app.testing = True
        self.client = self.app.test_client()
        self.storage: JobStorage = self.app.extensions["rescue_storage"]
        self.image = make_image(self.sectors)
        resp = self.client.post(
            "/api/job",
            data={"image": (io.BytesIO(self.image), "sample.img")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resp.status_code, 201)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def entry(self, index: int) -> dict:
        return {
            "sector": index,
            "data": base64.b64encode(
                self.image[index * SECTOR_SIZE : (index + 1) * SECTOR_SIZE]
            ).decode(),
        }

    def report(self, indexes):
        return self.client.post(
            "/api/sectors",
            data=json.dumps({"sectors": [self.entry(i) for i in indexes]}),
            content_type="application/json",
        )


class SectorLandingFaultApiTests(FaultApiTestBase):
    def test_batch_continues_and_failed_sector_is_missing_everywhere(self):
        original = self.storage.save_sector

        def flaky(index, data, persist=True):
            if index == 2:
                raise OSError("EIO injected: sector volume write error")
            return original(index, data, persist=persist)

        with mock.patch.object(self.storage, "save_sector", side_effect=flaky):
            resp = self.report([0, 1, 2, 3, 4])

        # 不再整批 HTTP 500
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertTrue(body["ok"])
        # 同批剩余扇区全部继续并保存成功
        self.assertEqual(body["saved_now"], [0, 1, 3, 4])
        # 失败扇区通过各种可能的字段名都能被识别为“需补读”
        self.assertEqual(body["failed"], [2])
        self.assertEqual(body["need_reread"], [2])
        self.assertEqual([d["sector"] for d in body["failed_sectors"]], [2])
        rejected_ids = [r["sector"] for r in body["rejected"]]
        self.assertIn(2, rejected_ids)

        # 单扇区读取：失败扇区 409，其余 200 且数据正确
        bad = self.client.get("/api/sectors/2")
        self.assertEqual(bad.status_code, 409)
        for i in (0, 1, 3, 4):
            ok = self.client.get(f"/api/sectors/{i}")
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(
                ok.data, self.image[i * SECTOR_SIZE : (i + 1) * SECTOR_SIZE]
            )

        # 进度：2 仍缺失，已保存的不在 missing
        progress = self.client.get("/api/job").get_json()["progress"]
        self.assertIn(2, progress["missing"])
        for i in (0, 1, 3, 4):
            self.assertNotIn(i, progress["missing"])

        # 新快照：缺失清单含 2，镜像中 2 的位置补零
        sid = (
            self.client.post("/api/snapshots")
            .get_json()["snapshot"]["snapshot_id"]
        )
        missing = self.client.get(
            f"/api/snapshots/{sid}/missing.json"
        ).get_json()["missing"]
        self.assertIn(2, missing)
        img = self.client.get(f"/api/snapshots/{sid}/image.bin").data
        self.assertEqual(img[2 * 512 : 3 * 512], b"\x00" * 512)
        self.assertEqual(img[3 * 512 : 4 * 512], self.image[3 * 512 : 4 * 512])

        # 故障消失后补读 2：成功，页面接口都不再缺失
        again = self.report([2])
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.get_json()["saved_now"], [2])
        self.assertEqual(self.client.get("/api/sectors/2").status_code, 200)
        self.assertNotIn(
            2, self.client.get("/api/missing").get_json()["missing"]
        )

    def test_post_rename_confirm_failure_leaves_no_readable_file(self):
        """模拟 rename 已发生、目录 fsync 确认失败：目标文件必须被清理。"""
        real_replace = os.replace

        def flaky_replace(src, dst):
            real_replace(src, dst)  # 真实完成 rename，制造“文件已在盘上”
            if str(dst).endswith(os.path.join("003", "3.bin")):
                raise OSError("EIO injected on directory fsync confirmation")

        with mock.patch("rescue.storage.os.replace", side_effect=flaky_replace):
            # 让 os.replace 在“真实替换后”抛错；save_sector 应清理目标。
            # 注意 side_effect 函数自己已经调用了 real_replace，mock 不会再调一次。
            resp = self.report([3, 5])

        body = resp.get_json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(body["saved_now"], [5])
        self.assertEqual(body["failed"], [3])
        target = os.path.join(self.tmp, "sectors", "003", "3.bin")
        self.assertFalse(os.path.exists(target), "失败扇区的目标文件应被清理")
        self.assertEqual(self.client.get("/api/sectors/3").status_code, 409)
        self.assertIn(3, self.client.get("/api/missing").get_json()["missing"])

    def test_all_zero_sector_saved_in_fault_batch(self):
        original = self.storage.save_sector

        def flaky(index, data, persist=True):
            if index == 1:
                raise OSError("injected")
            return original(index, data, persist=persist)

        with mock.patch.object(self.storage, "save_sector", side_effect=flaky):
            body = self.report([1, 4]).get_json()
        self.assertEqual(body["saved_now"], [4])
        self.assertNotIn(
            4, self.client.get("/api/missing").get_json()["missing"]
        )
        got = self.client.get("/api/sectors/4")
        self.assertEqual(got.status_code, 200)
        self.assertEqual(got.data, b"\x00" * SECTOR_SIZE)


class RestartAfterSectorFaultTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="rescue-fault-restart-")
        self.storage = JobStorage(self.tmp)
        self.image = make_image(6)
        self.storage.create_job(io.BytesIO(self.image))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_failed_sector_missing_after_restart_then_reread_succeeds(self):
        real_replace = os.replace

        def flaky_replace(src, dst):
            real_replace(src, dst)
            if str(dst).endswith(os.path.join("002", "2.bin")):
                raise OSError("EIO injected after rename")

        items = [
            (i, self.image[i * 512 : (i + 1) * 512]) for i in range(5)
        ]
        with mock.patch("rescue.storage.os.replace", side_effect=flaky_replace):
            result = self.storage.save_batch(items)

        self.assertEqual(result["saved_now"], [0, 1, 3, 4])
        self.assertEqual(result["failed"], [2])
        self.assertIsNone(self.storage.read_saved_sector(2))

        # “重启”：同一数据目录重新构造存储（mock 已退出）
        reopened = JobStorage(self.tmp)
        state = reopened.get_state()
        self.assertEqual(state["saved"], [0, 1, 3, 4])
        self.assertIn(2, state["missing"])
        self.assertIsNone(reopened.read_saved_sector(2))

        # 重启后仍可补读并再次重启核对
        self.assertTrue(
            reopened.save_sector(2, self.image[2 * 512 : 3 * 512])
        )
        reopened2 = JobStorage(self.tmp)
        self.assertEqual(reopened2.get_state()["missing"], [5])
        self.assertEqual(
            reopened2.read_saved_sector(2), self.image[2 * 512 : 3 * 512]
        )


class CacheFailureTests(FaultApiTestBase):
    def test_job_cache_persist_failure_reports_saved_and_recovers_on_restart(self):
        with mock.patch.object(
            JobStorage, "_persist_state", side_effect=OSError("ENOSPC job.json")
        ):
            resp = self.report([0, 1])

        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        body = resp.get_json()
        # 已确认落盘 -> 必须报保存成功，不能因为缓存写失败而 500
        self.assertEqual(body["saved_now"], [0, 1])
        self.assertTrue(body["cache_ok"] is False)
        self.assertTrue(body.get("cache_warning") or body.get("warning"))

        # 同一进程内内存进度正确，单扇区可读
        progress = self.client.get("/api/job").get_json()["progress"]
        self.assertEqual(progress["saved_count"], 2)
        self.assertEqual(progress["saved"], [0, 1])
        self.assertEqual(self.client.get("/api/sectors/0").status_code, 200)
        self.assertEqual(self.client.get("/api/sectors/1").status_code, 200)

        # 磁盘上确实有扇区文件
        for i in (0, 1):
            self.assertTrue(
                os.path.isfile(
                    os.path.join(self.tmp, "sectors", f"{i % 1000:03d}", f"{i}.bin")
                )
            )

        # 重启：即使故障 patch 仍在，构造函数也不应被缓存回写失败击倒；
        # 磁盘扫描应认回 0,1（job.json 还是建任务时的旧版本）。
        with mock.patch.object(
            JobStorage, "_persist_state", side_effect=OSError("ENOSPC job.json")
        ):
            reopened = JobStorage(self.tmp)
        state = reopened.get_state()
        self.assertEqual(state["saved"], [0, 1])
        self.assertIn(2, state["missing"])

        # 故障消失后再上报，缓存恢复正常
        resp2 = self.report([2])
        self.assertEqual(resp2.status_code, 200)
        self.assertEqual(resp2.get_json()["saved_now"], [2])
        self.assertTrue(resp2.get_json()["cache_ok"])


class PageContractTests(FaultApiTestBase):
    def test_page_contains_saved_and_reread_regions(self):
        html = self.client.get("/").get_data(as_text=True)
        self.assertIn("需补读", html)
        self.assertIn("report-detail", html)
        self.assertIn("已保存", html)


if __name__ == "__main__":
    unittest.main(verbosity=2)
