#!/usr/bin/env python3
"""真实 HTTP 进程 + 真实文件系统权限故障 + 两次重启的端到端自测。

分别制造并核对：

A) 扇区落盘报错：把某个分片目录改成只读 -> open(tmp,'wb') EACCES。
   期望：HTTP 200（不是 500），同批其余扇区保存成功，失败扇区在
   响应 failed/need_reread/rejected、GET /api/sectors/N(409)、
   /api/missing、新快照缺失清单与补零镜像中都算缺失；故障恢复后可补读。

B) 进度缓存报错：把数据根目录改成只读 -> job.json 临时文件创建 EACCES，
   但 sectors/ 子目录仍可写。期望：已确认落盘的扇区仍在 saved_now、
   HTTP 200、带 cache_warning，同进程进度正确。

随后两次重启服务，核对磁盘重建出的进度、失败扇区重启后仍缺失且可补读、
补读结果二次重启仍在，以及全零扇区算已救。

默认用 gunicorn（若 .pylibs 或环境里有）以镜像同款单进程多线程方式启动；
加 --flask 则改用 wsgi.py 开发服务器。

运行：python3 -I selftest/live_fault_selftest.py [--flask]
"""
from __future__ import annotations

import base64
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (ROOT, os.path.join(ROOT, ".pylibs")):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

PORT = int(os.environ.get("SELFTEST_PORT", "8811"))
BASE = f"http://127.0.0.1:{PORT}"
SECTORS = 20
USE_FLASK = "--flask" in sys.argv
FAILURES: list[str] = []


def check(cond, msg):
    if cond:
        print("  ✓", msg, flush=True)
    else:
        FAILURES.append(msg)
        print("  ✗ 失败：", msg, flush=True)


def make_image(n):
    buf = bytearray((i * 31 + 7) % 256 for i in range(n * 512))
    buf[4 * 512:5 * 512] = b"\x00" * 512  # 真实全零扇区
    return bytes(buf)


class Client:
    def upload(self, image):
        boundary = "----selftest-" + uuid.uuid4().hex
        body = io.BytesIO()
        body.write(f"--{boundary}\r\n".encode())
        body.write(b'Content-Disposition: form-data; name="image"; filename="s.img"\r\n')
        body.write(b"Content-Type: application/octet-stream\r\n\r\n")
        body.write(image)
        body.write(f"\r\n--{boundary}--\r\n".encode())
        req = urllib.request.Request(
            BASE + "/api/job", data=body.getvalue(), method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read())

    def post_json(self, path, payload):
        req = urllib.request.Request(
            BASE + path, data=json.dumps(payload).encode(), method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def get(self, path):
        try:
            with urllib.request.urlopen(BASE + path, timeout=15) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def get_json(self, path):
        st, raw = self.get(path)
        return st, json.loads(raw)


def wait_port(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(0.4)
            if s.connect_ex(("127.0.0.1", PORT)) == 0:
                return
        time.sleep(0.25)
    raise RuntimeError("服务端口未就绪")


def _gunicorn_available() -> bool:
    if USE_FLASK:
        return False
    try:
        import gunicorn  # noqa: F401
        return True
    except ImportError:
        return os.path.exists(os.path.join(ROOT, ".pylibs", "gunicorn"))


def start_server(data_dir):
    env = dict(os.environ, RESCUE_DATA_DIR=data_dir)
    log = open(os.path.join(data_dir, "server.log"), "ab")
    if _gunicorn_available():
        cmd = [
            sys.executable, "-I",
            os.path.join(ROOT, "selftest", "gunicorn_boot.py"),
            "--bind", f"127.0.0.1:{PORT}", "--workers", "1",
            "--threads", "8", "--timeout", "60", "wsgi:app",
        ]
        kind = "gunicorn"
    else:
        env["PORT"] = str(PORT)
        cmd = [sys.executable, "-I", os.path.join(ROOT, "wsgi.py")]
        kind = "flask"
    proc = subprocess.Popen(
        cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    wait_port()
    print(f"  （服务方式：{kind}）")
    return proc, log


def stop_server(proc, log):
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()


def entry(image, i):
    return {"sector": i,
            "data": base64.b64encode(image[i * 512:(i + 1) * 512]).decode()}


def report(client, image, idxs):
    return client.post_json("/api/sectors",
                            {"sectors": [entry(image, i) for i in idxs]})


def shard(data_dir, i):
    return os.path.join(data_dir, "sectors", f"{i % 1000:03d}")


def main():
    data_dir = tempfile.mkdtemp(prefix="rescue-live-")
    os.chmod(data_dir, 0o755)
    print("数据目录：", data_dir)
    image = make_image(SECTORS)
    client = Client()

    proc, log = start_server(data_dir)
    try:
        st, body = client.upload(image)
        check(st == 201 and body["progress"]["sector_count"] == SECTORS,
              f"上传 {SECTORS} 扇区建任务（HTTP {st}）")

        html_st, html = client.get("/")
        text = html.decode("utf-8", "replace")
        check(html_st == 200 and "需补读" in text and "report-detail" in text,
              "救援页包含已保存/需补读编号区域")

        st, body = report(client, image, [0, 1, 2])
        check(st == 200 and body["saved_now"] == [0, 1, 2],
              f"健康批次保存 0,1,2（HTTP {st} {body.get('saved_now')}）")

        # ---- A) 扇区落盘报错：7 号分片只读 ----
        os.makedirs(shard(data_dir, 7), exist_ok=True)
        os.chmod(shard(data_dir, 7), 0o555)
        st, body = report(client, image, [6, 7, 8, 9])
        check(st == 200, f"扇区落盘报错不再导致整批 HTTP 500（HTTP {st}）")
        check(body.get("saved_now") == [6, 8, 9],
              f"同批剩余扇区 6,8,9 继续保存成功：{body.get('saved_now')}")
        check(body.get("failed") == [7] and body.get("need_reread") == [7],
              f"响应列出 7 为需补读：failed={body.get('failed')}")
        check(any(r.get("sector") == 7 for r in body.get("rejected", [])),
              "rejected 通道也包含失败扇区 7")
        s7, _ = client.get("/api/sectors/7")
        check(s7 == 409, f"失败扇区 7 单扇区读取按缺失返回 409（实际 {s7}）")
        _, miss = client.get_json("/api/missing")
        check(7 in miss["missing"], "进度缺失清单包含 7")
        sid = client.post_json("/api/snapshots", {})[1]["snapshot"]["snapshot_id"]
        _, sm = client.get_json(f"/api/snapshots/{sid}/missing.json")
        check(7 in sm["missing"], "新快照缺失清单包含 7")
        _, img = client.get(f"/api/snapshots/{sid}/image.bin")
        check(img[7 * 512:8 * 512] == b"\x00" * 512, "新快照镜像中 7 号位置补零")

        os.chmod(shard(data_dir, 7), 0o755)
        st, body = report(client, image, [7])
        check(st == 200 and body["saved_now"] == [7],
              f"故障恢复后补读 7 成功（{body.get('saved_now')}）")

        # 13 号失败后重启前不补，验证重启后仍缺失、仍可补
        os.makedirs(shard(data_dir, 13), exist_ok=True)
        os.chmod(shard(data_dir, 13), 0o555)
        st, body = report(client, image, [12, 13, 14])
        check(st == 200 and body.get("saved_now") == [12, 14]
              and body.get("failed") == [13],
              f"第二批 12,14 保存、13 需补读：saved={body.get('saved_now')} failed={body.get('failed')}")

        # ---- B) 进度缓存报错：数据根目录只读（sectors/ 子目录仍可写）----
        os.chmod(data_dir, 0o555)
        st, body = report(client, image, [10, 11])
        check(st == 200, f"缓存写失败时上报仍为 HTTP 200（实际 {st}）")
        check(body.get("saved_now") == [10, 11],
              f"已确认落盘的 10,11 报保存成功：{body.get('saved_now')}")
        check(body.get("cache_ok") is False
              and (body.get("warning") or body.get("cache_warning")),
              "响应给出进度缓存失败警告 cache_ok=false")
        _, job = client.get_json("/api/job")
        check({10, 11}.issubset(set(job["progress"]["saved"])),
              "同进程进度仍显示 10,11 已保存（内存状态正确）")
        s10, raw10 = client.get("/api/sectors/10")
        check(s10 == 200 and raw10 == image[10 * 512:11 * 512],
              "缓存失败不影响已落盘扇区的读取")
        os.chmod(data_dir, 0o755)
    finally:
        stop_server(proc, log)

    # ================= 第一次重启：核对进度 =================
    print("\n-- 第一次重启服务，核对磁盘重建的进度 --", flush=True)
    proc, log = start_server(data_dir)
    try:
        _, job = client.get_json("/api/job")
        prog = job["progress"]
        saved = set(prog["saved"])
        expected = {0, 1, 2, 6, 7, 8, 9, 10, 11, 12, 14}
        check(saved == expected,
              f"重启后 saved 恰为 {sorted(expected)}（实际 {sorted(saved)}）")
        check({10, 11}.issubset(saved),
              "缓存写失败的 10,11 重启后由 sectors/ 认回")
        check(13 in prog["missing"], "未落盘成功的 13 重启后仍算缺失")
        s13, _ = client.get("/api/sectors/13")
        check(s13 == 409, "重启后单扇区读取 13 仍为 409")

        # 偶发故障已消失（恢复分片可写），再补读 13
        os.chmod(shard(data_dir, 13), 0o755)
        st, body = report(client, image, [13])
        check(st == 200 and body["saved_now"] == [13],
              f"故障恢复并重启后可补读 13 并保存成功"
              f"（{body.get('saved_now')} / failed={body.get('failed')}）")
    finally:
        stop_server(proc, log)

    # ================= 第二次重启：确认补读持久 =================
    print("\n-- 第二次重启服务，确认补读结果持久 --", flush=True)
    proc, log = start_server(data_dir)
    try:
        _, job = client.get_json("/api/job")
        prog = job["progress"]
        check(prog["saved_count"] == 12 and 13 in prog["saved"],
              f"二次重启后 13 仍在、共 12 个已救（实际 saved_count={prog['saved_count']}）")
        s13, raw13 = client.get("/api/sectors/13")
        check(s13 == 200 and raw13 == image[13 * 512:14 * 512],
              "二次重启后 13 数据逐字节正确")

        st, zbody = report(client, image, [4])
        check(st == 200 and zbody["saved_now"] == [4], "全零扇区 4 可正常上报保存")
        s4, raw4 = client.get("/api/sectors/4")
        check(s4 == 200 and raw4 == b"\x00" * 512, "全零扇区 4 单扇区读取为 512 个 0")

        sid = client.post_json("/api/snapshots", {})[1]["snapshot"]["snapshot_id"]
        _, sm = client.get_json(f"/api/snapshots/{sid}/missing.json")
        check(13 not in sm["missing"] and 4 not in sm["missing"],
              "新快照不把已补读的 13、全零扇区 4 列为缺失")
    finally:
        stop_server(proc, log)
        for root, dirs, _files in os.walk(data_dir):
            for d in dirs:
                try:
                    os.chmod(os.path.join(root, d), 0o755)
                except OSError:
                    pass
        shutil.rmtree(data_dir, ignore_errors=True)

    print()
    if FAILURES:
        print("存在失败项：", FAILURES)
        return 1
    print("真实进程 + 落盘/缓存故障 + 两次重启的自测全部通过 ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
