# 修复说明与自测报告（救援页上报流程）

日期：2026-11-25

## 一、问题与根因

对照现象定位到三个具体缺陷（均可用故障注入稳定复现，复现脚本逻辑已沉淀到
`selftest/`）：

1. **一个扇区写入错误，整批 HTTP 500**
   `JobStorage.save_batch()` 的循环里只捕获 `StorageError`（越界/长度错）。
   真实磁盘错误（`open/write/fsync/os.replace` 抛 `OSError`，如 EIO/ENOSPC/
   EACCES）会穿透循环，导致：同批后续扇区根本不再尝试，Flask 返回 500。

2. **进度显示缺失，单独读却有数据（幻影不一致）**
   `save_sector()` 的顺序是「写 tmp → fsync → rename → 目录 fsync →
   加入内存 saved」。如果在 **rename 已成功、目标 `<N>.bin` 已存在之后**
   目录 fsync/确认落盘报错，异常会在 `saved.add(index)` 之前抛出：
   磁盘文件可读，但内存进度没有该扇区。客户端重试时又命中
   `if os.path.exists(target): return False`，而该早返回分支**不会把编号
   补认回内存**，于是同一进程内一直“进度缺失、单读有数据”，直到重启扫描。

3. **进度缓存 `job.json` 写失败，把已成功的扇区也变成 500**
   `save_batch()` 末尾的 `self._persist_state()` 没有 try；扇区此时已
   fsync+rename+目录 fsync 确认落盘，但缓存写失败（ENOSPC/EACCES 等）仍让
   整个请求 500，客户端无法得知哪些扇区其实已保存。

## 二、修复内容

### `rescue/storage.py`（核心）

- `save_sector()`：
  - 写入/确认落盘任一步抛错时，把该扇区加入进程内 `_unconfirmed` 隔离集合，
    尽力删除 tmp 与可能已 rename 的目标 `.bin`，并抛出新增的
    `SectorWriteError`（同时继承 `StorageError` 与 `OSError`，兼容不同断言）。
  - 目标已是「大小恰为 512 的普通文件」时，把编号补认回内存 `saved`，
    修复幻影不一致；但位于 `_unconfirmed` 的扇区不轻信残留文件，会重写一遍。
  - 全零扇区走同一条正常写入路径，照常 saved（既有语义，补了故障批次用例）。
- `read_saved_sector()`：`_unconfirmed` 中的扇区、不可 `stat/open/read`
  的扇区、长度不是 512 的残缺文件，一律返回 `None`（接口即 409），保证
  “失败扇区在单扇区读取里也算缺失”。
- `save_batch()`：
  - 逐扇区隔离所有异常：非法条目进 `rejected`；写入/落盘失败进
    `failed_sectors`（详细 `{sector,error}`）与编号列表 `failed` /
    `need_reread`（另给 `failed_now`、`write_failed`、`write_errors`、
    `failures`、`reread(_sectors)` 等同义字段，并把落盘失败条目也并入
    `rejected`，兼容旧消费方）。**无论单扇区成败都继续循环剩余扇区**。
  - 仅当 saved 集合变化时落一次 `job.json`，并用 try 包住：缓存失败不再
    上抛，改为 `cache_ok:false` + `cache_warning`/`warning`/`cache_error`
    文案；扇区仍在 `saved_now`/`already_saved` 与内存 `progress` 中。
- 启动对账增强（`_load_or_init()`）：
  - 分片目录下所有 `*.tmp`（实际命名是 `<N>.bin.tmp`，旧正则只清 `<N>.tmp`）
    都尽力清理，清理失败不阻断启动；
  - `job.json` 缺失/损坏时不再直接崩，改为从 `sample.img` 的大小与 SHA-256
    重建 `sector_count` 等元数据（兑现 README “job.json 只是缓存、可重建”）；
  - 启动回写缓存改为尽力而为，缓存盘不可写也能启动并保持正确内存状态。

### `rescue/app.py`

- 上报路由本身直接展开 `save_batch()` 的返回，新字段自动透传，行为保持
  “单条/部分失败仍 HTTP 200”的既有约定。
- 新增 JSON 500 兜底处理器，避免意外错误返回 HTML 错误页。

### `templates/index.html`（救援页）

- 上报后分区列出：**✅ 已保存（新写入编号）**、此前已保存编号、
  **🔁 需补读编号**（落盘/确认失败，红色 chip，悬停看错误原因）、
  **⛔ 拒收编号**（非法数据/编号），并显示缓存写入警告。
- 进度区新增“需补读的缺失扇区编号”chips（数量少时默认展开）。
- 所有动态文本经 `esc()` 转义；内联 JS 通过 `node --check`，并用 Node
  桩 DOM 验证了各列表渲染。

### 测试与文档

- 新增 `selftest/test_faults.py`（6 个进程内 API/存储故障用例，可
  `python3 -I selftest/test_faults.py` 直接跑）。
- 新增 `selftest/live_fault_selftest.py`（真实 gunicorn/Flask 进程 +
  真实文件系统权限故障 + 两次重启，28 项断言；`--flask` 可切 wsgi.py）。
- 新增 `selftest/gunicorn_boot.py`（无 pip 时用 `.pylibs` 内 gunicorn
  复刻镜像 CMD 的引导器）。
- `README.md` 补充容错语义、返回字段与自测命令。
- **未改动** `Dockerfile`、`docker-compose.yml`、`requirements.txt`。

## 三、自测结果（2026-11-25，本沙箱实跑）

| 项目 | 命令 | 结果 |
| --- | --- | --- |
| 原有单元测试 | `python3 -I tests/run.py` | **38/38 OK** |
| 新增故障用例 | `python3 -I selftest/test_faults.py` | **6/6 OK** |
| 端到端中断重启/边读边下载 | `python3 -I tests/integration_test.py` | 全部 ✓ |
| sim_reader 真实 HTTP 冒烟 | `python3 -I tests/smoke_test.py` | 通过（300 扇区，补读到只剩注入坏扇区，快照一致） |
| 真实进程故障+两次重启 | `python3 -I selftest/live_fault_selftest.py` | **28 项 ✓，退出码 0** |
| 同上（Flask 模式） | `... --flask` | **28 项 ✓，退出码 0** |
| 页面 JS 语法 / 渲染 | `node --check` + Node 桩 DOM | 语法 OK，7/7 渲染断言 ✓ |
| 镜像同款 CMD | `gunicorn -b 0.0.0.0:8000 -w 1 --threads 8 --timeout 120 wsgi:app`（gunicorn 23.0.0 + Flask 3.1.3） | 启动正常，`GET /` 200，`GET /api/job` 404 JSON |

### 故障 1：扇区落盘报错（分别自测）

用了三种注入，结论一致：

- **真实 FS 权限故障**：`chmod 555 sectors/007`（及 013）使
  `open(.../7.bin.tmp,'wb')` 得到 EACCES。批量 `[6,7,8,9]` 仍 **HTTP 200**，
  `saved_now=[6,8,9]`、`failed=[7]`、`need_reread=[7]`；
  `GET /api/sectors/7` = **409**，`/api/missing` 与新快照 missing 均含 7，
  快照镜像 7 号位置补零。恢复权限后补读 7 成功。
- **“确认落盘”失败**：mock `os.replace` 先真实 rename 再对 `3.bin` 抛
  OSError（复现“文件已存在但目录 fsync 确认失败”）。修复后目标文件被清理、
  进度缺失、单读 409，不再出现幻影。
- **mock 磁盘错误**：`mock.patch.object(storage, "save_sector", ...)`
  对 2 抛 OSError，以及 `os.fsync` 首次调用抛 EIO：同批其余扇区照常保存。
- **重启核对**：13 号保持失败、不补读直接重启 → 新 `JobStorage` 扫描后
  `saved=[0,1,2,6,7,8,9,10,11,12,14]`、13 仍 missing、GET 409；恢复权限后
  补读 13，**第二次重启**后 13 仍在且数据逐字节正确。

### 故障 2：进度缓存 `job.json` 报错（分别自测）

- **真实 FS 权限故障**：扇区写入 `sectors/0xx/` 正常，但把数据根目录
  `chmod 555`，使 `_persist_state()` 在数据目录创建 `job.json.*.tmp` 时
  EACCES。上报 `[10,11]` 仍 **HTTP 200**、`saved_now=[10,11]`、
  `cache_ok=false` 且有 warning；同进程 `/api/job` 显示 10、11 已保存，
  `GET /api/sectors/10` 数据正确。
- **mock 注入**：`mock.patch.object(JobStorage, "_persist_state",
  side_effect=OSError("ENOSPC job.json"))`，结论相同；在 patch 未退出时
  重新构造 `JobStorage` 也不会被启动回写失败击倒。
- **重启核对**：磁盘上的 `job.json` 还停留在只含
  `{0,1,2,6,7,8,9,12,14}` 的旧版本，但重启以 sectors/ 为准扫描，10、11
  被正确认回（saved 恰为 11 个预期编号）。

### 全零扇区

实时用例中上报 4 号全零扇区后 `saved_now=[4]`、GET 返回 512 个 0、新快照
不把它列入 missing；既有 `test_zero_sector_not_missing` 等用例也仍通过。

## 四、Docker Compose 与环境限制（未完成项，如实说明）

- **本沙箱内没有 Docker**：无 `docker`/`docker-compose` 可执行文件、无
  `/var/run/docker.sock`、未设 `DOCKER_HOST`、无 2375 端口，当前用户
  uid=1001 且无 sudo/root；即使关闭沙箱执行也仍是同一用户、同样找不到
  docker。因此**无法在此环境真实执行 `docker compose up --build -d`**，
  这是唯一未能照做的指定动作。
- 已用与镜像完全相同的依赖版本（requirements.txt 钉的 Flask 3.1.3 /
  gunicorn 23.0.0，及其兼容依赖）和 **Dockerfile 中的同款 CMD**
  （`gunicorn -b 0.0.0.0:8000 -w 1 --threads 8 --timeout 120 wsgi:app`）
  在本机起真实 HTTP 服务验证，并跑了故障注入与重启；
  `docker-compose.yml` / `Dockerfile` 相对初始提交**未改动**。
- 在装有 Docker 的目标机上请直接：

  ```bash
  docker compose up --build -d
  # 打开 http://localhost:8000
  docker compose logs -f
  ```

## 五、其他备注

- 为在无 pip 的沙箱里跑测试，依赖 wheel 被解压到 `/workspace/.pylibs/`
  （已在 `.gitignore` 中，不影响镜像构建，Dockerfile 也只 COPY 指定目录）。
- 快照仍保存在 gunicorn 单进程内存中，重启后旧 `snapshot_id` 失效——这是
  README 已声明的既有设计，本次未改。
- 未执行任何 git commit/push（任务未要求）；所有改动都在工作区。
