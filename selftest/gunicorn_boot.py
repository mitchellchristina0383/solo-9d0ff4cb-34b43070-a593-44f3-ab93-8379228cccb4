"""本机无 pip 时，用 .pylibs 里的 gunicorn 启动的引导器（等价镜像 CMD）。

用法：
    python3 -I selftest/gunicorn_boot.py --bind 127.0.0.1:8811 \\
        --workers 1 --threads 8 wsgi:app

Docker 镜像内依赖已装到 site-packages，直接用 gunicorn 命令即可，不需要本文件。
"""
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (_ROOT, os.path.join(_ROOT, ".pylibs")):
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from gunicorn.app.wsgiapp import WSGIApplication  # noqa: E402

if __name__ == "__main__":
    WSGIApplication("%(prog)s [options]").run()
