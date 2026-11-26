"""HTTP API + SSE + 静态页服务（仅依赖标准库）。"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import crd
from .app import SimulationApp
from .conversion import ConversionError
from .store import ConflictError, NotFoundError, VersionNotServedError

STATIC_DIR = Path(__file__).resolve().parent / "static"


class ApiHandler(BaseHTTPRequestHandler):
    app: SimulationApp  # 由工厂注入

    # ---------------------------------------------------------- 基础工具
    def log_message(self, fmt: str, *args) -> None:  # 安静一点
        pass

    def _json(self, payload, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, exc: Exception, status: int) -> None:
        self._json({"error": type(exc).__name__, "message": str(exc)}, status)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"请求体不是合法 JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return data

    def _exception_guard(self, fn):
        try:
            return fn()
        except ConflictError as exc:
            self._error(exc, 409)
        except NotFoundError as exc:
            self._error(exc, 404)
        except VersionNotServedError as exc:
            self._error(exc, 410)
        except (ConversionError, ValueError) as exc:
            self._error(exc, 422)
        except RuntimeError as exc:
            self._error(exc, 409)

    # ---------------------------------------------------------- 路由
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/"):
            self._exception_guard(lambda: self._handle_api_get(path, parse_qs(parsed.query)))
            return
        self._serve_static(path)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path.startswith("/api/"):
            self._exception_guard(lambda: self._handle_api_post(path))

    def do_PUT(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path.startswith("/api/"):
            self._exception_guard(lambda: self._handle_api_put(path))

    def do_DELETE(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path.startswith("/api/"):
            self._exception_guard(lambda: self._handle_api_delete(path))

    # ---------------------------------------------------------- GET API
    def _handle_api_get(self, path: str, query: dict) -> None:
        app = self.app
        if path == "/api/overview":
            return self._json(app.overview())
        if path == "/api/migrator":
            return self._json(app.migrator.status())
        if path == "/api/objects":
            return self._json(
                [app.store.views(r["name"]) for r in app.store.list()]
            )
        if path.startswith("/api/objects/"):
            name = path.rsplit("/", 1)[-1]
            return self._json(app.store.views(name))
        if path == "/api/crd":
            return self._json(app.definition.snapshot())
        if path == "/api/events":
            return self._sse()
        raise NotFoundError(f"没有这个接口: {path}")

    # ---------------------------------------------------------- POST API
    def _handle_api_post(self, path: str) -> None:
        app = self.app
        body = self._body()

        if path == "/api/reset":
            app.reset()
            return self._json(app.overview())

        if path == "/api/conversion-review":
            return self._json(app.converter.serve_conversion_review(body))

        if path == "/api/objects":
            stored, meta = app.store.create(body)
            return self._json({"stored": stored, "meta": meta}, 201)

        if path.endswith("/patch"):
            name = path.split("/")[3]
            version = body.get("version", crd.V1ALPHA1)
            client = app.client(version, f"api-client:{version}")
            stored = client.patch(name, body.get("specPatch", {}))
            return self._json({"stored": stored})

        if path.endswith("/status"):
            name = path.split("/")[3]
            version = body.get("version", crd.V1)
            client = app.client(version, f"api-client:{version}")
            stored = client.update_status(name, body.get("statusPatch", {}))
            return self._json({"stored": stored})

        if path.endswith("/blast"):
            name = path.split("/")[3]
            results = app.blast.run(
                name,
                old_patch=body.get("oldPatch"),
                new_patch=body.get("newPatch"),
                old_status=body.get("oldStatus"),
                new_status=body.get("newStatus"),
            )
            return self._json({"results": results, "views": app.store.views(name)})

        if path == "/api/migrate/flip":
            status = app.migrator.flip_storage_version(body.get("version", crd.V1))
            return self._json(status)
        if path == "/api/migrate/start":
            return self._json(app.migrator.start())
        if path == "/api/migrate/retry":
            return self._json(app.migrator.retry_failed())
        if path == "/api/migrate/rollback":
            return self._json(app.migrator.rollback())
        if path == "/api/migrate/finalize":
            return self._json(app.migrator.finalize())
        if path == "/api/migrator/config":
            return self._json(app.migrator.update_config(body))

        raise NotFoundError(f"没有这个接口: {path}")

    # ---------------------------------------------------------- PUT/DELETE
    def _handle_api_put(self, path: str) -> None:
        app = self.app
        body = self._body()
        if path.startswith("/api/objects/"):
            name = path.rsplit("/", 1)[-1]
            body.setdefault("metadata", {})["name"] = name
            expected = body.get("metadata", {}).pop("resourceVersion", None)
            stored, meta = app.store.update(body, expected_rv=expected)
            return self._json({"stored": stored, "meta": meta})
        raise NotFoundError(f"没有这个接口: {path}")

    def _handle_api_delete(self, path: str) -> None:
        app = self.app
        if path.startswith("/api/objects/"):
            name = path.rsplit("/", 1)[-1]
            app.store.delete(name)
            return self._json({"deleted": name})
        raise NotFoundError(f"没有这个接口: {path}")

    # ---------------------------------------------------------- SSE
    def _sse(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        subscription = self.app.bus.subscribe()
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    event = subscription.wait(timeout=15)
                except Exception:
                    event = None
                if event is None:
                    self.wfile.write(b": keepalive\n\n")
                else:
                    payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
                    self.wfile.write(
                        b"event: " + event["type"].encode() + b"\n"
                        b"id: " + str(event["seq"]).encode() + b"\n"
                        b"data: " + payload + b"\n\n"
                    )
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            subscription.close()

    # ---------------------------------------------------------- 静态文件
    def _serve_static(self, path: str) -> None:
        if path in ("/", "/index.html"):
            target = STATIC_DIR / "index.html"
        else:
            target = (STATIC_DIR / path.lstrip("/")).resolve()
            if STATIC_DIR not in target.parents and target != STATIC_DIR:
                return self._json({"error": "forbidden"}, 403)
        if not target.exists() or target.is_dir():
            return self._error(NotFoundError(path), 404)
        content = target.read_bytes()
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "text/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
        }.get(target.suffix, "application/octet-stream")
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def build_server(host: str = "127.0.0.1", port: int = 8080) -> tuple[ThreadingHTTPServer, SimulationApp]:
    app = SimulationApp()

    class _BoundHandler(ApiHandler):
        pass

    _BoundHandler.app = app
    server = ThreadingHTTPServer((host, port), _BoundHandler)
    server.daemon_threads = True
    return server, app


def serve(host: str = "127.0.0.1", port: int = 8080) -> None:
    server, app = build_server(host, port)
    print(f"CRD 存储版本迁移模拟台: http://{host}:{port}")
    print("按 Ctrl+C 退出")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.migrator.shutdown()
        server.shutdown()
