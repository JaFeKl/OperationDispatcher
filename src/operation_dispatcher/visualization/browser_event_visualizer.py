from __future__ import annotations

import json
import queue
import threading
from collections import deque
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from ..models import DispatchEvent
from ..operation_dispatcher import OperationDispatcher


class BrowserEventVisualizer:
    """
    Lightweight browser visualizer for dispatcher events.

    Typical usage:

    - Call `start()` once during setup.
    - In `on_request` callback call `on_request(event)`.
    - In `on_notification` callback call `on_notification(event)`.
    - Open `url` in a browser to view a live timeline.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        max_events: int = 1000,
        operation_dispatcher: OperationDispatcher | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._max_events = max_events

        self._events: deque[dict[str, Any]] = deque(maxlen=max_events)
        self._events_lock = threading.Lock()

        self._subscribers: set[queue.Queue[str]] = set()
        self._subscribers_lock = threading.Lock()

        self._server: ThreadingHTTPServer | None = None
        self._server_thread: threading.Thread | None = None
        self._is_running = False
        self._operation_dispatcher = operation_dispatcher

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        browser_host = self._host
        if browser_host == "0.0.0.0":
            browser_host = "127.0.0.1"
        if browser_host == "::":
            browser_host = "[::1]"
        return f"http://{browser_host}:{self._port}"

    @property
    def is_running(self) -> bool:
        return self._is_running

    def start(self) -> None:
        if self._is_running:
            return

        handler_cls = self._build_handler_class()
        self._server = ThreadingHTTPServer((self._host, self._port), handler_cls)
        self._server.daemon_threads = True

        def run_server() -> None:
            assert self._server is not None
            self._server.serve_forever(poll_interval=0.2)

        self._server_thread = threading.Thread(
            target=run_server,
            name="BrowserEventVisualizerServer",
            daemon=True,
        )
        self._is_running = True
        self._server_thread.start()

    def stop(self) -> None:
        if not self._is_running:
            return

        server = self._server
        if server is not None:
            server.shutdown()
            server.server_close()

        thread = self._server_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)

        self._server = None
        self._server_thread = None
        self._is_running = False

        with self._subscribers_lock:
            for subscriber in list(self._subscribers):
                try:
                    subscriber.put_nowait("event: close\\ndata: {}\\n\\n")
                except queue.Full:
                    pass
            self._subscribers.clear()

    def on_request(self, event: DispatchEvent) -> None:
        self.publish_dispatch_event(event, source="request")

    def on_notification(self, event: DispatchEvent) -> None:
        self.publish_dispatch_event(event, source="notification")

    def publish_dispatch_event(
        self,
        event: DispatchEvent,
        source: str,
    ) -> None:
        event_details = {
            "id": str(event.id),
            "resource_id": event.resource_id,
            "operation_id": (
                str(event.operation_id) if event.operation_id is not None else None
            ),
            "event_type": event.event_type.name,
            "event_type_value": event.event_type.value,
            "created_at": event.created_at.isoformat(),
            "changes": [change.model_dump(mode="json") for change in event.changes],
            "meta_data": event.meta_data,
        }
        operation_details = self._resolve_related_details(event)

        self.publish_event(
            event_type=event.event_type.name,
            resource_id=event.resource_id,
            operation_id=event.operation_id,
            source=source,
            meta_data=event.meta_data,
            changes=[change.model_dump(mode="json") for change in event.changes],
            event_details=event_details,
            operation_details=operation_details,
        )

    def publish_event(
        self,
        event_type: str,
        resource_id: str | None = None,
        operation_id: UUID | str | None = None,
        source: str = "custom",
        meta_data: dict[str, Any] | None = None,
        changes: list[dict[str, Any]] | None = None,
        event_details: dict[str, Any] | None = None,
        operation_details: dict[str, Any] | None = None,
    ) -> None:
        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "source": source,
            "resource_id": resource_id,
            "operation_id": (
                str(operation_id) if isinstance(operation_id, UUID) else operation_id
            ),
            "meta_data": meta_data or {},
            "changes": changes or [],
            "event_details": event_details,
            "operation_details": operation_details,
        }

        with self._events_lock:
            self._events.append(message)

        encoded = f"event: dispatch\\ndata: {json.dumps(message)}\\n\\n"

        stale_subscribers: list[queue.Queue[str]] = []
        with self._subscribers_lock:
            for subscriber in self._subscribers:
                try:
                    subscriber.put_nowait(encoded)
                except queue.Full:
                    stale_subscribers.append(subscriber)

            for stale in stale_subscribers:
                self._subscribers.discard(stale)

    def _resolve_related_details(
        self,
        event: DispatchEvent,
    ) -> dict[str, Any] | None:
        operation_dispatcher = self._operation_dispatcher
        if operation_dispatcher is None:
            return None

        operation_id = event.operation_id
        if operation_id is None:
            return None

        operation = operation_dispatcher.get_operation(operation_id)
        if operation is None:
            history = operation_dispatcher.get_history(limit=self._max_events)
            for history_record in history.records:
                if history_record.operation.id != operation_id:
                    continue
                operation = history_record.operation
                break

        if operation is None:
            return None

        return operation.model_dump(mode="json")

    def _get_history(self) -> list[dict[str, Any]]:
        with self._events_lock:
            return list(self._events)

    def _build_handler_class(self) -> type[BaseHTTPRequestHandler]:
        visualizer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                request_path = urlsplit(self.path).path

                if request_path == "/":
                    self._serve_index()
                    return
                if request_path == "/history":
                    self._serve_history()
                    return
                if request_path == "/events":
                    self._serve_events()
                    return

                self.send_response(HTTPStatus.NOT_FOUND)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _serve_index(self) -> None:
                body = _INDEX_HTML.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _serve_history(self) -> None:
                history = visualizer._get_history()
                body = json.dumps({"events": history}).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _serve_events(self) -> None:
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache, no-transform")
                self.send_header("Connection", "keep-alive")
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()

                self.wfile.write(b"event: ready\\ndata: {}\\n\\n")
                self.wfile.flush()

                subscriber_queue: queue.Queue[str] = queue.Queue(maxsize=200)
                with visualizer._subscribers_lock:
                    visualizer._subscribers.add(subscriber_queue)

                try:
                    while visualizer._is_running:
                        try:
                            message = subscriber_queue.get(timeout=1.0)
                            self.wfile.write(message.encode("utf-8"))
                            self.wfile.flush()
                        except queue.Empty:
                            self.wfile.write(b": keepalive\\n\\n")
                            self.wfile.flush()
                except (ConnectionResetError, BrokenPipeError):
                    pass
                finally:
                    with visualizer._subscribers_lock:
                        visualizer._subscribers.discard(subscriber_queue)

        return Handler


_INDEX_HTML = """<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Operation Dispatcher Visualizer</title>
    <style>
      :root {
        color-scheme: light dark;
        --bg: #0f172a;
        --panel: #111827;
        --muted: #94a3b8;
        --fg: #e5e7eb;
        --accent: #22d3ee;
        --ok: #34d399;
        --warn: #f59e0b;
        --err: #f87171;
      }
      body {
        margin: 0;
        font-family: Inter, Segoe UI, Roboto, sans-serif;
        background: linear-gradient(180deg, #020617 0%, #111827 100%);
        color: var(--fg);
      }
      .wrap {
        max-width: 1080px;
        margin: 0 auto;
        padding: 24px;
      }
      .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        margin-bottom: 16px;
      }
      .title {
        font-size: 22px;
        font-weight: 700;
      }
      .status {
        font-size: 13px;
        color: var(--muted);
      }
      .timeline {
        display: grid;
        gap: 10px;
      }
      .event {
        background: rgba(17, 24, 39, 0.95);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 12px;
        padding: 12px 14px;
      }
      .event-layout {
        display: flex;
        justify-content: flex-start;
        align-items: flex-start;
        gap: 12px;
      }
      .event-main {
        min-width: 0;
        flex: 0 1 auto;
      }
      .row {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        align-items: center;
      }
      .chip {
        display: inline-block;
        padding: 3px 8px;
        border-radius: 999px;
        font-size: 12px;
        border: 1px solid rgba(148, 163, 184, 0.35);
      }
      .chip.source-request { color: var(--warn); }
      .chip.source-notification { color: var(--ok); }
      .chip.source-custom { color: var(--accent); }
      .type {
        font-weight: 600;
        letter-spacing: 0.2px;
      }
      .meta {
        font-size: 12px;
        color: var(--muted);
      }
      .details-grid {
        display: flex;
        flex-direction: column;
        gap: 8px;
        width: 460px;
        max-width: 60%;
        margin-left: 18px;
        margin-right: 0;
      }
      .details-grid details {
        margin-top: 0;
      }
      details {
        margin-top: 8px;
      }
      @media (max-width: 900px) {
        .event-layout {
          flex-direction: column;
        }
        .details-grid {
          width: 100%;
          max-width: 100%;
        }
      }
      summary {
        cursor: pointer;
        font-size: 12px;
        color: var(--accent);
        user-select: none;
      }
      pre {
        margin: 8px 0 0 0;
        background: rgba(2, 6, 23, 0.8);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 8px;
        padding: 10px;
        overflow: auto;
        white-space: pre;
        font-size: 12px;
      }
      .empty {
        color: var(--muted);
        padding: 20px;
        border: 1px dashed rgba(148, 163, 184, 0.35);
        border-radius: 12px;
      }
    </style>
  </head>
  <body>
    <div class=\"wrap\">
      <div class=\"header\">
        <div>
          <div class=\"title\">Operation Dispatcher Event Stream</div>
          <div class=\"status\" id=\"status\">Connecting…</div>
        </div>
      </div>
      <div class=\"timeline\" id=\"timeline\"></div>
    </div>

    <script>
      const statusEl = document.getElementById('status');
      const timelineEl = document.getElementById('timeline');
      const seenEventKeys = new Set();

      const colorClassBySource = {
        request: 'source-request',
        notification: 'source-notification',
        custom: 'source-custom',
      };

      function eventKey(event) {
        const details = event.event_details || {};
        const id = details.id || '';
        return `${id}|${event.timestamp}|${event.event_type}|${event.source}|${event.resource_id || ''}|${event.operation_id || ''}`;
      }

      function toLocalTime(isoTs) {
        try {
          return new Date(isoTs).toLocaleTimeString();
        } catch {
          return isoTs;
        }
      }

      function renderEvent(event) {
        const key = eventKey(event);
        if (seenEventKeys.has(key)) {
          return;
        }
        seenEventKeys.add(key);

        const item = document.createElement('div');
        item.className = 'event';

        const sourceClass = colorClassBySource[event.source] || 'source-custom';
        const resourceId = event.resource_id || '';
        const operationId = event.operation_id || '';
        const metaParts = [];
        if (resourceId !== '') metaParts.push(`resource_id=${resourceId}`);
        if (operationId !== '') metaParts.push(`operation_id=${operationId}`);
        const eventDetails = event.event_details || {
          meta_data: event.meta_data || {},
          changes: event.changes || [],
        };
        const operationDetails = event.operation_details || null;

        function detailsBlock(title, data) {
          return `
            <details>
              <summary>${title}</summary>
              <pre>${JSON.stringify(data, null, 2)}</pre>
            </details>
          `;
        }

        let detailsHtml = detailsBlock('Show event details', eventDetails);
        if (hasOperationContext && operationDetails !== null) {
          detailsHtml += detailsBlock('Show operation details', operationDetails);
        }

        const operationMeta = metaParts.length > 0 ? ` · ${metaParts.join(' · ')}` : '';

        item.innerHTML = `
          <div class="event-layout">
            <div class="event-main">
              <div class="row">
                <span class="chip ${sourceClass}">${event.source}</span>
                <span class="type">${event.event_type}</span>
              </div>
              <div class="meta">${toLocalTime(event.timestamp)}${operationMeta}</div>
            </div>
            <div class="details-grid">
              ${detailsHtml}
            </div>
          </div>
        `;

        timelineEl.prepend(item);
      }

      function renderEmpty() {
        if (timelineEl.children.length > 0) return;
        const empty = document.createElement('div');
        empty.className = 'empty';
        empty.textContent = 'No events yet. Trigger dispatcher actions to see updates.';
        timelineEl.appendChild(empty);
      }

      async function loadHistory() {
        const response = await fetch(`/history?t=${Date.now()}`);
        const payload = await response.json();
        const events = payload.events || [];
        for (const event of events) {
          renderEvent(event);
        }
        renderEmpty();
      }

      function startPollingFallback() {
        setInterval(() => {
          loadHistory().catch(() => {
            // best-effort fallback path
          });
        }, 1000);
      }

      function connect() {
        const eventSource = new EventSource('/events');

        eventSource.onopen = () => {
          statusEl.textContent = 'Connected (live stream)';
        };

        eventSource.onerror = () => {
          statusEl.textContent = 'Disconnected, using polling fallback…';
        };

        eventSource.addEventListener('dispatch', (event) => {
          const parsed = JSON.parse(event.data);
          const empty = timelineEl.querySelector('.empty');
          if (empty) empty.remove();
          renderEvent(parsed);
        });
      }

      loadHistory().then(() => {
        connect();
        startPollingFallback();
      }).catch((error) => {
        statusEl.textContent = `Failed to load history: ${error}`;
      });
    </script>
  </body>
</html>
"""
