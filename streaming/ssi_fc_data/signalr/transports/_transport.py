import websocket
import threading
import requests
import traceback
import time
import ssl
import certifi
import socket
from .reconnection import ConnectionStateChecker
from .connection import ConnectionState
from ..hubs.errors import HubError, HubConnectionError, UnAuthorizedHubError
from .base_transport import BaseTransport
from ..helpers import Helpers
from json import dumps, loads
from urllib.parse import urlparse, urlunparse, urlencode
from queue import Queue, Empty

class WebsocketTransport(BaseTransport):
    def __init__(self,
            url="",
            headers=None,
            keep_alive_interval=30,
            reconnection_handler=None,
            verify_ssl=True,               # bật verify SSL mặc định
            skip_negotiation=False,
            enable_trace=False,
            **kwargs):
        super(WebsocketTransport, self).__init__(**kwargs)
        self.protocol_version = '1.5'
        self._ws = None
        self.enable_trace = enable_trace
        self._thread = None
        self.skip_negotiation = skip_negotiation
        self.url = url
        if headers is None:
            self.headers = dict()
        else:
            self.headers = headers
        self.handshake_received = False
        self.token = None  # auth
        self.state = ConnectionState.disconnected
        self.connection_alive = False
        self._thread = None
        self._ws = None
        self._errq = Queue()
        self._last_error = None  # giữ lỗi gần nhất để _runner đẩy ra ngoài
        self.verify_ssl = verify_ssl
        self.conn_data = ''
        self.connection_checker = ConnectionStateChecker(
            lambda: self.logger.debug("Ping"),
            keep_alive_interval
        )
        self.reconnection_handler = reconnection_handler
        self._stopping = False

        if len(self.logger.handlers) > 0:
            websocket.enableTrace(self.enable_trace, self.logger.handlers[0])
        
        self.keep_alive_interval = int(keep_alive_interval)  # s (mặc định 15)
        self.server_timeout = 5400                              # s (có thể đưa thành tham số)
        self._watchdog_grace = 30                              # s
        self._last_rx_ts = 0.0
        self._last_tx_ts = 0.0
        self._stop_evt = threading.Event()
        self._hb_thread = None
        self._consumer_thread = None
        self._in_queue = Queue()  # tránh block on_message
    
    def is_running(self):
        return self.state != ConnectionState.disconnected

    def stop(self):
        self._stopping = True
        self._stop_evt.set() 
        try:
            if self.state == ConnectionState.connected:
                try:
                    self.connection_checker.stop()
                except Exception:
                    pass
            if self._ws is not None:
                try:
                    # close "mềm"; tham số tùy lib, để trống cho tương thích
                    self._ws.close()
                except Exception:
                    pass
            # << THÊM: join các thread phụ
            try:
                if self._hb_thread and self._hb_thread.is_alive():
                    self._hb_thread.join(timeout=1.0)
            except Exception:
                pass
            try:
                if self._consumer_thread and self._consumer_thread.is_alive():
                    self._consumer_thread.join(timeout=1.0)
            except Exception:
                pass
        finally:
            self.state = ConnectionState.disconnected
            self.handshake_received = False

    def start(self, connectionData = ''):
        self.conn_data = connectionData
        if not self.skip_negotiation:
            self._negotiate()

        if self.state == ConnectionState.connected:
            self.logger.warning("Already connected unable to start")
            return False

        self.state = ConnectionState.connecting
        self.logger.debug("start url:" + self.url)
        # BẢO ĐẢM GỬI ORIGIN HỢP LỆ (http/https tương ứng ws/wss)
        try:
            _p = urlparse(self.url)
            _origin_scheme = "https" if _p.scheme in ("wss", "https") else "http"
            _origin = f"{_origin_scheme}://{_p.hostname}"
            if _p.port:
                _origin += f":{_p.port}"
            self.headers.setdefault("Origin", _origin)
        except Exception:
            pass
        self._ws = websocket.WebSocketApp(
            self.url,
            header=self.headers,
            on_message=self.on_message,
            on_error=self.on_socket_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
            
        def _runner():
            try:
                # (tuỳ chọn) đặt socket timeout toàn cục, tương đương ý nghĩa bạn muốn
                websocket.setdefaulttimeout(60)
        
                # chuẩn bị sslopt như cũ
                sslopt = (
                    {"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()}
                    if self.verify_ssl else
                    {"cert_reqs": ssl.CERT_NONE}
                )
        
                # một số hệ (Windows/macOS) có thể không có TCP_KEEP* -> bọc try
                sockopt = [
                    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                ]
                try:
                    sockopt += [
                        (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30),
                        (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10),
                        (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3),
                    ]
                except Exception:
                    pass
        
                self._ws.run_forever(
                    sslopt=sslopt,
                    ping_interval=0,    # tắt WS ping để khỏi timeout giả
                    ping_timeout=None,
                    # timeout=60,       # <-- GỠ DÒNG NÀY
                    sockopt=sockopt,
                )
            finally:
                if self._last_error:
                    self._errq.put(self._last_error); self._last_error = None
                elif not self._stopping:
                    self._errq.put(HubConnectionError("WS stopped"))
                self._stopping = False


        self._thread = threading.Thread(target=_runner)
        self._thread.daemon = True
        self._thread.start()
        # Heartbeat + watchdog
        if not self._hb_thread or not self._hb_thread.is_alive():
            self._stop_evt.clear()
            self._hb_thread = threading.Thread(target=self._heartbeat_loop, name="ws-hb", daemon=True)
            self._hb_thread.start()

        # Consumer tách xử lý nặng khỏi on_message
        if not self._consumer_thread or not self._consumer_thread.is_alive():
            self._consumer_thread = threading.Thread(target=self._consumer_loop, name="ws-consumer", daemon=True)
            self._consumer_thread.start()

        return True

    def check_and_raise(self, timeout: float = 0.0):
        try:
            err = self._errq.get_nowait() if timeout == 0 else self._errq.get(timeout=timeout)
        except Empty:
            return
        raise err

    @staticmethod
    def _format_url(url, action, query):
        return '{url}/{action}?{query}'.format(url=url, action=action, query=query)
    
    def _get_ws_url_from(self):
        parsed = urlparse(self.url)
        scheme = 'wss' if parsed.scheme == 'https' else 'ws'
        url_data = (scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
        return urlunparse(url_data)

    @staticmethod
    def _get_cookie_str(request):
        return '; '.join([
            '%s=%s' % (name, value)
            for name, value in request.items()
        ])

    def _get_socket_url(self, responseData):
        ws_url = self._get_ws_url_from()
        query = urlencode({
            'transport': 'webSockets',
            'connectionToken': responseData['ConnectionToken'],
            'connectionData': self.conn_data,
            'clientProtocol': responseData['ProtocolVersion'],
        })
        return self._format_url(ws_url, 'connect', query)

    def _negotiate(self):
        query = urlencode({
            'connectionData': self.conn_data,
            'clientProtocol': self.protocol_version,
        })
        negotiate_url = self._format_url(self.url, 'negotiate', query)
        self.logger.debug("Negotiate url:{0}".format(negotiate_url))

        response = requests.post(
            negotiate_url, headers=self.headers,
            verify=(certifi.where() if self.verify_ssl else False)
        )
        self.logger.debug(
            "Response status code{0}".format(response.status_code))

        if response.status_code != 200:
            raise HubError(response.status_code) if response.status_code != 401 else UnAuthorizedHubError()

        data = response.json()
        # Gắn cookie từ negotiate để WS handshake hợp lệ
        try:
            if response.cookies:
                self.headers['Cookie'] = self._get_cookie_str(response.cookies)
        except Exception:
            pass
        self.url = self._get_socket_url(data)

    def on_open(self, _):
        self.logger.debug("-- web socket open --")
        self.state = ConnectionState.connected

        # Mốc thời gian để heartbeat/watchdog
        now = time.time()
        self._last_rx_ts = now
        self._last_tx_ts = now

        # (Tuỳ server) SignalR JSON protocol handshake; bỏ nếu bạn không dùng JSON
        try:
            hs = dumps({"protocol": "json", "version": 1}) + "\x1e"
            self._ws.send(hs)
        except Exception:
            self.logger.debug("handshake send failed", exc_info=True)

        try:
            self.connection_checker.start()
        except Exception:
            pass
        self._on_open()

    def on_close(self, wsapp, close_status_code, close_reason):
        self.logger.debug("-- web socket close --")
        self.logger.debug(close_status_code)
        self.logger.debug(close_reason)
        self.state = ConnectionState.disconnected
        if self._on_close is not None and callable(self._on_close):
            self._on_close()
        # KHÔNG raise ở đây – callback sẽ bị WebSocketApp nuốt.
        self._last_error = HubConnectionError(
            f"WS closed code={close_status_code} reason={close_reason}"
        )
        self._stop_evt.set()   # << THÊM
        try:
            # đảm bảo run_forever thoát
            self._ws.close()
        except Exception:
            pass

    def on_reconnect(self):
        self.logger.debug("-- web socket reconnecting --")
        self.state = ConnectionState.disconnected
        if self._on_close is not None and callable(self._on_close):
            self._on_close()

    def on_socket_error(self, app, error):
        """
        Args:
            _: Required to support websocket-client version equal or greater than 0.58.0
            error ([type]): [description]

        Raises:
            HubError: [description]
        """
        self.logger.debug("-- web socket error --")
        self.logger.error(traceback.format_exc(10, True))
        self.logger.error("{0} {1}".format(self, error))
        self.logger.error("{0} {1}".format(error, type(error)))
        if self._on_close is not None and callable(self._on_close):
            self._on_close()
        self.state = ConnectionState.disconnected
        # KHÔNG raise – ghi lại để _runner đẩy ra ngoài
        self._last_error = HubConnectionError(str(error))
        self._stop_evt.set()
        try:
            self._ws.close()
        except Exception:
            pass

    def on_message(self, app, raw_message):
        self._last_rx_ts = time.time()
        try:
            self._in_queue.put_nowait(raw_message)
        except Exception:
            pass
        # Không gọi trực tiếp self._on_message ở đây

    def _consumer_loop(self):
        while not self._stop_evt.is_set():
            try:
                msg = self._in_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                if msg:
                    self._on_message(msg)
            except Exception:
                self.logger.error("handle payload failed", exc_info=True)

    def _heartbeat_loop(self):
        """
        Gửi SignalR ping (type=6) định kỳ và watchdog khi server im quá lâu.
        """
        # JSON protocol: mỗi frame kết thúc bằng RS = 0x1E
        ping_frame = dumps({"type": 6}) + "\x1e"

        while not self._stop_evt.is_set():
            now = time.time()

            # Watchdog: im lặng > server_timeout + grace -> đóng để trigger reconnect
            idle = now - self._last_rx_ts
            if self.state == ConnectionState.connected and idle > (self.server_timeout + self._watchdog_grace):
                self.logger.error(f"watchdog: no server activity for {idle:.1f}s => reconnect")
                try:
                    if self._ws is not None:
                        self._ws.close()
                except Exception:
                    pass
                self._stop_evt.set()
                break

            # Heartbeat: gửi ping type=6 mỗi keep_alive_interval giây
            since_tx = now - self._last_tx_ts
            if self.state == ConnectionState.connected and since_tx >= self.keep_alive_interval:
                try:
                    if self._ws is not None:
                        self._ws.send(ping_frame)
                        self._last_tx_ts = now
                except Exception:
                    self.logger.error("send SignalR ping failed", exc_info=True)

            time.sleep(1)



    def send(self, message):
        self.logger.debug("Sending message {0}".format(message))
        try:
            self._ws.send(dumps(message))
            self.connection_checker.last_message = time.time()
            if self.reconnection_handler is not None:
                self.reconnection_handler.reset()
        except (
                websocket._exceptions.WebSocketConnectionClosedException,
                OSError) as ex:
            self.handshake_received = False
            self.logger.warning("Connection closed {0}".format(ex))
            self.state = ConnectionState.disconnected
            if self.reconnection_handler is None:
                if self._on_close is not None and\
                        callable(self._on_close):
                    self._on_close()
                raise ValueError(str(ex))
            # Connection closed
            self.handle_reconnect()
        except Exception as ex:
            raise ex

    def handle_reconnect(self):
        if not self.reconnection_handler.reconnecting and self._on_reconnect is not None and \
                callable(self._on_reconnect):
            self._on_reconnect()
        self.reconnection_handler.reconnecting = True
        try:
            self.stop()
            self.start()
        except Exception as ex:
            self.logger.error(ex)
            sleep_time = self.reconnection_handler.next()
            threading.Thread(
                target=self.deferred_reconnect,
                args=(sleep_time,)
            ).start()

    def deferred_reconnect(self, sleep_time):
        time.sleep(sleep_time)
