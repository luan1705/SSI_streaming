import json
from .signalr import Connection
from .model import api
from .model import constants
from .fc_md_client import MarketDataClient


class MarketDataStream(object):

  def __init__(self, _config, client: MarketDataClient, on_close=None, on_open=None):

    self._config = _config
    self._client = client
    self._handlers = []
    self._error_handlers = []
    self._on_open = on_open
    self._on_close = on_close
    self._handlers_bound = False
    self._subscribed_channel = None


  def _on_message(self, _message):
    x = json.loads(_message)
    try:
      for _handler in self._handlers:
        _handler(x)
    except:
      raise Exception(constants.RECEIVE_ERROR_MESSAGE)

  def on_close(self):
      if self._on_close and callable(self._on_close):
        self._on_close()

  def on_open(self):
    if self._on_open and callable(self._on_open):
      self._on_open()
  def _on_error(self, _error):

    _error = _error

    for _error_handler in self._error_handlers:

      _error_handler(_error)

  def swith_channel(self, channel):
    self.hub_proxy.server.invoke('SwitchChannels', channel)
     
  def start(self, _on_message, _on_error, _selected_channel, *argv):

    # reset để tránh nhân đôi handler khi start nhiều lần
    self._handlers = [_on_message]
    self._error_handlers = [_on_error]
    self._channel = _selected_channel
    headers = {}#utils.default_headers()
    using_jwt = self._client._get_access_token()
    headers['Authorization'] = self._config.auth_type \
          + constants.ONE_WHITE_SPACE + using_jwt
      
      
    self.connection = Connection(self._config.stream_url + api.SIGNALR, headers, on_close=self.on_close, on_open=self.on_open)
    
    self.hub_proxy = self.connection.register_hub(api.SIGNALR_HUB_MD)
    # Nếu lib có off(): gỡ trước khi on để chắc chắn không nhân đôi
    try:
        self.hub_proxy.client.off(api.SIGNALR_METHOD, self._on_message)
        self.hub_proxy.client.off(api.SIGNALR_ERROR_METHOD, self._on_error)
    except Exception:
        pass
    if not self._handlers_bound:
        self.hub_proxy.client.on(api.SIGNALR_METHOD, self._on_message)
        self.hub_proxy.client.on(api.SIGNALR_ERROR_METHOD, self._on_error)
        self._handlers_bound = True
    
    # Tránh cộng dồn: gỡ cũ nếu EventHook hỗ trợ hoặc set lại
    try:
        self.connection.error.clear()  # nếu EventHook có clear()
    except Exception:
        pass
    self.connection.error += _on_error
    def _subscribe_once():
        try:
            if self._subscribed_channel != _selected_channel:
                self.hub_proxy.server.invoke('SwitchChannels', _selected_channel)
                self._subscribed_channel = _selected_channel
        except Exception as e:
            # để error flow xử lý
            pass
    self.connection.on_open_callback(_subscribe_once)
    self.connection.start()

    # LẤY transport bên dưới và BLOCK ở đây đến khi WS thread báo lỗi
    transport = getattr(self.connection, "_transport", None) or getattr(self.connection, "transport", None)
    if transport is None or not hasattr(transport, "check_and_raise"):
        raise RuntimeError("WebsocketTransport missing or not patched with check_and_raise()")
    transport.check_and_raise(timeout=None)
      