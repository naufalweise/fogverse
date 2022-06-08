import inspect
import traceback

def _get_func(obj, func_name):
    func = getattr(obj, func_name, None)
    return func if callable(func) else None

def _call_func(obj, func_name, args=[], kwargs={}):
    func = _get_func(obj, func_name)
    return func(*args, **kwargs) if func is not None else None

async def _call_func_async(obj, func_name):
    coro = _call_func(obj, func_name)
    return await coro if coro is not None else None

class Runnable:
    def on_error(self, _):
        traceback.print_exc()

    def process(self, data):
        return data

    async def _start(self):
        if getattr(self, '_started', False) == True: return
        await _call_func_async(self, 'start_consumer')
        await _call_func_async(self, 'start_producer')
        self._started = True

    async def _close(self):
        await _call_func_async(self, 'close_producer')
        await _call_func_async(self, 'close_consumer')

    async def run(self):
        _call_func(self, '_before_start')
        await self._start()
        _call_func(self, '_after_start')
        try:
            while True:
                _call_func(self, '_before_receive')
                self.message = await self.receive()
                if self.message is None: continue
                _call_func(self, '_after_receive', args=(self.message,))

                # kafka and opencv consumer compatibility
                getvalue = getattr(self.message, 'value', None)
                if getvalue is None:
                    value = self.message
                elif callable(getvalue):
                    value = self.message.value()
                else:
                    value = self.message.value

                _call_func(self, '_before_decode', args=(value,))
                data = self.decode(value)
                _call_func(self, '_after_decode', args=(data,))

                _call_func(self, '_before_process', args=(data,))
                result = self.process(data)
                if inspect.iscoroutine(result):
                    result = await result
                _call_func(self, '_after_process', args=(result,))

                _call_func(self, '_before_encode', args=(result,))
                result_bytes = self.encode(result)
                _call_func(self, '_after_encode', args=(result_bytes,))

                _call_func(self, '_before_send', args=(result_bytes,))
                await self.send(result_bytes)
                _call_func(self, '_after_send', args=(result_bytes,))
        except Exception as e:
            self.on_error(e)
        finally:
            _call_func(self, '_before_close')
            await self._close()
            _call_func(self, '_after_close')
