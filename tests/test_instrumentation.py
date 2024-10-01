from kstreams import StreamEngine
from wrapt import BoundFunctionWrapper

from opentelemetry_instrumentation_kstreams import KStreamsInstrumentor


def test_instrument_api() -> None:
    instrumentation = KStreamsInstrumentor()
    instrumentation.instrument()
    assert isinstance(StreamEngine.send, BoundFunctionWrapper)
    assert isinstance(StreamEngine.build_stream_middleware_stack, BoundFunctionWrapper)
