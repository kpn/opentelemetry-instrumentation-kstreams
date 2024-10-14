from kstreams import Stream, StreamEngine
from wrapt import BoundFunctionWrapper

from opentelemetry_instrumentation_kstreams import KStreamsInstrumentor


def test_instrument_api() -> None:
    instrumentation = KStreamsInstrumentor()
    instrumentation.instrument()
    assert isinstance(StreamEngine.send, BoundFunctionWrapper)
    assert isinstance(Stream.get_middlewares, BoundFunctionWrapper)
