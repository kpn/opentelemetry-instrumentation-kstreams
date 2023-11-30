from kstreams import Stream, StreamEngine
from opentelemetry_instrumentation_kstreams import KStreamsInstrumentor
from wrapt import BoundFunctionWrapper


def test_instrument_api() -> None:
    instrumentation = KStreamsInstrumentor()
    instrumentation.instrument()
    assert isinstance(StreamEngine.send, BoundFunctionWrapper)
    assert isinstance(Stream.__anext__, BoundFunctionWrapper)
