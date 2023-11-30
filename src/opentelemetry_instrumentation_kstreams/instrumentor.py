from typing import Any, Collection

from kstreams import StreamEngine, Stream
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.utils import unwrap
from wrapt import wrap_function_wrapper
from .version import __version__
from .utils import (
    _wrap_anext,
    _wrap_send,
    _wrap_getone,
)

from .package import _instruments


class KStreamsInstrumentor(BaseInstrumentor):
    """Instrument kstreams with OpenTelemetry.

    Usage:
    ```
    from opentelemetry_instrumentation_kstreams import KStreamsInstrumentor

    KStreamsInstrumentor().instrument()
    ```
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any):
        tracer_provider = kwargs.get("tracer_provider")
        tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )
        wrap_function_wrapper(StreamEngine, "send", _wrap_send(tracer))

        wrap_function_wrapper(Stream, "__anext__", _wrap_anext(tracer))

        # kstreams > 0.13.0
        if hasattr(Stream, "getone"):
            wrap_function_wrapper(Stream, "getone", _wrap_getone(tracer))

    def _uninstrument(self, **kwargs: Any):
        unwrap(StreamEngine, "send")
        unwrap(Stream, "__anext__")

        # kstreams > 0.13.0
        if hasattr(Stream, "getone"):
            unwrap(Stream, "getone")
