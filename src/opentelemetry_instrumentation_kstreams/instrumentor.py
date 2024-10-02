from typing import Any, Collection

from kstreams import Stream, StreamEngine
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.utils import unwrap
from wrapt import wrap_function_wrapper

from .package import _instruments
from .version import __version__
from .wrappers import (
    # _wrap_getone,
    _wrap_build_stream_middleware_stack,
    _wrap_send,
)


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
        wrap_function_wrapper(
            StreamEngine,
            "build_stream_middleware_stack",
            _wrap_build_stream_middleware_stack(tracer),
        )

    def _uninstrument(self, **kwargs: Any):
        unwrap(StreamEngine, "send")
        unwrap(Stream, "build_stream_middleware_stack")
