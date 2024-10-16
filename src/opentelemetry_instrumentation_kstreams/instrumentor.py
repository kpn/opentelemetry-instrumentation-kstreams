from typing import Any, Collection

from kstreams import Stream, StreamEngine
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.utils import unwrap
from wrapt import wrap_function_wrapper

from .package import _instruments
from .version import __version__
from .wrappers import (
    _wrap_build_stream_middleware_stack,
    _wrap_get_middlewares,
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

        # kstreams >= 0.24.1
        if hasattr(Stream, "get_middlewares"):
            wrap_function_wrapper(
                Stream,
                "get_middlewares",
                _wrap_get_middlewares(tracer),
            )
        else:
            wrap_function_wrapper(
                StreamEngine,
                "_build_stream_middleware_stack",
                _wrap_build_stream_middleware_stack(tracer),
            )

    def _uninstrument(self, **kwargs: Any):
        unwrap(StreamEngine, "send")

        # kstreams >= 0.24.1
        if hasattr(Stream, "get_middlewares"):
            unwrap(Stream, "get_middlewares")
        else:
            unwrap(StreamEngine, "_build_stream_middleware_stack")
