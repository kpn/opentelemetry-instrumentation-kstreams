from typing import Awaitable, Callable

from kstreams import (
    RecordMetadata,
    Send,
    Stream,
    StreamEngine,
    middleware,
)
from kstreams.backends.kafka import Kafka
from kstreams.types import NextMiddlewareCall
from opentelemetry import propagate, trace

# Enable after 0.49 is released
# from opentelemetry.semconv._incubating.attributes import messaging_attributes as SpanAttributes
from opentelemetry.trace import SpanKind, Tracer

from . import utils
from .middlewares import OpenTelemetryMiddleware
from .utils import (
    KStreamsKafkaExtractor,
    _get_span_name,
    _kstreams_setter,
)


def _wrap_send(tracer: Tracer) -> Callable:
    """
    Wraps the send function of a Kafka producer with tracing capabilities.

    Args:
        tracer: The OpenTelemetry tracer used to create spans.

    Returns:
        Callable: An asynchronous function that wraps the original send function,
                  adding tracing information to the Kafka message headers and
                  enriching the span with metadata about the message.

    Raises:
        NotImplementedError: If the backend of the instance is not Kafka.

    The wrapped function performs the following steps:
        1. Checks if the backend of the instance is Kafka.
        2. Extracts or initializes the headers for the Kafka message.
        3. Extracts the client ID, bootstrap servers, and topic from the instance.
        4. Creates a span with the name "send" and the topic.
        5. Enriches the span with base information such as bootstrap servers, topic, and client ID.
        6. Injects the tracing context into the message headers.
        7. Calls the original send function and awaits its result.
        8. Extracts partition and offset information from the result.
        9. Enriches the span with record metadata such as partition and offset.
    """

    async def _traced_send(
        func: Send, instance: StreamEngine, args, kwargs
    ) -> Awaitable[RecordMetadata]:
        if not isinstance(instance.backend, Kafka):
            raise NotImplementedError("Only Kafka backend is supported for now")

        headers = KStreamsKafkaExtractor.extract_send_headers(args, kwargs)

        if headers is None:
            headers = []
            kwargs["headers"] = headers
        client_id = KStreamsKafkaExtractor.extract_producer_client_id(instance)
        bootstrap_servers = KStreamsKafkaExtractor.extract_bootstrap_servers(
            instance.backend
        )
        topic = KStreamsKafkaExtractor.extract_send_topic(args, kwargs)

        span_name = _get_span_name("send", topic)
        with tracer.start_as_current_span(span_name, kind=SpanKind.PRODUCER) as span:
            utils._enrich_base_span(span, bootstrap_servers, topic, client_id)
            propagate.inject(
                headers,
                context=trace.set_span_in_context(span),
                setter=_kstreams_setter,
            )
            record_metadata = await func(*args, **kwargs)

            partition = KStreamsKafkaExtractor.extract_send_partition(record_metadata)
            offset = KStreamsKafkaExtractor.extract_send_offset(record_metadata)
            utils._enrich_span_with_record_info(span, topic, partition, offset)

        return record_metadata

    return _traced_send


def _wrap_build_stream_middleware_stack(
    tracer: Tracer,
) -> Callable:
    def _traced_build_stream_middleware_stack(
        func, instance: StreamEngine, args, kwargs
    ) -> NextMiddlewareCall:
        # this should fail if stream is not in kwargs
        # so we don't catch the exception
        stream: Stream = kwargs["stream"]

        stream.middlewares.insert(
            0, middleware.Middleware(OpenTelemetryMiddleware, tracer=tracer)
        )
        next_call = func(*args, **kwargs)

        return next_call

    return _traced_build_stream_middleware_stack


def _wrap_get_middlewares(
    tracer: Tracer,
) -> Callable:
    def _traced_get_middlewares(
        func, instance: Stream, args, kwargs
    ) -> NextMiddlewareCall:
        # let's check if otel is already present in the middlewares
        if (
            len(instance.middlewares) > 0
            and instance.middlewares[0].middleware == OpenTelemetryMiddleware
        ):
            return func(*args, **kwargs)

        instance.middlewares.insert(
            0, middleware.Middleware(OpenTelemetryMiddleware, tracer=tracer)
        )

        next_call = func(*args, **kwargs)

        return next_call

    return _traced_get_middlewares
