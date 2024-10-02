from typing import Any, Optional

from kstreams import (
    ConsumerRecord,
    middleware,
)
from kstreams.backends.kafka import Kafka
from opentelemetry import context, propagate, trace
from opentelemetry.context.context import Context

# Enable after 0.49 is released
# from opentelemetry.semconv._incubating.attributes import messaging_attributes as SpanAttributes
from opentelemetry.trace import SpanKind, Tracer

from . import utils
from .utils import (
    KStreamsKafkaExtractor,
    _get_span_name,
    _kstreams_getter,
)


class OpenTelemetryMiddleware(middleware.BaseMiddleware):
    """
    Middleware for integrating OpenTelemetry tracing with Kafka Streams.

    This middleware extracts tracing information from Kafka consumer records and
    creates spans for tracing the processing of these records.

    Attributes:
        tracer: The OpenTelemetry tracer instance used for creating spans.

    Methods:
        __call__(cr: ConsumerRecord) -> Any:
            Asynchronously processes a Kafka consumer record, creating and enriching
            an OpenTelemetry span with tracing information.
    """

    def __init__(self, *, tracer: Optional[Tracer] = None, **kwargs) -> None:
        super().__init__(**kwargs)
        if tracer is None:
            tracer = trace.get_tracer(__name__)

        # The current tracer instance
        self.tracer = tracer

        # Initialize variables computed once which are injected into the span
        if not isinstance(self.stream.backend, Kafka):
            raise NotImplementedError("Only Kafka backend is supported for now")
        self.bootstrap_servers = KStreamsKafkaExtractor.extract_bootstrap_servers(
            self.stream.backend
        )
        self.consumer_group = KStreamsKafkaExtractor.extract_consumer_group(
            self.stream.consumer
        )
        self.client_id = KStreamsKafkaExtractor.extract_consumer_client_id(self.stream)

    async def __call__(self, cr: ConsumerRecord) -> Any:
        """
        Asynchronously processes a ConsumerRecord by creating and managing a span.

        Args:
            cr (ConsumerRecord): The consumer record to be processed.

        Returns:
            Any: The result of the next call in the processing chain.

        This method performs the following steps:
        1. Extracts the context from the record headers.
        2. Starts a new span with the extracted context.
        3. Enriches the span with base and record-specific information.
        4. Optionally sets the consumer group attribute (currently commented out).
        5. Calls the next processing function in the chain.
        6. Detaches the context token.
        """
        tracer = self.tracer
        record = cr
        bootstrap_servers = self.bootstrap_servers
        client_id = self.client_id
        span_name = _get_span_name("receive", record.topic)
        extracted_context: Context = propagate.extract(
            record.headers, getter=_kstreams_getter
        )

        with tracer.start_as_current_span(
            span_name,
            context=extracted_context,
            end_on_exit=True,
            kind=SpanKind.CONSUMER,
        ) as span:
            new_context = trace.set_span_in_context(span, extracted_context)
            context_token = context.attach(new_context)

            utils._enrich_base_span(
                span,
                bootstrap_servers,
                record.topic,
                client_id,
            )
            utils._enrich_span_with_record_info(
                span, record.topic, record.partition, record.offset
            )

            # TODO: enable after 0.49 is released
            # if self.consumer_group is not None:
            #     span.set_attribute(
            #         SpanAttributes.MESSAGING_CONSUMER_GROUP_NAME, self.consumer_group
            #     )

            await self.next_call(cr)
            context.detach(context_token)
