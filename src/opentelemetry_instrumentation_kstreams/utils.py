import json
from logging import getLogger
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Union

from kstreams import Send, Stream, StreamEngine, ConsumerRecord, RecordMetadata
from kstreams.backends.kafka import Kafka
from opentelemetry import propagate, context, trace
from opentelemetry.propagators import textmap
from opentelemetry.semconv.trace import SpanAttributes

# Enable after 0.49 is released
# from opentelemetry.semconv._incubating.attributes import messaging_attributes as SpanAttributes
from opentelemetry.trace import SpanKind, Tracer
from opentelemetry.trace.span import Span
from opentelemetry.context.context import Context

_LOG = getLogger(__name__)


HeadersT = Union[list[tuple[str, Union[bytes, None]]], dict[str, Union[str, None]]]


class KStreamsContextGetter(textmap.Getter[HeadersT]):
    def get(self, carrier: HeadersT, key: str) -> Optional[List[str]]:
        if carrier is None:
            return None
        carrier_items: Iterable = carrier
        if isinstance(carrier, dict):
            carrier_items = carrier.items()

        for item_key, value in carrier_items:
            if item_key == key:
                if value is not None:
                    return [value.decode()]
        return None

    def keys(self, carrier: HeadersT) -> List[str]:
        if carrier is None:
            return []
        carrier_items: Iterable = carrier
        if isinstance(carrier, dict):
            carrier_items = carrier.items()

        return [key for (key, _) in carrier_items]


class KStreamsContextSetter(textmap.Setter[HeadersT]):
    def set(self, carrier: HeadersT, key: str, value: Optional[str]) -> None:
        if carrier is None or key is None:
            return

        if isinstance(carrier, list):
            if value is not None:
                carrier.append((key, value.encode()))
        elif isinstance(carrier, dict):
            if value is not None:
                carrier[key] = value


_kstreams_getter: KStreamsContextGetter = KStreamsContextGetter()
_kstreams_setter: KStreamsContextSetter = KStreamsContextSetter()


class KStreamsKafkaExtractor:
    @staticmethod
    def extract_bootstrap_servers(backend: Kafka) -> List[str]:
        return backend.bootstrap_servers

    @staticmethod
    def _extract_argument(
        key: str, position: int, default_value: Any, args: Any, kwargs: Any
    ) -> Any:
        if len(args) > position:
            return args[position]
        return kwargs.get(key, default_value)

    @staticmethod
    def extract_send_headers(args: Any, kwargs: Any) -> Any:
        """extract headers from `send` method arguments in KafkaProducer class"""
        return KStreamsKafkaExtractor._extract_argument(
            "headers", 3, None, args, kwargs
        )

    @staticmethod
    def extract_send_topic(args, kwargs) -> Any:
        """extract topic from `send` method arguments in KafkaProducer class"""
        return KStreamsKafkaExtractor._extract_argument(
            "topic", 0, "unknown", args, kwargs
        )

    @staticmethod
    def extract_send_partition(record_metadata: Any) -> Optional[int]:
        return getattr(record_metadata, "partition", None)

    @staticmethod
    def extract_send_offset(record_metadata: Any) -> Optional[int]:
        return getattr(record_metadata, "offset", None)

    @staticmethod
    def extract_consumer_group(consumer: Any) -> Optional[str]:
        return getattr(consumer, "group_id", None)

    @staticmethod
    def extract_producer_client_id(instance: StreamEngine) -> Optional[str]:
        if instance._producer is None:
            return None
        return instance._producer.client._client_id

    @staticmethod
    def extract_consumer_client_id(instance: Stream) -> str:
        if instance.consumer is None:
            return ""
        return instance.consumer._client._client_id


def _enrich_base_span(
    span: Span,
    bootstrap_servers: List[str],
    topic: str,
    client_id: Optional[str],
) -> None:
    """
    Enriches the given span with Kafka-related attributes.

    Used to enrich consumer and producer spans.

    Args:
        span: The span to enrich.
        bootstrap_servers: List of Kafka bootstrap servers.
        topic: The Kafka topic name.
        partition: The partition number, if available.
        client_id: The Kafka client ID, if available.
        offset: The message offset, if available. Defaults to None.
    Returns:
        None
    """
    if not span.is_recording():
        return

    span.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "kafka")
    span.set_attribute(SpanAttributes.MESSAGING_DESTINATION, topic)
    span.set_attribute(SpanAttributes.MESSAGING_URL, json.dumps(bootstrap_servers))
    span.set_attribute(SpanAttributes.MESSAGING_DESTINATION_KIND, "topic")

    if client_id is not None:
        span.set_attribute(SpanAttributes.MESSAGING_KAFKA_CLIENT_ID, client_id)


def _enrich_span_with_record_info(
    span: Span, topic: str, partition: Optional[int], offset: Optional[int] = None
) -> None:
    """Used both by consumer and producer spans

    It's in a different function because send needs to be injected with the span
    info, but we want to be able to keep updating the span with the record info

    Args:
        span: The span to enrich.
        topic: The Kafka topic name.
        partition: The partition number, if available.
        offset: The message offset, if available. Defaults to None.
    Returns:
        None
    """
    if offset is not None:
        span.set_attribute(SpanAttributes.MESSAGING_KAFKA_MESSAGE_OFFSET, offset)
    if partition is not None:
        span.set_attribute(SpanAttributes.MESSAGING_KAFKA_PARTITION, partition)
    if offset is not None and partition is not None:
        span.set_attribute(
            SpanAttributes.MESSAGING_MESSAGE_ID,
            f"{topic}.{partition}.{offset}",
        )


def _get_span_name(operation: str, topic: str) -> str:
    return f"{topic} {operation}"


def _wrap_send(tracer: Tracer) -> Callable:
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
            _enrich_base_span(span, bootstrap_servers, topic, client_id)
            propagate.inject(
                headers,
                context=trace.set_span_in_context(span),
                setter=_kstreams_setter,
            )
            record_metadata = await func(*args, **kwargs)

            partition = KStreamsKafkaExtractor.extract_send_partition(record_metadata)
            offset = KStreamsKafkaExtractor.extract_send_offset(record_metadata)
            _enrich_span_with_record_info(span, topic, partition, offset)

        return record_metadata

    return _traced_send


def _create_consumer_span(
    tracer: Tracer,
    record: ConsumerRecord,
    extracted_context: Context,
    bootstrap_servers: List[str],
    client_id: str,
    consumer_group: Optional[str],
    args: Any,
    kwargs: Any,
) -> None:
    """
    Creates and starts a consumer span for a given Kafka record.

    Args:
        tracer: The tracer instance used to create the span.
        record: The Kafka consumer record for which the span is created.
        extracted_context: The context extracted from the incoming message.
        bootstrap_servers: List of bootstrap servers for the Kafka cluster.
        client_id: The client ID of the Kafka consumer.
        args: Additional positional arguments.
        kwargs: Additional keyword arguments.

    Returns:
        None
    """
    span_name = _get_span_name("receive", record.topic)
    with tracer.start_as_current_span(
        span_name,
        context=extracted_context,
        kind=SpanKind.CONSUMER,
    ) as span:
        new_context = trace.set_span_in_context(span, extracted_context)
        token = context.attach(new_context)
        _enrich_base_span(
            span,
            bootstrap_servers,
            record.topic,
            client_id,
        )
        _enrich_span_with_record_info(
            span, record.topic, record.partition, record.offset
        )
        # TODO: enable after 0.49 is released
        # if consumer_group is not None:
        #     span.set_attribute(
        #         SpanAttributes.MESSAGING_CONSUMER_GROUP_NAME, consumer_group
        #     )
        # trace.set_span_in_context(span)
        context.detach(token)


def _wrap_anext(
    tracer: Tracer,
) -> Callable:
    async def _traced_anext(func, instance: Stream, args, kwargs):
        if not isinstance(instance.backend, Kafka):
            raise NotImplementedError("Only Kafka backend is supported for now")
        record: ConsumerRecord = await func(*args, **kwargs)
        bootstrap_servers = KStreamsKafkaExtractor.extract_bootstrap_servers(
            instance.backend
        )
        client_id = KStreamsKafkaExtractor.extract_consumer_client_id(instance)
        # consumer_group = KStreamsKafkaExtractor.extract_consumer_group(
        #     instance.consumer
        # )
        consumer_group = None
        extracted_context: Context = propagate.extract(
            record.headers, getter=_kstreams_getter
        )

        _create_consumer_span(
            tracer,
            record,
            extracted_context,
            bootstrap_servers,
            client_id,
            consumer_group,
            args,
            kwargs,
        )
        # instance._current_context_token = context.attach(
        # )

        return record

    return _traced_anext


_wrap_getone = _wrap_anext
