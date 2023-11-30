import json
from logging import getLogger
from typing import Any, Callable, List, Optional

from kstreams import Stream, StreamEngine, ConsumerRecord
from kstreams.backends.kafka import Kafka
from opentelemetry import propagate, context, trace
from opentelemetry.propagators import textmap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, Tracer
from opentelemetry.trace.span import Span
from opentelemetry.context.context import Context

_LOG = getLogger(__name__)


class KStreamsContextGetter(textmap.Getter[textmap.CarrierT]):
    def get(self, carrier: textmap.CarrierT, key: str) -> Optional[List[str]]:
        if carrier is None:
            return None

        carrier_items = carrier
        if isinstance(carrier, dict):
            carrier_items = carrier.items()  # type: ignore

        for item_key, value in carrier_items:  # type: ignore
            if item_key == key:
                if value is not None:
                    return [value.decode()]
        return None

    def keys(self, carrier: textmap.CarrierT) -> List[str]:
        if carrier is None:
            return []
        carrier_items = carrier
        if isinstance(carrier, dict):
            carrier_items = carrier.items()  # type: ignore

        return [key for (key, _) in carrier_items]  # type: ignore


class KStreamsContextSetter(textmap.Setter[textmap.CarrierT]):
    def set(self, carrier: textmap.CarrierT, key: str, value: str) -> None:
        if carrier is None or key is None:
            return

        if isinstance(carrier, list):
            carrier.append((key, value))  # type: ignore

        if isinstance(carrier, dict):
            carrier[key] = value  # type: ignore


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
    def extract_send_partition(args: Any, kwargs: Any) -> Any:
        return KStreamsKafkaExtractor._extract_argument(
            "partition", 4, None, args, kwargs
        )

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


def _enrich_span(
    span: Span,
    bootstrap_servers: List[str],
    topic: str,
    partition: Optional[int],
    client_id: Optional[str],
):
    span.set_attribute(SpanAttributes.MESSAGING_SYSTEM, "kafka")
    span.set_attribute(SpanAttributes.MESSAGING_DESTINATION, topic)
    span.set_attribute(SpanAttributes.MESSAGING_URL, json.dumps(bootstrap_servers))
    span.set_attribute(SpanAttributes.MESSAGING_DESTINATION_KIND, "topic")
    if client_id is not None:
        span.set_attribute(SpanAttributes.MESSAGING_KAFKA_CLIENT_ID, client_id)

    if span.is_recording():
        if partition is not None:
            span.set_attribute(SpanAttributes.MESSAGING_KAFKA_PARTITION, partition)


def _get_span_name(operation: str, topic: str):
    return f"{topic} {operation}"


def _wrap_send(tracer: Tracer) -> Callable:
    async def _traced_send(func, instance: StreamEngine, args, kwargs):
        if not isinstance(instance.backend, Kafka):
            raise NotImplementedError("Only Kafka backend is supported for now")

        headers = KStreamsKafkaExtractor.extract_send_headers(args, kwargs)

        if headers is None:
            headers = []
            kwargs["headers"] = headers.copy()

        topic = KStreamsKafkaExtractor.extract_send_topic(args, kwargs)
        bootstrap_servers = KStreamsKafkaExtractor.extract_bootstrap_servers(
            instance.backend
        )
        partition = KStreamsKafkaExtractor.extract_send_partition(args, kwargs)
        client_id = KStreamsKafkaExtractor.extract_producer_client_id(instance)

        span_name = _get_span_name("send", topic)
        with tracer.start_as_current_span(span_name, kind=SpanKind.PRODUCER) as span:
            _enrich_span(span, bootstrap_servers, topic, partition, client_id)
            propagate.inject(
                headers,
                context=trace.set_span_in_context(span),
                setter=_kstreams_setter,
            )

        return await func(*args, **kwargs)

    return _traced_send


def _create_consumer_span(
    tracer: Tracer,
    record: ConsumerRecord,
    extracted_context: Context,
    bootstrap_servers: List[str],
    client_id: str,
    args: Any,
    kwargs: Any,
) -> None:
    span_name = _get_span_name("receive", record.topic)
    with tracer.start_as_current_span(
        span_name,
        context=extracted_context,
        kind=SpanKind.CONSUMER,
    ) as span:
        new_context = trace.set_span_in_context(span, extracted_context)
        token = context.attach(new_context)
        _enrich_span(span, bootstrap_servers, record.topic, record.partition, client_id)

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
        extracted_context: Context = propagate.extract(
            record.headers, getter=_kstreams_getter
        )

        _create_consumer_span(
            tracer,
            record,
            extracted_context,
            bootstrap_servers,
            client_id,
            args,
            kwargs,
        )

        return record

    return _traced_anext


_wrap_getone = _wrap_anext
