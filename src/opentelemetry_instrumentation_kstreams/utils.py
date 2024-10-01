import json
from logging import getLogger
from typing import Any, Iterable, List, Optional, Union

from kstreams import (
    Stream,
    StreamEngine,
)
from kstreams.backends.kafka import Kafka
from opentelemetry.propagators import textmap
from opentelemetry.semconv.trace import SpanAttributes

# Enable after 0.49 is released
# from opentelemetry.semconv._incubating.attributes import messaging_attributes as SpanAttributes
from opentelemetry.trace.span import Span

_LOG = getLogger(__name__)


HeadersT = Union[list[tuple[str, Union[bytes, None]]], dict[str, Union[str, None]]]


class KStreamsContextGetter(textmap.Getter[HeadersT]):
    """
    KStreamsContextGetter is a custom implementation of the textmap.Getter interface
    for extracting context from Kafka Streams headers.

    Methods:
        get(carrier: HeadersT, key: str) -> Optional[List[str]]:
            Extracts the value associated with the given key from the carrier.
            If the carrier is None or the key is not found, returns None.
            If the value is found and is not None, it decodes the value and returns it as a list of strings.

        keys(carrier: HeadersT) -> List[str]:
            Returns a list of all keys present in the carrier.
            If the carrier is None, returns an empty list.
    """

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
    """
    A context setter for KStreams that implements the textmap.Setter interface.

    Methods:
        set(carrier: HeadersT, key: str, value: Optional[str]) -> None
            Sets a key-value pair in the carrier. If the carrier is a list, the key-value pair is appended.
            If the carrier is a dictionary, the key-value pair is added or updated.

            Parameters:
                carrier : HeadersT
                    The carrier to set the key-value pair in. Can be a list or a dictionary.
                key : str
                    The key to set in the carrier.
                value : Optional[str]
                    The value to set for the key in the carrier. If None, the key-value pair is not set.
    """

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
