# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# pylint: disable=unnecessary-dunder-call

import asyncio
from unittest import TestCase, mock

from opentelemetry_instrumentation_kstreams.utils import (
    KStreamsKafkaExtractor,
    _create_consumer_span,
    _get_span_name,
    _kstreams_getter,
    _kstreams_setter,
    _wrap_anext,
    _wrap_send,
)
from opentelemetry.trace import SpanKind
from kstreams.backends.kafka import Kafka


class TestUtils(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.topic_name = "test_topic"
        self.args = [self.topic_name]
        self.headers: list = []
        self.kwargs = {"partition": 0, "headers": self.headers}

    @mock.patch(
        "opentelemetry_instrumentation_kstreams.utils.KStreamsKafkaExtractor.extract_bootstrap_servers"
    )
    @mock.patch(
        "opentelemetry_instrumentation_kstreams.utils.KStreamsKafkaExtractor.extract_send_partition"
    )
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._enrich_span")
    @mock.patch("opentelemetry.trace.set_span_in_context")
    @mock.patch("opentelemetry.propagate.inject")
    def test_wrap_send_with_topic_as_arg(
        self,
        inject: mock.MagicMock,
        set_span_in_context: mock.MagicMock,
        enrich_span: mock.MagicMock,
        extract_send_partition: mock.MagicMock,
        extract_bootstrap_servers: mock.MagicMock,
    ) -> None:
        self.wrap_send_helper(
            inject,
            set_span_in_context,
            enrich_span,
            extract_send_partition,
            extract_bootstrap_servers,
        )

    @mock.patch(
        "opentelemetry_instrumentation_kstreams.utils.KStreamsKafkaExtractor.extract_bootstrap_servers"
    )
    @mock.patch(
        "opentelemetry_instrumentation_kstreams.utils.KStreamsKafkaExtractor.extract_send_partition"
    )
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._enrich_span")
    @mock.patch("opentelemetry.trace.set_span_in_context")
    @mock.patch("opentelemetry.propagate.inject")
    def test_wrap_send_with_topic_as_kwarg(
        self,
        inject: mock.MagicMock,
        set_span_in_context: mock.MagicMock,
        enrich_span: mock.MagicMock,
        extract_send_partition: mock.MagicMock,
        extract_bootstrap_servers: mock.MagicMock,
    ) -> None:
        self.args = []
        self.kwargs["topic"] = self.topic_name
        self.wrap_send_helper(
            inject,
            set_span_in_context,
            enrich_span,
            extract_send_partition,
            extract_bootstrap_servers,
        )

    def wrap_send_helper(
        self,
        inject: mock.MagicMock,
        set_span_in_context: mock.MagicMock,
        enrich_span: mock.MagicMock,
        extract_send_partition: mock.MagicMock,
        extract_bootstrap_servers: mock.MagicMock,
    ) -> None:
        tracer = mock.MagicMock()
        original_send_callback = mock.AsyncMock()
        stream_engine = mock.MagicMock()
        stream_engine.backend = mock.MagicMock(spec_set=Kafka())
        client_id = "client_id"
        stream_engine._producer.client._client_id = client_id
        expected_span_name = _get_span_name("send", self.topic_name)
        wrapped_send = _wrap_send(tracer)
        retval = asyncio.run(
            wrapped_send(original_send_callback, stream_engine, self.args, self.kwargs)
        )

        extract_bootstrap_servers.assert_called_once_with(stream_engine.backend)
        extract_send_partition.assert_called_once_with(self.args, self.kwargs)
        tracer.start_as_current_span.assert_called_once_with(
            expected_span_name, kind=SpanKind.PRODUCER
        )

        span = tracer.start_as_current_span().__enter__.return_value
        enrich_span.assert_called_once_with(
            span,
            extract_bootstrap_servers.return_value,
            self.topic_name,
            extract_send_partition.return_value,
            client_id,
        )

        set_span_in_context.assert_called_once_with(span)
        context = set_span_in_context.return_value
        inject.assert_called_once_with(
            self.headers, context=context, setter=_kstreams_setter
        )

        original_send_callback.assert_called_once_with(*self.args, **self.kwargs)
        self.assertEqual(retval, original_send_callback.return_value)

    @mock.patch("opentelemetry.propagate.extract")
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._create_consumer_span")
    @mock.patch(
        "opentelemetry_instrumentation_kstreams.utils.KStreamsKafkaExtractor.extract_bootstrap_servers"
    )
    async def test_wrap_next(
        self,
        extract_bootstrap_servers: mock.MagicMock,
        _create_consumer_span: mock.MagicMock,
        extract: mock.MagicMock,
    ) -> None:
        tracer = mock.MagicMock()
        original_next_callback = mock.MagicMock()
        stream = mock.MagicMock()
        stream.backend = mock.MagicMock(spec_set=Kafka())

        wrapped_next = _wrap_anext(tracer)
        record = await wrapped_next(
            original_next_callback, stream, self.args, self.kwargs
        )

        extract_bootstrap_servers.assert_called_once_with(stream.backend)
        bootstrap_servers = extract_bootstrap_servers.return_value

        original_next_callback.assert_called_once_with(*self.args, **self.kwargs)
        self.assertEqual(record, original_next_callback.return_value)

        extract.assert_called_once_with(record.headers, getter=_kstreams_getter)
        context = extract.return_value

        _create_consumer_span.assert_called_once_with(
            tracer,
            record,
            context,
            bootstrap_servers,
            self.args,
            self.kwargs,
        )

    @mock.patch("opentelemetry.trace.set_span_in_context")
    @mock.patch("opentelemetry.context.attach")
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._enrich_span")
    @mock.patch("opentelemetry.context.detach")
    def test_create_consumer_span(
        self,
        detach: mock.MagicMock,
        enrich_span: mock.MagicMock,
        attach: mock.MagicMock,
        set_span_in_context: mock.MagicMock,
    ) -> None:
        tracer = mock.MagicMock()
        # consume_hook = mock.MagicMock()
        bootstrap_servers = mock.MagicMock()
        extracted_context = mock.MagicMock()
        record = mock.MagicMock()
        client_id = mock.MagicMock()

        _create_consumer_span(
            tracer,
            record,
            extracted_context,
            bootstrap_servers,
            client_id,
            self.args,
            self.kwargs,
        )

        expected_span_name = _get_span_name("receive", record.topic)

        tracer.start_as_current_span.assert_called_once_with(
            expected_span_name,
            context=extracted_context,
            kind=SpanKind.CONSUMER,
        )
        span = tracer.start_as_current_span.return_value.__enter__()
        set_span_in_context.assert_called_once_with(span, extracted_context)
        attach.assert_called_once_with(set_span_in_context.return_value)

        enrich_span.assert_called_once_with(
            span,
            bootstrap_servers,
            record.topic,
            record.partition,
            client_id,
            record.offset,
        )
        # consume_hook.assert_called_once_with(span, record, self.args, self.kwargs)
        detach.assert_called_once_with(attach.return_value)

    # @mock.patch("opentelemetry_instrumentation_kstreams.utils.KStreamsKafkaExtractor")
    def test_kafka_properties_extractor(
        self,
        # kafka_properties_extractor: mock.MagicMock,
    ):
        assert (
            KStreamsKafkaExtractor.extract_send_partition(self.args, self.kwargs) == 0
        )
