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

from kstreams import ConsumerRecord, StreamEngine
from kstreams.backends.kafka import Kafka
from kstreams.middleware import BaseMiddleware, ExceptionMiddleware, Middleware
from kstreams.streams_utils import StreamErrorPolicy
from opentelemetry.trace import SpanKind

from opentelemetry_instrumentation_kstreams.instrumentor import KStreamsInstrumentor
from opentelemetry_instrumentation_kstreams.middlewares import OpenTelemetryMiddleware
from opentelemetry_instrumentation_kstreams.utils import (
    _get_span_name,
    _kstreams_getter,
    _kstreams_setter,
)
from opentelemetry_instrumentation_kstreams.wrappers import (
    _wrap_build_stream_middleware_stack,
    _wrap_send,
)


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
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._enrich_base_span")
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
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._enrich_base_span")
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
        record = mock.MagicMock()
        original_send_callback = mock.AsyncMock()
        original_send_callback.return_value = record
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
        extract_send_partition.assert_called_once_with(record)
        tracer.start_as_current_span.assert_called_once_with(
            expected_span_name, kind=SpanKind.PRODUCER
        )

        span = tracer.start_as_current_span().__enter__.return_value
        enrich_span.assert_called_once_with(
            span,
            extract_bootstrap_servers.return_value,
            self.topic_name,
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
    @mock.patch("opentelemetry.trace.set_span_in_context")
    @mock.patch("opentelemetry.context.attach")
    @mock.patch("opentelemetry_instrumentation_kstreams.utils._enrich_base_span")
    @mock.patch(
        "opentelemetry_instrumentation_kstreams.utils._enrich_span_with_record_info"
    )
    @mock.patch("opentelemetry.context.detach")
    def test_opentelemetry_middleware(
        self,
        detach: mock.MagicMock,
        enrich_span_with_record_info: mock.MagicMock,
        enrich_base_span: mock.MagicMock,
        attach: mock.MagicMock,
        set_span_in_context: mock.MagicMock,
        extract: mock.MagicMock,
    ) -> None:
        async def func(cr): ...

        tracer = mock.MagicMock()

        next_call = func
        send = mock.MagicMock()
        stream = mock.MagicMock()
        stream.backend = Kafka()
        stream.consumer._client._client_id = "client_id"
        stream.consumer.group_id = "consumer_group"

        record = mock.MagicMock()
        middleware = OpenTelemetryMiddleware(
            next_call=next_call, send=send, stream=stream, tracer=tracer
        )

        assert middleware.bootstrap_servers == stream.backend.bootstrap_servers
        assert middleware.client_id == "client_id"
        assert middleware.consumer_group == "consumer_group"

        asyncio.run(middleware(record))

        extract.assert_called_once_with(record.headers, getter=_kstreams_getter)
        tracer.start_as_current_span.assert_called_once()
        attach.assert_called_once_with(set_span_in_context.return_value)
        detach.assert_called_once_with(attach.return_value)
        span = tracer.start_as_current_span.return_value.__enter__()

        enrich_base_span.assert_called_once_with(
            span,
            stream.backend.bootstrap_servers,
            record.topic,
            "client_id",
        )
        enrich_span_with_record_info.assert_called_once_with(
            span,
            record.topic,
            record.partition,
            record.offset,
        )

    def test_build_stream_middleware_stack_receives_stream(self):
        tracer = mock.MagicMock()
        middlewares = []

        func = mock.MagicMock()
        instance = mock.MagicMock()
        args = mock.MagicMock()
        stream = mock.MagicMock()
        stream.middlewares = middlewares
        kwargs = {"stream": stream}

        _wrap_build_stream_middleware_stack(tracer)(func, instance, args, kwargs)

        first_middleware_class = stream.middlewares[0].middleware
        assert first_middleware_class == OpenTelemetryMiddleware

    def test_correct_stack_build(self):
        KStreamsInstrumentor().instrument()

        class S3Middleware(BaseMiddleware):
            async def __call__(self, cr: ConsumerRecord):
                print("Dummy backup to s3")
                return await self.next_call(cr)

        consumer_class = mock.MagicMock()
        producer_class = mock.MagicMock()
        monitor = mock.MagicMock()

        # Create the stream with an extra middleware
        stream = mock.MagicMock()
        stream.middlewares = [Middleware(S3Middleware)]

        backend = Kafka()
        stream_engine = StreamEngine(
            title="test stream",
            backend=backend,
            consumer_class=consumer_class,
            producer_class=producer_class,
            monitor=monitor,
        )
        stream_engine.add_stream(stream)
        stream_engine.start()

        # Build the middleware stack
        stream_engine.build_stream_middleware_stack(
            stream=stream, error_policy=StreamErrorPolicy.STOP_ENGINE
        )
        stream_engine.stop()

        assert len(stream.middlewares) == 3

        # In this case, we simulated the real workflow using the stream_engine
        # so the first should be the ExceptionMiddleware
        first_middleware_class = stream.middlewares[0].middleware
        assert first_middleware_class == ExceptionMiddleware

        # The second should be the OpenTelemetryMiddleware
        second_middleware_class = stream.middlewares[1].middleware
        assert second_middleware_class == OpenTelemetryMiddleware

        # The third should be the S3Middleware
        third_middleware_class = stream.middlewares[2].middleware
        assert third_middleware_class == S3Middleware
