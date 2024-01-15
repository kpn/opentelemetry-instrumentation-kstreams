# opentelemetry-instrumentation-kstreams

> OTEL for kstreams

Version: `0.1.1`

## Installation

```sh
pip install -U opentelemetry_instrumentation_kstreams
```

## Usage

```python
from opentelemetry_instrumentation_kstreams import KStreamsInstrumentor

KStreamsInstrumentor().instrument()
```

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

## Release

The release process is automated by the CI, if you have to do it manually then:

```sh
./scripts/install
./scripts/bump
./scripts/release
```

Note: this will not release the docs.
