"""
quorum/observability/otel_setup.py
------------------------------------
OpenTelemetry tracing bootstrap for the Quorum service.

SEPARATE from the ring_buffer path — no shared state, no cross-imports.
Configures a TracerProvider backed by an OTLP gRPC exporter and exposes
helpers used by the rest of the codebase.

Typical usage::

    # In your application entrypoint (e.g. main.py or api/app.py):
    from quorum.observability.otel_setup import setup_otel
    setup_otel(service_name="quorum")

    # In any module that needs a tracer:
    from quorum.observability.otel_setup import get_tracer
    tracer = get_tracer()
    with tracer.start_as_current_span("my-operation") as span:
        ...

    # Tombstone a span (mark as superseded / dead) by span metadata:
    from quorum.observability.otel_setup import tombstone_span
    tombstone_span(span_id="abc123", run_id="run-42")
"""

import logging
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from config.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level state — intentionally hidden behind accessors so callers never
# depend on the concrete provider type.
# ---------------------------------------------------------------------------

_provider: Optional[TracerProvider] = None
_tracer: Optional[trace.Tracer] = None

# Instrument name used across all Quorum spans.
_INSTRUMENTATION_NAME = "quorum"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def setup_otel(service_name: str = "quorum") -> trace.Tracer:
    """Initialise the OpenTelemetry TracerProvider and return a root Tracer.

    Creates a :class:`~opentelemetry.sdk.trace.TracerProvider` with:
    * A ``service.name`` resource attribute set to *service_name*.
    * A :class:`~opentelemetry.sdk.trace.export.BatchSpanProcessor` backed by
      an OTLP/gRPC exporter pointed at
      :attr:`~config.settings.Settings.OTEL_EXPORTER_OTLP_ENDPOINT`.

    The provider is registered globally via
    :func:`opentelemetry.trace.set_tracer_provider` so that any third-party
    library instrumented with OTel will automatically use it.

    Calling this function more than once is a no-op — the existing provider and
    tracer are returned unchanged.

    Args:
        service_name: Value for the ``service.name`` OTel resource attribute.
                      Defaults to ``"quorum"``.

    Returns:
        A :class:`~opentelemetry.trace.Tracer` instance bound to
        *service_name*.
    """
    global _provider, _tracer

    if _tracer is not None:
        logger.debug("OTel already initialised — skipping setup.")
        return _tracer

    resource = Resource(attributes={SERVICE_NAME: service_name})

    exporter = OTLPSpanExporter(
        endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT,
        insecure=True,  # TLS termination happens at the collector proxy layer
    )

    _provider = TracerProvider(resource=resource)
    _provider.add_span_processor(BatchSpanProcessor(exporter))

    # Register globally so third-party auto-instrumentation picks it up.
    trace.set_tracer_provider(_provider)

    _tracer = _provider.get_tracer(_INSTRUMENTATION_NAME)

    logger.info(
        "OTel tracing initialised. service=%s endpoint=%s",
        service_name,
        settings.OTEL_EXPORTER_OTLP_ENDPOINT,
    )
    return _tracer


def get_tracer() -> trace.Tracer:
    """Return the process-global Tracer, initialising OTel if necessary.

    This is the primary accessor used by all Quorum modules that need to create
    spans.  It is safe to call from any thread or asyncio coroutine.

    Returns:
        The :class:`~opentelemetry.trace.Tracer` configured by
        :func:`setup_otel`.  If :func:`setup_otel` has not been called yet
        this function calls it with the default ``service_name="quorum"``.
    """
    global _tracer
    if _tracer is None:
        setup_otel()
    return _tracer  # type: ignore[return-value]


def tombstone_span(span_id: str, run_id: str) -> None:
    """Emit a zero-duration *tombstone* span to signal that a logical unit of
    work has been superseded, abandoned, or declared dead.

    The tombstone does **not** cancel in-flight work — it is purely a signal in
    the trace backend (e.g. Jaeger / Tempo) that a reaper or adjudicator has
    flagged the referenced span.

    The emitted span carries the following attributes:

    * ``quorum.tombstone = True``
    * ``quorum.tombstoned_span_id`` — the span ID being flagged.
    * ``quorum.run_id`` — the run context in which the tombstone was created.

    Args:
        span_id: The OTel span ID string of the span being tombstoned.  This is
                 stored as a string attribute; OTel does not support direct
                 span cross-references.
        run_id:  The Quorum run ID associated with the dead agent or abandoned
                 task.  Stored as ``quorum.run_id``.

    Example::

        tombstone_span(span_id="abc123def456", run_id="run-2024-001")
    """
    tracer = get_tracer()
    with tracer.start_as_current_span("quorum.tombstone") as span:
        span.set_attribute("quorum.tombstone", True)
        span.set_attribute("quorum.tombstoned_span_id", span_id)
        span.set_attribute("quorum.run_id", run_id)

    logger.debug(
        "Tombstone span emitted. tombstoned_span_id=%s run_id=%s",
        span_id,
        run_id,
    )
