// Package telemetry wires up OpenTelemetry tracing. Opt-in: if
// config.OTel.Enabled is false, Init returns a no-op tracer and a no-op
// shutdown function, so callers don't need to branch.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/khodaparastan/s5lb/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Providers is returned by Init and wraps the tracer plus a shutdown func.
type Providers struct {
	Tracer   trace.Tracer
	Shutdown func(ctx context.Context) error
}

// Init constructs an OTel tracer per config. When disabled, returns a no-op
// tracer and a no-op shutdown.
func Init(ctx context.Context, cfg config.OTelConfig, version, commit string) (Providers, error) {
	if !cfg.Enabled {
		return Providers{
			Tracer:   noop.NewTracerProvider().Tracer("s5lb"),
			Shutdown: func(context.Context) error { return nil },
		}, nil
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}
	// Reasonable default timeout so the process doesn't stall on export.
	opts = append(opts, otlptracegrpc.WithTimeout(10*time.Second))

	client := otlptracegrpc.NewClient(opts...)
	exp, err := otlptrace.New(ctx, client)
	if err != nil {
		return Providers{}, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(version),
			attribute.String("git.commit", commit),
		),
	)
	if err != nil {
		return Providers{}, fmt.Errorf("otel resource: %w", err)
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return Providers{
		Tracer:   tp.Tracer("s5lb"),
		Shutdown: tp.Shutdown,
	}, nil
}
