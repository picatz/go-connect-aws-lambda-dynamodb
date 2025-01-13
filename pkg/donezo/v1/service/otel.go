package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
)

type OTELConfig struct {
	ServiceName string

	ResourceAttributes []attribute.KeyValue

	Propagator propagation.TextMapPropagator

	TraceProvider *trace.TracerProvider

	MeterProvider *metric.MeterProvider

	LoggerProvider *log.LoggerProvider
}

func (cfg *OTELConfig) GetServiceName() string {
	return cfg.ServiceName
}

func (cfg *OTELConfig) GetResourceAttributes() []attribute.KeyValue {
	return cfg.ResourceAttributes
}

func (cfg *OTELConfig) GetPropagator() propagation.TextMapPropagator {
	if cfg.Propagator == nil {
		return newPropagator()
	}
	return cfg.Propagator
}

func (cfg *OTELConfig) GetTraceProvider() (*trace.TracerProvider, error) {
	if cfg.TraceProvider == nil {
		return newTraceProvider()
	}
	return cfg.TraceProvider, nil
}

func (cfg *OTELConfig) GetMeterProvider() (*metric.MeterProvider, error) {
	if cfg.MeterProvider == nil {
		return newMeterProvider()
	}
	return cfg.MeterProvider, nil
}

func (cfg *OTELConfig) GetLoggerProvider() (*log.LoggerProvider, error) {
	if cfg.LoggerProvider == nil {
		return newLoggerProvider()
	}
	return cfg.LoggerProvider, nil
}

// setupOpenTelemetry bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOpenTelemetry(ctx context.Context, cfg *OTELConfig) (shutdown func(context.Context) error, err error) {
	if cfg == nil {
		cfg = &OTELConfig{}
	}

	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := cfg.GetPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := cfg.GetTraceProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// Set up meter provider.
	meterProvider, err := cfg.GetMeterProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	// Set up logger provider.
	loggerProvider, err := cfg.GetLoggerProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider() (*trace.TracerProvider, error) {
	traceExporter, err := stdouttrace.New(
		stdouttrace.WithWriter(os.Stderr),
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(
			traceExporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			trace.WithBatchTimeout(time.Second),
		),
	)
	return traceProvider, nil
}

func newMeterProvider() (*metric.MeterProvider, error) {
	enc := json.NewEncoder(os.Stderr)
	// enc.SetIndent("", "  ")

	metricExporter, err := stdoutmetric.New(
		stdoutmetric.WithEncoder(enc),
		// stdoutmetric.WithoutTimestamps(),
	)
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(
			metric.NewPeriodicReader(
				metricExporter,
				// Default is 1m. Set to 3s for demonstrative purposes.
				metric.WithInterval(3*time.Second),
			),
		),
	)
	return meterProvider, nil
}

func newLoggerProvider() (*log.LoggerProvider, error) {
	logExporter, err := stdoutlog.New()
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(
			log.NewBatchProcessor(logExporter),
		),
	)
	return loggerProvider, nil
}
