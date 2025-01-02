package lambda

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/awslabs/aws-lambda-go-api-proxy/httpadapter"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/service"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/tasksv1connect"
	lambdadetector "go.opentelemetry.io/contrib/detectors/aws/lambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-lambda-go/otellambda/xrayconfig"
	"go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"google.golang.org/grpc"
)

func Run(ctx context.Context) error {
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to load aws config: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithDetectors(lambdadetector.NewResourceDetector()),
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(
			semconv.ServiceName("tasks"),
			semconv.ServiceVersion("v0.0.0"),            // TODO: Use a real version.
			semconv.DeploymentEnvironment("production"), // TODO: Use a real environment.
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	traceExporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint("0.0.0.0:4317"),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	)
	if err != nil {
		return fmt.Errorf("failed to create trace exporter: %w", err)
	}

	logExporter, err := stdoutlog.New()
	if err != nil {
		return fmt.Errorf("failed to create log exporter: %w", err)
	}

	otelConfig := &service.OTELConfig{
		Propagator: xray.Propagator{},
		TraceProvider: trace.NewTracerProvider(
			trace.WithResource(res),
			trace.WithSampler(trace.AlwaysSample()),
			trace.WithBatcher(traceExporter),
			trace.WithIDGenerator(xray.NewIDGenerator()),
		),
		LoggerProvider: log.NewLoggerProvider(
			log.WithResource(res),
			log.WithProcessor(
				log.NewBatchProcessor(logExporter),
			),
		),
		MeterProvider: metric.NewMeterProvider(
			metric.WithResource(res),
		),
	}

	srv, err := service.NewServer(
		ctx,
		awsConfig,
		otelConfig,
	)
	defer srv.Shutdown(ctx)

	otelInterceptor, err := otelconnect.NewInterceptor()
	if err != nil {
		return fmt.Errorf("failed to create otel interceptor: %w", err)
	}

	validateInterceptor, err := validate.NewInterceptor()
	if err != nil {
		return fmt.Errorf("failed to create validate interceptor: %w", err)
	}

	mux := http.NewServeMux()

	mux.Handle(
		tasksv1connect.NewTasksServiceHandler(
			srv,
			connect.WithInterceptors(
				otelInterceptor,
				validateInterceptor,
			),
		),
	)

	lambda.StartWithOptions(
		otellambda.InstrumentHandler(
			httpadapter.New(mux).Proxy,
			xrayconfig.WithRecommendedOptions(otelConfig.TraceProvider)...,
		),
		lambda.WithContext(ctx),
	)

	return nil
}
