package service

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/donezov1connect"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Table is the name of the DynamoDB table used to store tasks.
var Table = aws.String("donezo")

// Server implements all Donezo services.
type Server struct {
	logger   *slog.Logger
	tracer   trace.Tracer
	db       *dynamodb.Client
	cfg      *OTELConfig
	shutdown func(context.Context) error
	_        donezov1connect.UnimplementedTasksServiceHandler
	_        donezov1connect.UnimplementedOrganizationsServiceHandler
	_        donezov1connect.UnimplementedUsersServiceHandler
	_        donezov1connect.UnimplementedProjectsServiceHandler
}

// NewServer initializes a new server instance with the provided AWS and OpenTelemetry configurations.
// It sets up OpenTelemetry for observability (tracing and logging), configures AWS SDK with OpenTelemetry
// middlewares, and returns a server instance ready to handle requests.
func NewServer(
	ctx context.Context,
	awsConfig aws.Config,
	otelConfig *OTELConfig,
) (*Server, error) {
	shutdown, err := setupOpenTelemetry(ctx, otelConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to setup OpenTelemetry SDK: %w", err)
	}

	serviceName := otelConfig.GetServiceName()

	tracerProvider, err := otelConfig.GetTraceProvider()
	if err != nil {
		return nil, fmt.Errorf("failed to get trace provider: %w", err)
	}

	otelaws.AppendMiddlewares(
		&awsConfig.APIOptions,
		otelaws.WithTracerProvider(tracerProvider),
		otelaws.WithTextMapPropagator(otelConfig.GetPropagator()),
	)

	loggingProvider, err := otelConfig.GetLoggerProvider()
	if err != nil {
		return nil, fmt.Errorf("failed to get logger provider: %w", err)
	}

	// metricsProvider, err := cfg.GetMeterProvider()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to get meter provider: %w", err)
	// }

	return &Server{
		logger: slog.New(
			otelslog.NewHandler(
				serviceName,
				otelslog.WithLoggerProvider(loggingProvider),
			),
		),
		tracer:   tracerProvider.Tracer(serviceName),
		db:       dynamodb.NewFromConfig(awsConfig),
		cfg:      otelConfig,
		shutdown: shutdown,
	}, nil
}

// errorf logs an internal error, and records it in the trace span,
// and returns an error with a user-friendly message. This is useful
// to prevent leaking internal error details to the client that might
// be present in the raw underlying error, while still providing enough
// information for debugging to both users and operators appropriately.
func (s *Server) errorf(ctx context.Context, c connect.Code, underlying error, userMessageFormat string, a ...interface{}) error {
	userMessage := fmt.Sprintf(userMessageFormat, a...)

	span := trace.SpanFromContext(ctx)

	span.RecordError(
		underlying,
		trace.WithStackTrace(true),
		trace.WithAttributes(attribute.KeyValue{
			Key:   attribute.Key("user_message"),
			Value: attribute.StringValue(userMessage),
		}),
	)

	s.logger.With(
		"trace_id", span.SpanContext().TraceID(),
		"span_id", span.SpanContext().SpanID(),
	).ErrorContext(
		ctx,
		underlying.Error(),
		"user_message", userMessage,
	)

	return connect.NewError(c, fmt.Errorf("%s: %s: %s", span.SpanContext().TraceID(), span.SpanContext().SpanID(), userMessage))
}

// Shutdown shutsdown the OpenTelemetry SDK pipelines.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.shutdown(ctx)
}
