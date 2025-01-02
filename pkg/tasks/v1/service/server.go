package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/picatz/dynabuf"
	"github.com/picatz/dynabuf/expr"
	tasksv1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/tasksv1connect"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// TableTasks is the name of the DynamoDB table used to store tasks.
var TableTasks = aws.String("tasks")

// Server implements a tasks service.
type Server struct {
	logger   *slog.Logger
	tracer   trace.Tracer
	db       *dynamodb.Client
	cfg      *OTELConfig
	shutdown func(context.Context) error
	_        tasksv1connect.UnimplementedTasksServiceHandler
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

// errorf is a helper function to log an error, record it in the current span,
// and return a connect.Error with the given code and user message. The error
// message will include the trace ID and span ID for easier debugging, and the
// user message will be included as an attribute in the span. The error will
// also be logged with the trace ID and span ID for correlation purposes.
func (s *Server) errorf(ctx context.Context, c connect.Code, underlying error, userMessage string) error {
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

// CreateTask creates a new task, stores it in DynamoDB, and returns the created task.
func (s *Server) CreateTask(ctx context.Context, req *connect.Request[tasksv1.CreateTaskRequest]) (*connect.Response[tasksv1.CreateTaskResponse], error) {
	task := &tasksv1.Task{
		Id:          uuid.New().String(),
		Title:       req.Msg.GetTitle(),
		Description: req.Msg.Description,
		Completed:   req.Msg.GetCompleted(),
	}

	taskItem, err := dynabuf.Marshal(task)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to marshal task")
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: TableTasks,
		Item:      taskItem.(map[string]types.AttributeValue),
	})

	s.logger.InfoContext(ctx, "created task", "task_id", task.Id)

	return connect.NewResponse(&tasksv1.CreateTaskResponse{
		Task: task,
	}), nil
}

// DeleteTask deletes a task from DynamoDB based on the provided task ID.
func (s *Server) DeleteTask(ctx context.Context, req *connect.Request[tasksv1.DeleteTaskRequest]) (*connect.Response[tasksv1.DeleteTaskResponse], error) {
	taskID := req.Msg.GetId()

	_, err := s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: TableTasks,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: taskID},
		},
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to delete task")
	}

	s.logger.InfoContext(ctx, "deleted task", "task_id", taskID)

	return connect.NewResponse(&tasksv1.DeleteTaskResponse{
		Id: taskID,
	}), nil
}

// GetTask retrieves a task from DynamoDB based on the provided task ID.
func (s *Server) GetTask(ctx context.Context, req *connect.Request[tasksv1.GetTaskRequest]) (*connect.Response[tasksv1.GetTaskResponse], error) {
	taskId := req.Msg.GetId()

	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: TableTasks,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: taskId},
		},
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to get task")
	}

	if resp.Item == nil {
		return nil, s.errorf(ctx, connect.CodeNotFound, fmt.Errorf("unable to find task with ID %q", taskId), "task not found")
	}

	task := &tasksv1.Task{}
	err = dynabuf.Unmarshal(resp.Item, task)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal task")
	}

	s.logger.InfoContext(ctx, "got task", "task_id", taskId)

	return connect.NewResponse(&tasksv1.GetTaskResponse{
		Task: task,
	}), nil
}

// ListTasks retrieves a list of tasks from DynamoDB with optional pagination and filtering.
func (s *Server) ListTasks(ctx context.Context, req *connect.Request[tasksv1.ListTasksRequest]) (*connect.Response[tasksv1.ListTasksResponse], error) {
	// Set default page size if not provided
	pageSize := req.Msg.GetPageSize()
	if pageSize == 0 {
		pageSize = 10
	}

	// Setup page token for pagination if nessessary
	var exclusiveStartKey map[string]types.AttributeValue
	pageToken := req.Msg.GetPageToken()
	if pageToken != "" {
		// Decode pageToken to get ExclusiveStartKey
		decodedToken, err := base64.URLEncoding.DecodeString(pageToken)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "invalid page token")
		}
		err = json.Unmarshal(decodedToken, &exclusiveStartKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "invalid page token")
		}
	}

	var filterExpr expression.Expression
	if req.Msg.Filter != nil {
		ctx, span := s.tracer.Start(ctx, "build-filter-expression")
		defer span.End()

		filterEnv, err := expr.NewEnv(expr.MessageFieldVariables(&tasksv1.Task{})...)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to create CEL environment")
		}

		filterAst, issues := filterEnv.Compile(req.Msg.GetFilter())
		if issues != nil && issues.Err() != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, issues.Err(), "failed to compile CEL expression")
		}

		filterCond, err := expr.Filter(filterAst)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to create filter expression")
		}

		filterExpr, err = expression.NewBuilder().WithFilter(filterCond).Build()
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build filter expression")
		}

		span.End()
	}

	// Perform the DynamoDB Scan operation
	scanInput := &dynamodb.ScanInput{
		TableName:                 TableTasks,
		Limit:                     aws.Int32(pageSize),
		ExclusiveStartKey:         exclusiveStartKey,
		FilterExpression:          filterExpr.Filter(),
		ExpressionAttributeNames:  filterExpr.Names(),
		ExpressionAttributeValues: filterExpr.Values(),
	}
	resp, err := s.db.Scan(ctx, scanInput)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to list tasks")
	}

	// Unmarshal the items into tasks
	ctx, span := s.tracer.Start(ctx, "unmarshal-tasks")
	defer span.End()

	tasks := make([]*tasksv1.Task, 0, len(resp.Items))
	err = dynabuf.Unmarshal(resp.Items, &tasks)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal tasks")
	}

	span.End()

	// Prepare the next page token if there are more items
	var nextPageToken string
	if len(resp.LastEvaluatedKey) > 0 {
		// Encode LastEvaluatedKey as nextPageToken
		encodedKey, err := json.Marshal(resp.LastEvaluatedKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to encode next page token")
		}
		nextPageToken = base64.URLEncoding.EncodeToString(encodedKey)
	}

	s.logger.InfoContext(ctx, "listed tasks", "num_tasks", len(tasks), "page_size", pageSize, "next_page_token", nextPageToken)

	// Construct and return the response
	return connect.NewResponse(&tasksv1.ListTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageToken,
	}), nil
}

// UpdateTask updates a task in DynamoDB based on the provided task ID and field mask.
func (s *Server) UpdateTask(ctx context.Context, req *connect.Request[tasksv1.UpdateTaskRequest]) (*connect.Response[tasksv1.UpdateTaskResponse], error) {
	// Extract the task and update mask from the request
	updatedTask := req.Msg.GetTask()
	updateMask := req.Msg.GetUpdateMask()

	// Validate input
	if updatedTask == nil || updatedTask.Id == "" {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("task ID is required"), "task ID is required")
	}
	if updateMask == nil || len(updateMask.Paths) == 0 {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("update mask is required"), "update mask is required")
	}

	// Prepare the update expression based on the field mask
	var updateExpr expression.UpdateBuilder
	for _, path := range updateMask.Paths {
		switch path {
		case "title":
			updateExpr = updateExpr.Set(expression.Name("title"), expression.Value(updatedTask.Title))
		case "description":
			updateExpr = updateExpr.Set(expression.Name("description"), expression.Value(updatedTask.Description))
		case "completed":
			updateExpr = updateExpr.Set(expression.Name("completed"), expression.Value(updatedTask.Completed))
		default:
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("unsupported field in update mask: %s", path), "unsupported field in update mask")
		}
	}

	// Build the DynamoDB expression
	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build update expression")
	}

	// Perform the update operation
	input := &dynamodb.UpdateItemInput{
		TableName: TableTasks,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: updatedTask.Id,
			},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueAllNew,
	}

	result, err := s.db.UpdateItem(ctx, input)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to update task")
	}

	// Unmarshal the updated item
	updatedTask = &tasksv1.Task{}
	err = dynabuf.Unmarshal(result.Attributes, updatedTask)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal updated task")
	}

	s.logger.InfoContext(ctx, "updated task", "task_id", updatedTask.Id)

	// Return the updated task in the response
	return connect.NewResponse(&tasksv1.UpdateTaskResponse{
		Task: updatedTask,
	}), nil
}
