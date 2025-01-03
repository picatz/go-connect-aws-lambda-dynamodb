package service_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"

	tasksv1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/service"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/service/localstack"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/tasksv1connect"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	printer "github.com/picatz/otel-tracetest-printer"
	"github.com/shoenig/test/must"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func testContext(t *testing.T) context.Context {
	t.Helper()

	dl, ok := t.Deadline()
	if !ok {
		return context.Background()
	}

	ctx, cancel := context.WithDeadline(context.Background(), dl)
	t.Cleanup(cancel)

	return ctx
}

func TestServer(t *testing.T) {
	ctx := context.Background()

	awsConfig := localstack.Setup(t)

	err := localstack.SetupDynamoDB(ctx, awsConfig)
	must.NoError(t, err)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	exp, err := stdoutmetric.New(
		stdoutmetric.WithEncoder(enc),
		stdoutmetric.WithoutTimestamps(),
	)
	must.NoError(t, err)
	t.Cleanup(func() {
		must.NoError(t, exp.Shutdown(ctx))
	})

	exporter := tracetest.NewInMemoryExporter()

	mux := http.NewServeMux()

	srv, err := service.NewServer(
		ctx,
		awsConfig,
		&service.OTELConfig{
			TraceProvider: trace.NewTracerProvider(
				trace.WithSyncer(exporter),
			),
		},
	)
	must.NoError(t, err)

	t.Cleanup(func() {
		must.NoError(t, srv.Shutdown(ctx))
	})

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.NoError(t, err)

	token, err := jwt.New(
		header.Parameters{
			header.Type:      jwt.Type,
			header.Algorithm: jwa.ES256,
		},
		jwt.ClaimsSet{
			"sub":  "1234567890",
			"name": "John Doe",
		},
		privateKey,
	)
	must.NoError(t, err)

	t.Logf("using token for client requests: %q", token)

	authMiddleware := authn.NewMiddleware(func(ctx context.Context, r *http.Request) (any, error) {
		bearerToken, err := jwt.FromHTTPAuthorizationHeader(r)
		if err != nil {
			return nil, authn.Errorf("failed to get bearer token from authorization header: %w", err)
		}

		token, err := jwt.ParseAndVerify(bearerToken, jwt.WithKey(&privateKey.PublicKey))
		if err != nil {
			return nil, authn.Errorf("failed to parse and verify bearer token: %w", err)
		}

		return token, nil
	})

	otelInterceptor, err := otelconnect.NewInterceptor()
	must.NoError(t, err)

	validateInterceptor, err := validate.NewInterceptor()
	must.NoError(t, err)

	mux.Handle(
		tasksv1connect.NewTasksServiceHandler(
			srv,
			connect.WithInterceptors(
				otelInterceptor,
				validateInterceptor,
			),
		),
	)

	server := httptest.NewServer(authMiddleware.Wrap(mux))

	t.Cleanup(server.Close)

	unauthenticatedClient := tasksv1connect.NewTasksServiceClient(server.Client(), server.URL)

	_, err = unauthenticatedClient.CreateTask(ctx, &connect.Request[tasksv1.CreateTaskRequest]{
		Msg: &tasksv1.CreateTaskRequest{
			Title: "test",
		},
	})
	must.Error(t, err, must.Sprint("expected error creating task"))

	authenticatedClient := tasksv1connect.NewTasksServiceClient(server.Client(), server.URL, connect.WithInterceptors(
		// Add the token to all the client requests.
		connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, ar connect.AnyRequest) (connect.AnyResponse, error) {
				ar.Header().Set("Authorization", "Bearer "+token.String())

				resp, err := next(ctx, ar)
				if err != nil {
					return nil, err
				}

				return resp, nil
			}
		}),
	))

	// Create initial task.
	createResp, err := authenticatedClient.CreateTask(ctx, &connect.Request[tasksv1.CreateTaskRequest]{
		Msg: &tasksv1.CreateTaskRequest{
			Title: "test",
		},
	})
	must.NoError(t, err, must.Sprint("failed to create task"))
	must.False(t, createResp.Msg.GetTask().GetCompleted())

	// Get task and verify it is not completed.
	getResp, err := authenticatedClient.GetTask(ctx, &connect.Request[tasksv1.GetTaskRequest]{
		Msg: &tasksv1.GetTaskRequest{
			Id: createResp.Msg.GetTask().GetId(),
		},
	})
	must.NoError(t, err, must.Sprint("failed to get task"))
	must.False(t, getResp.Msg.GetTask().GetCompleted())

	// Update task to be completed.
	updateResp, err := authenticatedClient.UpdateTask(ctx, &connect.Request[tasksv1.UpdateTaskRequest]{
		Msg: &tasksv1.UpdateTaskRequest{
			Task: &tasksv1.Task{
				Id:        getResp.Msg.GetTask().GetId(),
				Completed: true,
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"completed"},
			},
		},
	})
	must.NoError(t, err, must.Sprint("failed to update task"))
	must.True(t, updateResp.Msg.GetTask().GetCompleted())

	// Get task after update and verify it is completed.
	getTaskAfterUpdateResp, err := authenticatedClient.GetTask(ctx, &connect.Request[tasksv1.GetTaskRequest]{
		Msg: &tasksv1.GetTaskRequest{
			Id: updateResp.Msg.GetTask().GetId(),
		},
	})
	must.NoError(t, err, must.Sprint("failed to get task after update"))
	must.True(t, getTaskAfterUpdateResp.Msg.GetTask().GetCompleted())

	// Create another task to test list (and filter) functionality.
	createResp2, err := authenticatedClient.CreateTask(ctx, &connect.Request[tasksv1.CreateTaskRequest]{
		Msg: &tasksv1.CreateTaskRequest{
			Title: "test2",
		},
	})
	must.NoError(t, err, must.Sprint("failed to create task 2"))
	must.False(t, createResp2.Msg.GetTask().GetCompleted())

	// List tasks and verify there are two, one completed and one not.
	simpleListResp, err := authenticatedClient.ListTasks(ctx, &connect.Request[tasksv1.ListTasksRequest]{
		Msg: &tasksv1.ListTasksRequest{},
	})
	must.NoError(t, err, must.Sprint("failed to list tasks"))
	must.Len(t, 2, simpleListResp.Msg.GetTasks())

	// Test various filters.
	for _, test := range []struct {
		filter string
		count  int
	}{
		{
			filter: "completed == true",
			count:  1,
		},
		{
			filter: "completed == false",
			count:  1,
		},
		{
			filter: "completed == false && title == 'test2'",
			count:  1,
		},
		{
			filter: "completed == false || title == 'test'",
			count:  2,
		},
		{
			filter: "begins_with(title, 'test')", // DynamoDB native feeling filter.
			count:  2,
		},
		{
			filter: "title.startsWith('test')", // CEL native feeling filter.
			count:  2,
		},
	} {
		filterListResp, err := authenticatedClient.ListTasks(ctx, &connect.Request[tasksv1.ListTasksRequest]{
			Msg: &tasksv1.ListTasksRequest{
				Filter: proto.String(test.filter),
			},
		})
		must.NoError(t, err, must.Sprintf("failed to list tasks with filter: %s", test.filter))
		must.Len(t, test.count, filterListResp.Msg.GetTasks())
	}

	deleteResp, err := authenticatedClient.DeleteTask(ctx, &connect.Request[tasksv1.DeleteTaskRequest]{
		Msg: &tasksv1.DeleteTaskRequest{
			Id: updateResp.Msg.GetTask().GetId(),
		},
	})
	must.NoError(t, err, must.Sprint("failed to delete task"))

	_, err = authenticatedClient.GetTask(ctx, &connect.Request[tasksv1.GetTaskRequest]{
		Msg: &tasksv1.GetTaskRequest{
			Id: deleteResp.Msg.GetId(),
		},
	})
	must.Error(t, err, must.Sprint("expected error getting deleted task"))

	printer.PrintSpanTree(os.Stdout, exporter.GetSpans())
}
