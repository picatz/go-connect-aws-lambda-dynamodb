package service_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"

	tasksv1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/donezov1connect"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/service"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/service/localstack"
	"github.com/picatz/jose/pkg/header"
	"github.com/picatz/jose/pkg/jwa"
	"github.com/picatz/jose/pkg/jwt"
	printer "github.com/picatz/otel-tracetest-printer"
	"github.com/shoenig/test/must"
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

	traceExporter := tracetest.NewInMemoryExporter()

	mux := http.NewServeMux()

	srv, err := service.NewServer(
		ctx,
		awsConfig,
		&service.OTELConfig{
			TraceProvider: trace.NewTracerProvider(
				trace.WithSyncer(traceExporter),
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

	connectHandlerOpts := []connect.HandlerOption{
		connect.WithInterceptors(
			otelInterceptor,
			validateInterceptor,
		),
	}

	mux.Handle(
		donezov1connect.NewTasksServiceHandler(
			srv,
			connectHandlerOpts...,
		),
	)

	mux.Handle(
		donezov1connect.NewOrganizationsServiceHandler(
			srv,
			connectHandlerOpts...,
		),
	)

	mux.Handle(
		donezov1connect.NewProjectsServiceHandler(
			srv,
			connectHandlerOpts...,
		),
	)

	mux.Handle(
		donezov1connect.NewUsersServiceHandler(
			srv,
			connectHandlerOpts...,
		),
	)

	server := httptest.NewServer(authMiddleware.Wrap(mux))

	t.Cleanup(server.Close)

	clientOpts := []connect.ClientOption{
		connect.WithInterceptors(
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
		),
	}

	tasksClient := donezov1connect.NewTasksServiceClient(server.Client(), server.URL, clientOpts...)
	orgsClient := donezov1connect.NewOrganizationsServiceClient(server.Client(), server.URL, clientOpts...)
	projsClient := donezov1connect.NewProjectsServiceClient(server.Client(), server.URL, clientOpts...)
	usersClient := donezov1connect.NewUsersServiceClient(server.Client(), server.URL, clientOpts...)

	// Create organization
	createOrgResp, err := orgsClient.CreateOrganization(ctx, &connect.Request[tasksv1.CreateOrganizationRequest]{
		Msg: &tasksv1.CreateOrganizationRequest{
			Name: "test organization",
		},
	})
	must.NoError(t, err, must.Sprint("failed to create organization"))
	must.NotNil(t, createOrgResp.Msg.GetOrganization().GetId())

	orgID := createOrgResp.Msg.GetOrganization().GetId()

	t.Logf("created organization with id: %s", orgID)

	// Create project
	createProjectResp, err := projsClient.CreateProject(ctx, &connect.Request[tasksv1.CreateProjectRequest]{
		Msg: &tasksv1.CreateProjectRequest{
			Name:           "test project",
			OrganizationId: orgID,
		},
	})
	must.NoError(t, err, must.Sprint("failed to create project"))
	must.NotNil(t, createProjectResp.Msg.GetProject().GetId())

	projectID := createProjectResp.Msg.GetProject().GetId()

	t.Logf("created project with id: %s", projectID)

	// Create user
	createUserResp, err := usersClient.CreateUser(ctx, &connect.Request[tasksv1.CreateUserRequest]{
		Msg: &tasksv1.CreateUserRequest{
			Name: "test user",
		},
	})
	must.NoError(t, err, must.Sprint("failed to create user"))
	must.NotNil(t, createUserResp.Msg.GetUser().GetId())

	userID := createUserResp.Msg.GetUser().GetId()

	t.Logf("created user with id: %s", userID)

	// Create initial task.
	createResp, err := tasksClient.CreateTask(ctx, &connect.Request[tasksv1.CreateTaskRequest]{
		Msg: &tasksv1.CreateTaskRequest{
			Title:          "test",
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
	})
	must.NoError(t, err, must.Sprint("failed to create task"))
	must.False(t, createResp.Msg.GetTask().GetCompleted())

	taskID := createResp.Msg.GetTask().GetId()

	t.Logf("created task with id: %s", taskID)

	// Get task and verify it is not completed.
	getResp, err := tasksClient.GetTask(ctx, &connect.Request[tasksv1.GetTaskRequest]{
		Msg: &tasksv1.GetTaskRequest{
			Id:             taskID,
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
	})
	must.NoError(t, err, must.Sprint("failed to get task"))
	must.False(t, getResp.Msg.GetTask().GetCompleted())

	t.Logf("got task with id: %s", getResp.Msg.GetTask().GetId())

	// Update task to be completed.
	_, err = tasksClient.UpdateTask(ctx, &connect.Request[tasksv1.UpdateTaskRequest]{
		Msg: &tasksv1.UpdateTaskRequest{
			Task: &tasksv1.Task{
				Id:             taskID,
				Title:          getResp.Msg.GetTask().GetTitle(),
				OrganizationId: orgID,
				ProjectId:      projectID,
				Completed:      true,
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"completed"},
			},
		},
	})
	must.NoError(t, err, must.Sprint("failed to update task"))

	t.Logf("updated task with id: %s", getResp.Msg.GetTask().GetId())

	// Get task after update and verify it is completed.
	getTaskAfterUpdateResp, err := tasksClient.GetTask(ctx, &connect.Request[tasksv1.GetTaskRequest]{
		Msg: &tasksv1.GetTaskRequest{
			Id:             taskID,
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
	})
	must.NoError(t, err, must.Sprint("failed to get task after update"))
	must.True(t, getTaskAfterUpdateResp.Msg.GetTask().GetCompleted())

	t.Logf("got task after update with id: %s", getTaskAfterUpdateResp.Msg.GetTask().GetId())

	// Create another task to test list (and filter) functionality.
	createResp2, err := tasksClient.CreateTask(ctx, &connect.Request[tasksv1.CreateTaskRequest]{
		Msg: &tasksv1.CreateTaskRequest{
			Title:          "test2",
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
	})
	must.NoError(t, err, must.Sprint("failed to create task 2"))
	must.False(t, createResp2.Msg.GetTask().GetCompleted())

	t.Logf("created %q with id: %s", createResp2.Msg.GetTask().GetTitle(), createResp2.Msg.GetTask().GetId())

	// List tasks and verify there are two, one completed and one not.
	simpleListResp, err := tasksClient.ListTasks(ctx, &connect.Request[tasksv1.ListTasksRequest]{
		Msg: &tasksv1.ListTasksRequest{
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
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
		filterListResp, err := tasksClient.ListTasks(ctx, &connect.Request[tasksv1.ListTasksRequest]{
			Msg: &tasksv1.ListTasksRequest{
				Filter:         proto.String(test.filter),
				OrganizationId: orgID,
				ProjectId:      projectID,
			},
		})
		must.NoError(t, err, must.Sprintf("failed to list tasks with filter: %s", test.filter))
		must.Len(t, test.count, filterListResp.Msg.GetTasks())
	}

	_, err = tasksClient.DeleteTask(ctx, &connect.Request[tasksv1.DeleteTaskRequest]{
		Msg: &tasksv1.DeleteTaskRequest{
			Id:             createResp.Msg.GetTask().GetId(),
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
	})
	must.NoError(t, err, must.Sprint("failed to delete task"))

	_, err = tasksClient.GetTask(ctx, &connect.Request[tasksv1.GetTaskRequest]{
		Msg: &tasksv1.GetTaskRequest{
			Id:             createResp.Msg.GetTask().GetId(),
			OrganizationId: orgID,
			ProjectId:      projectID,
		},
	})
	must.Error(t, err, must.Sprint("expected error getting deleted task"))

	printer.PrintSpanTree(os.Stdout, traceExporter.GetSpans())
}
