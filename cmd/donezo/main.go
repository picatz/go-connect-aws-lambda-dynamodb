package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"connectrpc.com/connect"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/cmd/donezo/cli"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/donezov1connect"
	taskslambda "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/service/lambda"
	"github.com/spf13/cobra"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		err := taskslambda.Run(ctx)
		if err != nil {
			panic(err)
		}
		return
	}

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	serverAddr := "http://127.0.0.1:9090"
	if os.Getenv("DONEZO_SERVER_ADDR") != "" {
		serverAddr = os.Getenv("DONEZO_SERVER_ADDR")
	}

	clientOpts := []connect.ClientOption{}

	token := os.Getenv("DONEZO_TOKEN")
	if token != "" {
		clientOpts = append(clientOpts, connect.WithInterceptors(
			connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, ar connect.AnyRequest) (connect.AnyResponse, error) {
					ar.Header().Set("Authorization", "Bearer "+token)
					return next(ctx, ar)
				}
			}),
		))
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	retryClient.Logger = nil
	retryClient.HTTPClient = cleanhttp.DefaultClient()

	tasksClient := donezov1connect.NewTasksServiceClient(
		retryClient.StandardClient(),
		serverAddr,
		clientOpts...,
	)

	orgClient := donezov1connect.NewOrganizationsServiceClient(
		retryClient.StandardClient(),
		serverAddr,
		clientOpts...,
	)

	projClient := donezov1connect.NewProjectsServiceClient(
		retryClient.StandardClient(),
		serverAddr,
		clientOpts...,
	)

	cmd := &cobra.Command{
		Use:   "donezo",
		Short: "Donezo CLI",
	}

	cmd.PersistentFlags().StringVar(&serverAddr, "server-addr", serverAddr, "server address")

	taskCmd, err := cli.NewTaskCommand(tasksClient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating task command: %v\n", err)
		os.Exit(1)
	}

	projectCmd, err := cli.NewProjectCommand(projClient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating project command: %v\n", err)
		os.Exit(1)
	}

	organizationCmd, err := cli.NewOrganizationCommand(orgClient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating organization command: %v\n", err)
		os.Exit(1)
	}

	cmd.AddCommand(
		organizationCmd,
		projectCmd,
		taskCmd,
	)

	return cmd
}
