package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"connectrpc.com/connect"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
	tasksv1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1"
	taskslambda "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/service/lambda"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/tasksv1connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var rootCmd = &cobra.Command{
	Use:   "tasks",
	Short: "A simple CLI to manage tasks",
}

var createCmd = &cobra.Command{
	Use:           "create",
	Short:         "Create a new task",
	SilenceErrors: true,
	Args:          cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		title := cmd.Flag("title").Value.String()

		var description *string
		if cmd.Flag("description").Changed {
			description = proto.String(cmd.Flag("description").Value.String())
		}

		createResp, err := client.CreateTask(cmd.Context(), &connect.Request[tasksv1.CreateTaskRequest]{
			Msg: &tasksv1.CreateTaskRequest{
				Title:       title,
				Description: description,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create task: %w", err)
		}

		b, err := protojson.Marshal(createResp.Msg)
		if err != nil {
			return fmt.Errorf("failed to marshal response: %w", err)
		}

		_, err = cmd.OutOrStdout().Write(b)
		if err != nil {
			return fmt.Errorf("failed to write task ID: %w", err)
		}

		return nil
	},
}

func init() {
	createCmd.Flags().String("title", "", "Title of the task")
	createCmd.MarkFlagRequired("title")
	createCmd.Flags().String("description", "", "Description of the task")
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get a task by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		taskID := args[0]

		getResp, err := client.GetTask(cmd.Context(), &connect.Request[tasksv1.GetTaskRequest]{
			Msg: &tasksv1.GetTaskRequest{
				Id: taskID,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to get task: %w", err)
		}

		b, err := protojson.Marshal(getResp.Msg)
		if err != nil {
			return fmt.Errorf("failed to marshal response: %w", err)
		}

		_, err = cmd.OutOrStdout().Write(b)
		if err != nil {
			return fmt.Errorf("failed to write task: %w", err)
		}

		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List tasks",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			filter    *string
			pageToken *string
			pageSize  int32 = 10
		)
		if cmd.Flag("filter").Changed {
			filter = proto.String(cmd.Flag("filter").Value.String())
		}
		if cmd.Flag("page-token").Changed {
			pageToken = proto.String(cmd.Flag("page-token").Value.String())
		}
		if cmd.Flag("page-size").Changed {
			var err error
			pageSize, err = cmd.Flags().GetInt32("page-size")
			if err != nil {
				return fmt.Errorf("failed to get page size: %w", err)
			}

			if pageSize < 1 {
				return fmt.Errorf("page size must be greater than 0")
			}
		}

		listResp, err := client.ListTasks(cmd.Context(), &connect.Request[tasksv1.ListTasksRequest]{
			Msg: &tasksv1.ListTasksRequest{
				Filter:    filter,
				PageToken: pageToken,
				PageSize:  proto.Int32(pageSize),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to list tasks: %w", err)
		}

		b, err := protojson.Marshal(listResp.Msg)
		if err != nil {
			return fmt.Errorf("failed to marshal response: %w", err)
		}

		_, err = cmd.OutOrStdout().Write(b)
		if err != nil {
			return fmt.Errorf("failed to write tasks: %w", err)
		}

		return nil
	},
}

func init() {
	listCmd.Flags().String("filter", "", "Filter tasks")
	listCmd.Flags().String("page-token", "", "Page token for pagination")
	listCmd.Flags().Int32("page-size", 10, "Page size for pagination")
}

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update a task by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		taskID := args[0]

		task := &tasksv1.Task{
			Id: taskID,
		}

		updateMask := &fieldmaskpb.FieldMask{
			Paths: []string{},
		}

		if cmd.Flag("completed").Changed {
			var err error
			task.Completed, err = cmd.Flags().GetBool("completed")
			if err != nil {
				return fmt.Errorf("failed to get completed: %w", err)
			}
			updateMask.Paths = append(updateMask.Paths, "completed")
		}
		if cmd.Flag("title").Changed {
			task.Title = cmd.Flag("title").Value.String()
			updateMask.Paths = append(updateMask.Paths, "title")
		}
		if cmd.Flag("description").Changed {
			task.Description = proto.String(cmd.Flag("description").Value.String())
			updateMask.Paths = append(updateMask.Paths, "description")
		}

		updateResp, err := client.UpdateTask(cmd.Context(), &connect.Request[tasksv1.UpdateTaskRequest]{
			Msg: &tasksv1.UpdateTaskRequest{
				Task:       task,
				UpdateMask: updateMask,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to update task: %w", err)
		}

		b, err := protojson.Marshal(updateResp.Msg)
		if err != nil {
			return fmt.Errorf("failed to marshal response: %w", err)
		}

		_, err = cmd.OutOrStdout().Write(b)
		if err != nil {
			return fmt.Errorf("failed to write task: %w", err)
		}

		return nil
	},
}

func init() {
	updateCmd.Flags().Bool("completed", false, "Mark the task as completed")
	updateCmd.Flags().String("title", "", "Title of the task")
	updateCmd.Flags().String("description", "", "Description of the task")
	updateCmd.MarkFlagRequired("title")
}

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a task by ID",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		taskID := args[0]

		deleteResp, err := client.DeleteTask(cmd.Context(), &connect.Request[tasksv1.DeleteTaskRequest]{
			Msg: &tasksv1.DeleteTaskRequest{
				Id: taskID,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete task: %w", err)
		}

		b, err := protojson.Marshal(deleteResp.Msg)
		if err != nil {
			return fmt.Errorf("failed to marshal response: %w", err)
		}

		_, err = cmd.OutOrStdout().Write(b)
		if err != nil {
			return fmt.Errorf("failed to write task: %w", err)
		}

		return nil
	},
}

var client tasksv1connect.TasksServiceClient

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

	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(updateCmd)
	rootCmd.AddCommand(deleteCmd)

	serverAddr := os.Getenv("TASKS_SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = "http://localhost:9090"
	}

	clientOpts := []connect.ClientOption{}

	token := os.Getenv("TASKS_TOKEN")
	if token != "" {
		clientOpts = append(clientOpts, connect.WithInterceptors(
			connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, ar connect.AnyRequest) (connect.AnyResponse, error) {
					ar.Header().Set("Authorization", "Bearer "+token)

					resp, err := next(ctx, ar)
					if err != nil {
						return nil, err
					}

					return resp, nil
				}
			}),
		))
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	retryClient.HTTPClient = cleanhttp.DefaultClient()

	client = tasksv1connect.NewTasksServiceClient(
		retryClient.StandardClient(),
		serverAddr,
		clientOpts...,
	)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
