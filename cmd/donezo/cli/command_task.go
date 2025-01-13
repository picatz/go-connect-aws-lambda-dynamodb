package cli

import (
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	donezov1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/donezov1connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protopath"
	"google.golang.org/protobuf/reflect/protorange"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func NewTaskCommand(taskClient donezov1connect.TasksServiceClient) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:     "task",
		Short:   "Manage tasks",
		Args:    cobra.NoArgs,
		Aliases: []string{"t"},
	}
	cmd.PersistentFlags().StringP("organization-id", "o", os.Getenv("DONEZO_ORGANIZATION"), "Organization ID (overrides context)")
	cmd.PersistentFlags().StringP("project-id", "p", os.Getenv("DONEZO_PROJECT"), "Project ID (overrides context)")

	addRequiredOrgAndProjectFlags := func(c *cobra.Command) {
		c.Flags().StringP("organization-id", "o", os.Getenv("DONEZO_ORGANIZATION"), "Organization ID (overrides context)")
		c.Flags().StringP("project-id", "p", os.Getenv("DONEZO_PROJECT"), "Project ID (overrides context)")
		c.MarkFlagRequired("organization-id")
		c.MarkFlagRequired("project-id")
	}

	createCmd := &cobra.Command{
		Use:   "create <title>",
		Short: "Create a new task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			title := args[0]

			resp, err := taskClient.CreateTask(
				ctx,
				connect.NewRequest(&donezov1.CreateTaskRequest{
					Title: title,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Created task: %s\n", resp.Msg.GetTask().GetId())
			return err
		},
	}

	createTaskFlags, err := FlagsFromMessage((&donezov1.Task{}).ProtoReflect().Descriptor(),
		[]string{"id", "project_id", "organization_id", "completed", "tags"},
	)
	if err != nil {
		return nil, err
	}
	createCmd.Flags().AddFlagSet(createTaskFlags)

	getCmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Get a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			req := &donezov1.GetTaskRequest{
				Id: args[0],
			}

			err := ApplyFlagsToMessage(cmd.Flags(), req)
			if err != nil {
				return fmt.Errorf("failed to apply flags to message: %w", err)
			}

			resp, err := taskClient.GetTask(
				ctx,
				connect.NewRequest(req),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", resp.Msg.GetTask().GetId(), resp.Msg.GetTask().GetTitle())
			if err != nil {
				return err
			}

			return nil
		},
	}
	addRequiredOrgAndProjectFlags(getCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			id := args[0]

			req := &donezov1.DeleteTaskRequest{
				Id: id,
			}

			err := ApplyFlagsToMessage(cmd.Flags(), req)
			if err != nil {
				return fmt.Errorf("failed to apply flags to message: %w", err)
			}

			_, err = taskClient.DeleteTask(
				ctx,
				connect.NewRequest(req),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted task: %s\n", id)
			if err != nil {
				return err
			}

			return nil
		},
	}
	addRequiredOrgAndProjectFlags(deleteCmd)

	updateCmd := &cobra.Command{
		Use:   "update <id>",
		Short: "Update a task",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			id := args[0]

			updateMask := &fieldmaskpb.FieldMask{}

			task := &donezov1.Task{
				Id: id,
			}

			err := ApplyFlagsToMessage(cmd.Flags(), task)
			if err != nil {
				return fmt.Errorf("failed to apply flags to message: %w", err)
			}

			err = protorange.Range(task.ProtoReflect(), func(p protopath.Values) error {
				path := strings.TrimPrefix(p.Path[1:].String(), ".")

				switch path {
				case "id":
					return nil
				case "project_id":
					return nil
				case "organization_id":
					return nil
				}

				updateMask.Paths = append(updateMask.Paths, path)
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to range over task: %w", err)
			}

			if len(updateMask.Paths) == 0 {
				return fmt.Errorf("no fields to update")
			}

			_, err = taskClient.UpdateTask(
				ctx,
				connect.NewRequest(&donezov1.UpdateTaskRequest{
					Task:       task,
					UpdateMask: updateMask,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Updated task: %s\n", id)

			return err
		},
	}
	addRequiredOrgAndProjectFlags(updateCmd)

	updateTaskFlags, err := FlagsFromMessage(
		(&donezov1.Task{}).ProtoReflect().Descriptor(),
		[]string{"id", "project_id", "organization_id"},
	)
	if err != nil {
		return nil, err
	}
	updateCmd.Flags().AddFlagSet(updateTaskFlags)

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			orgId := cmd.Flags().Lookup("organization-id").Value.String()
			if orgId == "" {
				return fmt.Errorf("organization-id is required")
			}

			var (
				pageToken *string
				pageSize  = int32(10)
			)

			if pt, _ := cmd.Flags().GetString("page-token"); pt != "" {
				pageToken = &pt
			}

			if ps, _ := cmd.Flags().GetInt32("page-size"); ps > 0 {
				pageSize = ps
			}

			resp, err := taskClient.ListTasks(
				ctx,
				connect.NewRequest(&donezov1.ListTasksRequest{
					// OrganizationId: orgId,
					PageSize:  proto.Int32(pageSize),
					PageToken: pageToken,
				}),
			)
			if err != nil {
				return err
			}

			for _, task := range resp.Msg.GetTasks() {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", task.GetId(), task.GetTitle())
				if err != nil {
					return err
				}
			}

			return nil
		},
	}

	listCmd.Flags().String("page-token", "", "Page token")
	listCmd.Flags().Int32("page-size", 10, "Page size")

	cmd.AddCommand(
		createCmd,
		getCmd,
		deleteCmd,
		updateCmd,
		listCmd,
	)

	return cmd, nil
}
