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

func NewProjectCommand(projClient donezov1connect.ProjectsServiceClient) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:     "project",
		Short:   "Manage projects",
		Args:    cobra.NoArgs,
		Aliases: []string{"proj", "p"},
	}
	cmd.PersistentFlags().StringP("organization-id", "", os.Getenv("DONEZO_ORGANIZATION"), "Organization ID")

	addRequiredOrgFlag := func(c *cobra.Command) {
		c.Flags().StringP("organization-id", "", os.Getenv("DONEZO_ORGANIZATION"), "Organization ID")
		c.MarkFlagRequired("organization-id")
	}

	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			name := args[0]

			resp, err := projClient.CreateProject(
				ctx,
				connect.NewRequest(&donezov1.CreateProjectRequest{
					Name: name,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Created project: %s\n", resp.Msg.GetProject().GetId())

			return err

		},
	}
	addRequiredOrgFlag(createCmd)

	getCmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Get a project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			id := args[0]

			orgId := cmd.Flags().Lookup("organization-id").Value.String()
			if orgId == "" {
				return fmt.Errorf("organization-id is required")
			}

			resp, err := projClient.GetProject(
				ctx,
				connect.NewRequest(&donezov1.GetProjectRequest{
					Id:             id,
					OrganizationId: orgId,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", resp.Msg.GetProject().GetId(), resp.Msg.GetProject().GetName())
			if err != nil {
				return err
			}

			return nil

		},
	}
	addRequiredOrgFlag(getCmd)

	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a project",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			id := args[0]

			_, err := projClient.DeleteProject(
				ctx,
				connect.NewRequest(&donezov1.DeleteProjectRequest{
					Id: id,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted project: %s\n", id)
			if err != nil {
				return err
			}

			return nil
		},
	}
	addRequiredOrgFlag(deleteCmd)

	updateCmd := &cobra.Command{
		Use:   "update <id>",
		Short: "Update a project",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			updateMask := &fieldmaskpb.FieldMask{}

			id := args[0]

			proj := &donezov1.Project{
				Id: id,
			}

			err := ApplyFlagsToMessage(cmd.Flags(), proj)
			if err != nil {
				return fmt.Errorf("failed to apply flags to message: %w", err)
			}

			err = protorange.Range(proj.ProtoReflect(), func(p protopath.Values) error {
				path := strings.TrimPrefix(p.Path[1:].String(), ".")

				switch path {
				case "id":
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

			_, err = projClient.UpdateProject(
				ctx,
				connect.NewRequest(&donezov1.UpdateProjectRequest{
					Project:    proj,
					UpdateMask: updateMask,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Updated project: %s\n", id)

			return err
		},
	}
	addRequiredOrgFlag(updateCmd)

	updateCmdFlags, err := FlagsFromMessage(
		(&donezov1.Project{}).ProtoReflect().Descriptor(),
		[]string{"id", "organization_id"},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create flags from message: %w", err)
	}
	updateCmd.Flags().AddFlagSet(updateCmdFlags)

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List projects",
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

			resp, err := projClient.ListProjects(
				ctx,
				connect.NewRequest(&donezov1.ListProjectsRequest{
					// OrganizationId: orgId,
					PageSize:  proto.Int32(pageSize),
					PageToken: pageToken,
				}),
			)
			if err != nil {
				return err
			}

			for _, project := range resp.Msg.GetProjects() {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", project.GetId(), project.GetName())
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
