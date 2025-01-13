package cli

import (
	"fmt"

	"connectrpc.com/connect"
	donezov1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/donezov1connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
)

func NewOrganizationCommand(orgClient donezov1connect.OrganizationsServiceClient) (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:     "organization",
		Short:   "Manage organizations",
		Args:    cobra.NoArgs,
		Aliases: []string{"org", "o"},
	}

	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			name := args[0]

			resp, err := orgClient.CreateOrganization(
				ctx,
				connect.NewRequest(&donezov1.CreateOrganizationRequest{
					Name: name,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Created organization: %s\n", resp.Msg.GetOrganization().GetId())

			return err
		},
	}

	updateCmd := &cobra.Command{
		Use:   "update <id>",
		Short: "update an existing organization",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			id := args[0]

			name, _ := cmd.Flags().GetString("name")

			_, err := orgClient.UpdateOrganization(
				ctx,
				connect.NewRequest(&donezov1.UpdateOrganizationRequest{
					Organization: &donezov1.Organization{
						Id:   id,
						Name: name,
					},
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Updated organization: %s\n", id)

			return err
		},
	}

	updateCmd.Flags().String("name", "", "Organization name")
	updateCmd.MarkFlagRequired("name")

	deleteCmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			confirm, _ := cmd.Flags().GetBool("confirm")
			if !confirm {
				return fmt.Errorf("must confirm deletion with --confirm")
			}

			id := args[0]

			_, err := orgClient.DeleteOrganization(
				ctx,
				connect.NewRequest(&donezov1.DeleteOrganizationRequest{
					Id: id,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted organization: %s\n", id)

			return err
		},
	}

	deleteCmd.Flags().Bool("confirm", false, "Confirm deletion")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List organizations",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

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

			resp, err := orgClient.ListOrganizations(
				ctx,
				connect.NewRequest(&donezov1.ListOrganizationsRequest{
					PageToken: pageToken,
					PageSize:  proto.Int32(pageSize),
				}),
			)
			if err != nil {
				return err
			}

			for _, org := range resp.Msg.GetOrganizations() {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", org.GetId(), org.GetName())
				if err != nil {
					return err
				}
			}

			if pt := resp.Msg.GetNextPageToken(); pt != "" {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "Next page token: %s\n", pt)
				if err != nil {
					return err
				}
			}

			return nil

		},
	}

	listCmd.Flags().String("page-token", "", "Page token")
	listCmd.Flags().Int32("page-size", 10, "Page size")

	getCmd := &cobra.Command{
		Use:   "get <id>",
		Short: "Get an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			id := args[0]

			resp, err := orgClient.GetOrganization(
				ctx,
				connect.NewRequest(&donezov1.GetOrganizationRequest{
					Id: id,
				}),
			)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", resp.Msg.GetOrganization().GetId(), resp.Msg.GetOrganization().GetName())
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(createCmd)
	cmd.AddCommand(getCmd)
	cmd.AddCommand(updateCmd)
	cmd.AddCommand(deleteCmd)
	cmd.AddCommand(listCmd)

	return cmd, nil
}
