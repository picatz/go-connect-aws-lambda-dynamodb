package localstack

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/service"
	"github.com/shoenig/test/must"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
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

func Setup(t *testing.T) aws.Config {
	t.Helper()

	ctx := testContext(t)

	localStack, err := localstack.Run(
		ctx,
		"localstack/localstack:latest",
		testcontainers.WithHostPortAccess(),
	)
	must.NoError(t, err, must.Sprint("failed to run localstack container"))

	t.Cleanup(func() {
		localStack.Terminate(ctx)
	})

	host, err := localStack.Host(ctx)
	must.NoError(t, err, must.Sprint("failed to get host port from localstack"))

	port, err := localStack.MappedPort(ctx, "4566/tcp")
	must.NoError(t, err, must.Sprint("failed to get mapped port from localstack"))

	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     "test",
					SecretAccessKey: "test",
					Source:          "test",
				}, nil
			}),
		),
	)
	must.NoError(t, err, must.Sprint("failed to load default config"))

	awsConfig.BaseEndpoint = aws.String("http://" + net.JoinHostPort(host, port.Port()))

	return awsConfig
}

func SetupDev(ctx context.Context) (aws.Config, func(), error) {
	var noop = func() {}

	localStack, err := localstack.Run(
		ctx,
		"localstack/localstack:latest",
		testcontainers.WithHostPortAccess(),
	)
	if err != nil {
		return aws.Config{}, noop, fmt.Errorf("failed to run localstack container: %w", err)
	}

	host, err := localStack.Host(ctx)
	if err != nil {
		return aws.Config{}, noop, fmt.Errorf("failed to get host port from localstack: %w", err)
	}

	port, err := localStack.MappedPort(ctx, "4566/tcp")
	if err != nil {
		return aws.Config{}, noop, fmt.Errorf("failed to get mapped port from localstack: %w", err)
	}

	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Source:          "hardcoded credentials",
			}, nil
		})),
	)
	if err != nil {
		return aws.Config{}, noop, fmt.Errorf("failed to load AWS config: %w", err)
	}

	awsConfig.BaseEndpoint = aws.String(fmt.Sprintf("http://%s:%s", host, port.Port()))

	return awsConfig, func() {
		localStack.Terminate(ctx)
	}, nil
}

func SetupDynamoDB(ctx context.Context, awsConfig aws.Config) error {
	dynamoDBClient := dynamodb.NewFromConfig(awsConfig)

	_, err := dynamoDBClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   service.Table,
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       types.KeyTypeRange,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}
