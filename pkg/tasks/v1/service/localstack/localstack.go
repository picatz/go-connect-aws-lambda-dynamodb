package localstack

import (
	"context"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
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
