package lambda_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	tasksv1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/donezov1connect"
	"github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1/service/localstack"
	"github.com/shoenig/test/must"
	"google.golang.org/protobuf/encoding/protojson"
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

func bundleCode(t *testing.T) []byte {
	t.Helper()

	// Set the environment variables for cross-compilation.
	env := append(os.Environ(),
		"GOOS=linux",
		"GOARCH=arm64",
		"CGO_ENABLED=0", // Disable CGO to ensure a statically linked binary.
	)

	// Prepare the 'go build' command.
	cmd := exec.Command("go", "build", "-tags", "lambda.norpc", "-o", "./testdata/bootstrap", "./testdata/main.go")
	cmd.Env = env

	// Capture the output and error streams.
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	// Run the command.
	err := cmd.Run()
	must.NoError(t, err, must.Sprintf("failed to run command: %v\nOutput: %s\nError: %s", err, outBuf.String(), errBuf.String()))

	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)

	// Create a new ZIP writer.
	zipWriter := zip.NewWriter(buf)

	// Open the file to be added to the archive.
	file, err := os.Open("./testdata/bootstrap")
	must.NoError(t, err, must.Sprint("failed to open file"))
	defer file.Close()

	// Get file information to obtain the file name and other details.
	fileInfo, err := file.Stat()
	must.NoError(t, err, must.Sprint("failed to get file info"))

	// Create a header based on the file info.
	header, err := zip.FileInfoHeader(fileInfo)
	must.NoError(t, err, must.Sprint("failed to create file info header"))

	// Set the compression method to Deflate for better compression.
	header.Method = zip.Deflate

	// Ensure the file is named "bootstrap" in the archive, as required by AWS Lambda.
	header.Name = "bootstrap"

	header.SetMode(0755 | os.ModeSymlink)

	header.CreatorVersion = 3 << 8    // indicates Unix
	header.ExternalAttrs = 0777 << 16 // -rwxrwxrwx file permissions

	// Create a writer for the file in the ZIP archive.
	writer, err := zipWriter.CreateHeader(header)
	must.NoError(t, err, must.Sprint("failed to create header"))

	// Copy the file content into the ZIP archive.
	_, err = io.Copy(writer, file)
	must.NoError(t, err, must.Sprint("failed to copy file content"))

	// Close the ZIP writer to flush the data to the buffer.
	err = zipWriter.Close()
	must.NoError(t, err, must.Sprint("failed to close zip writer"))

	// Return the bytes of the ZIP archive.
	return buf.Bytes()
}

func TestRun(t *testing.T) {
	ctx := testContext(t)

	awsConfig := localstack.Setup(t)

	svc := lambda.NewFromConfig(awsConfig)

	testFunction, err := svc.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: aws.String("test"),
		Runtime:      types.RuntimeProvidedal2,
		Role:         aws.String("arn:aws:iam::123456789012:role/execution_role"), // Replace with your IAM role ARN
		Handler:      aws.String("main"),
		Description:  aws.String("My Go Lambda function"),
		Timeout:      aws.Int32(15),
		MemorySize:   aws.Int32(128),
		Publish:      true,
		Code: &types.FunctionCode{
			ZipFile: bundleCode(t),
		},
	})
	must.NoError(t, err, must.Sprint("failed to create function"))

	for {
		fun, err := svc.GetFunction(ctx, &lambda.GetFunctionInput{
			FunctionName: testFunction.FunctionName,
		})
		must.NoError(t, err, must.Sprint("failed to get function"))

		if fun.Configuration.State == types.StateActive {
			break
		}

		time.Sleep(1 * time.Second)
	}

	payloadBody := &tasksv1.CreateTaskRequest{
		Title: "test",
	}
	payloadBodyBytes, err := protojson.Marshal(payloadBody)
	must.NoError(t, err, must.Sprint("failed to marshal payload body"))

	payload, err := json.Marshal(&events.APIGatewayProxyRequest{
		HTTPMethod: http.MethodPost,
		Path:       donezov1connect.TasksServiceCreateTaskProcedure,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(payloadBodyBytes),
	})
	must.NoError(t, err, must.Sprint("failed to marshal payload"))

	output, err := svc.Invoke(ctx, &lambda.InvokeInput{
		FunctionName: testFunction.FunctionName,
		Payload:      payload,
	})
	must.NoError(t, err, must.Sprint("failed to invoke lambda"))

	resp := &events.APIGatewayProxyResponse{}
	err = json.Unmarshal(output.Payload, resp)
	must.NoError(t, err, must.Sprint("failed to unmarshal response"))

	t.Logf("Response output status code: %d\n", resp.StatusCode)
	t.Logf("Response output headers: %#+v\n", resp.Headers)
	t.Logf("Response output body: %s\n", resp.Body)
}
