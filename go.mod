module github.com/picatz/go-connect-aws-lambda-dynamodb

go 1.23.1

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.1-20241127180247-a33202765966.1
	connectrpc.com/authn v0.2.0
	connectrpc.com/connect v1.17.0
	connectrpc.com/otelconnect v0.7.1
	connectrpc.com/validate v0.1.0
	github.com/aws/aws-lambda-go v1.47.0
	github.com/aws/aws-sdk-go-v2 v1.32.7
	github.com/aws/aws-sdk-go-v2/config v1.28.7
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.7.57
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.38.1
	github.com/aws/aws-sdk-go-v2/service/lambda v1.69.2
	github.com/awslabs/aws-lambda-go-api-proxy v0.16.2
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.25.1
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-retryablehttp v0.7.7
	github.com/picatz/dynabuf v0.0.0-20241230193431-4dc46648ce77
	github.com/picatz/jose v0.0.0-20241214175604-bb7476e581e6
	github.com/picatz/otel-tracetest-printer v0.0.0-20241230223140-f527e294b732
	github.com/shoenig/test v1.12.0
	github.com/spf13/cobra v1.8.1
	github.com/testcontainers/testcontainers-go v0.34.0
	github.com/testcontainers/testcontainers-go/modules/localstack v0.34.0
	go.opentelemetry.io/contrib/bridges/otelslog v0.8.0
	go.opentelemetry.io/contrib/detectors/aws/lambda v0.58.0
	go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws v0.58.0
	go.opentelemetry.io/contrib/propagators/aws v1.33.0
	go.opentelemetry.io/otel v1.33.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.33.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.9.0
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.33.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.33.0
	go.opentelemetry.io/otel/log v0.9.0
	go.opentelemetry.io/otel/sdk v1.33.0
	go.opentelemetry.io/otel/sdk/log v0.9.0
	go.opentelemetry.io/otel/sdk/metric v1.33.0
	go.opentelemetry.io/otel/trace v1.33.0
	google.golang.org/genproto/googleapis/api v0.0.0-20241230172942-26aa7a208def
	google.golang.org/grpc v1.69.2
	google.golang.org/protobuf v1.36.1
)

require (
	cel.dev/expr v0.19.1 // indirect
	dario.cat/mergo v1.0.1 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.48 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.15.22 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.24.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.33.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.37.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.3 // indirect
	github.com/aws/smithy-go v1.22.1 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/bufbuild/protovalidate-go v0.8.2 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/charmbracelet/lipgloss v1.0.0 // indirect
	github.com/charmbracelet/x/ansi v0.4.2 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/docker v27.4.1+incompatible // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/cel-go v0.22.1 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20240909124753-873cd0166683 // indirect
	github.com/magiconair/properties v1.8.9 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/user v0.3.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.9.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.58.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.33.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.33.0 // indirect
	go.opentelemetry.io/otel/metric v1.33.0 // indirect
	go.opentelemetry.io/proto/otlp v1.4.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/exp v0.0.0-20241217172543-b2144cdd0a67 // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241230172942-26aa7a208def // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)