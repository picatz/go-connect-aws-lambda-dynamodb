package main

import (
	"context"
	"os"
	"os/signal"

	tasklambda "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/tasks/v1/service/lambda"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err := tasklambda.Run(ctx)
	if err != nil {
		panic(err)
	}
}
