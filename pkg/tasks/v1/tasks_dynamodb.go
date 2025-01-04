package tasksv1

import (
	"fmt"
	"maps"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/picatz/dynabuf"
)

// Marshal marshals a task into a map of attribute values for storage in DynamoDB.
func (t *Task) Marshal() (map[string]types.AttributeValue, error) {
	// Marshal the task into a map of attribute values.
	taskItem, err := dynabuf.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal task: %w", err)
	}

	// Get the concrete type of the task item.
	taskItemMap := taskItem.(map[string]types.AttributeValue)

	primaryKey, err := t.PrimaryKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}

	// Add the primary key to the task item.
	maps.Copy(taskItemMap, primaryKey)

	// Return the task item.
	return taskItemMap, nil
}

// KeyCondition returns a key condition builder for the task that can be used
// for querying DynamoDB.
func (t *Task) KeyCondition() expression.KeyConditionBuilder {
	return expression.Key("pk").Equal(
		expression.Value(fmt.Sprintf("TASK#v1#%s", t.GetId())),
	).And(
		expression.Key("sk").Equal(
			expression.Value(fmt.Sprintf("TASK#v1#%s", t.GetId())),
		),
	)
}

// PrimaryKey returns the primary key for the task that can be used for
// storing, updating, or deleting the task in DynamoDB.
func (t *Task) PrimaryKey() (map[string]types.AttributeValue, error) {
	if t.GetId() == "" {
		return nil, fmt.Errorf("task id is required to generate primary key")
	}
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("TASK#v1#%s", t.GetId())},
		"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("TASK#v1#%s", t.GetId())},
	}, nil
}
