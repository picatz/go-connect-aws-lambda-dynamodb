package donezov1

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
	var (
		partitionKey = fmt.Sprintf("ORGANIZATION#%s#PROJECT#%s", t.GetOrganizationId(), t.GetProjectId())
		sortKey      = fmt.Sprintf("TASK#%s", t.GetId())
	)

	return expression.Key("pk").Equal(
		expression.Value(partitionKey),
	).And(
		expression.Key("sk").Equal(
			expression.Value(sortKey),
		),
	)
}

// PrimaryKey returns the primary key for the task that can be used for
// storing, updating, or deleting the task in DynamoDB.
func (t *Task) PrimaryKey() (map[string]types.AttributeValue, error) {
	var (
		partitionKey = fmt.Sprintf("ORGANIZATION#%s#PROJECT#%s", t.GetOrganizationId(), t.GetProjectId())
		sortKey      = fmt.Sprintf("TASK#%s", t.GetId())
	)

	if t.GetId() == "" {
		return nil, fmt.Errorf("task id is required to generate primary key")
	}
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: partitionKey},
		"sk": &types.AttributeValueMemberS{Value: sortKey},
	}, nil
}

// Marshal marshals an organization into a map of attribute values for storage in DynamoDB.
func (o *Organization) Marshal() (map[string]types.AttributeValue, error) {
	// Marshal the organization into a map of attribute values.
	orgItem, err := dynabuf.Marshal(o)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal organization: %w", err)
	}

	// Get the concrete type of the organization item.
	orgItemMap := orgItem.(map[string]types.AttributeValue)

	primaryKey, err := o.PrimaryKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}

	// Add the primary key to the organization item.
	maps.Copy(orgItemMap, primaryKey)

	// Return the organization item.
	return orgItemMap, nil
}

// KeyCondition returns a key condition builder for the organization that can be used
// for querying DynamoDB.
func (o *Organization) KeyCondition() expression.KeyConditionBuilder {
	return expression.Key("pk").Equal(
		expression.Value(fmt.Sprintf("ORGANIZATION#%s", o.GetId())),
	).And(
		expression.Key("sk").Equal(
			expression.Value(fmt.Sprintf("ORGANIZATION#%s", o.GetId())),
		),
	)
}

// PrimaryKey returns the primary key for the organization that can be used for
// storing, updating, or deleting the organization in DynamoDB.
func (o *Organization) PrimaryKey() (map[string]types.AttributeValue, error) {
	if o.GetId() == "" {
		return nil, fmt.Errorf("organization id is required to generate primary key")
	}
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("ORGANIZATION#%s", o.GetId())},
		"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("ORGANIZATION#%s", o.GetId())},
	}, nil
}

// Marshal marshals a User into a map of attribute values for storage in DynamoDB.
func (u *User) Marshal() (map[string]types.AttributeValue, error) {
	// Marshal the user into a map of attribute values.
	userItem, err := dynabuf.Marshal(u)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal user: %w", err)
	}

	// Get the concrete type of the user item.
	userItemMap := userItem.(map[string]types.AttributeValue)

	primaryKey, err := u.PrimaryKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}

	// Add the primary key to the user item.
	maps.Copy(userItemMap, primaryKey)

	// Return the user item.
	return userItemMap, nil
}

// KeyCondition returns a key condition builder for the user that can be used
// for querying DynamoDB.
func (u *User) KeyCondition() expression.KeyConditionBuilder {
	return expression.Key("pk").Equal(
		expression.Value(fmt.Sprintf("USER#%s", u.GetId())),
	).And(
		expression.Key("sk").Equal(
			expression.Value(fmt.Sprintf("USER#%s", u.GetId())),
		),
	)
}

// PrimaryKey returns the primary key for the user that can be used for
// storing, updating, or deleting the user in DynamoDB.
func (u *User) PrimaryKey() (map[string]types.AttributeValue, error) {
	if u.GetId() == "" {
		return nil, fmt.Errorf("user id is required to generate primary key")
	}
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", u.GetId())},
		"sk": &types.AttributeValueMemberS{Value: fmt.Sprintf("USER#%s", u.GetId())},
	}, nil
}

// Marshal marshals a project into a map of attribute values for storage in DynamoDB.
func (p *Project) Marshal() (map[string]types.AttributeValue, error) {
	// Marshal the profile into a map of attribute values.
	profileItem, err := dynabuf.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal profile: %w", err)
	}

	// Get the concrete type of the profile item.
	profileItemMap := profileItem.(map[string]types.AttributeValue)

	primaryKey, err := p.PrimaryKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}

	// Add the primary key to the profile item.
	maps.Copy(profileItemMap, primaryKey)

	// Return the profile item.
	return profileItemMap, nil
}

// KeyCondition returns a key condition builder for the profile that can be used
// for querying DynamoDB.
func (p *Project) KeyCondition() expression.KeyConditionBuilder {
	var (
		partitionKey = fmt.Sprintf("ORGANIZATION#%s", p.GetOrganizationId())
		sortKey      = fmt.Sprintf("PROJECT#%s", p.GetId())
	)

	return expression.Key("pk").Equal(
		expression.Value(partitionKey),
	).And(
		expression.Key("sk").Equal(
			expression.Value(sortKey),
		),
	)
}

// PrimaryKey returns the primary key for the profile that can be used for
// storing, updating, or deleting the profile in DynamoDB.
func (p *Project) PrimaryKey() (map[string]types.AttributeValue, error) {
	var (
		partitionKey = fmt.Sprintf("ORGANIZATION#%s", p.GetOrganizationId())
		sortKey      = fmt.Sprintf("PROJECT#%s", p.GetId())
	)

	if p.GetId() == "" {
		return nil, fmt.Errorf("project id is required to generate primary key")
	}
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: partitionKey},
		"sk": &types.AttributeValueMemberS{Value: sortKey},
	}, nil
}

// Marshal marshals a organization member into a map of attribute values for storage in DynamoDB.
func (m *OrganizationMember) Marshal() (map[string]types.AttributeValue, error) {
	// Marshal the organization member into a map of attribute values.
	memberItem, err := dynabuf.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal organization member: %w", err)
	}

	// Get the concrete type of the organization member item.
	memberItemMap := memberItem.(map[string]types.AttributeValue)

	primaryKey, err := m.PrimaryKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %w", err)
	}

	// Add the primary key to the organization member item.
	maps.Copy(memberItemMap, primaryKey)

	// Return the organization member item.
	return memberItemMap, nil
}

// KeyCondition returns a key condition builder for the organization member that can be used
// for querying DynamoDB.
func (m *OrganizationMember) KeyCondition() expression.KeyConditionBuilder {
	return expression.Key("pk").Equal(
		expression.Value(fmt.Sprintf("ORGANIZATION#%s", m.GetOrganizationId())),
	).And(
		expression.Key("sk").Equal(
			expression.Value(fmt.Sprintf("ORGANIZATION_MEMBER#%s", m.GetUserId())),
		),
	)
}

// PrimaryKey returns the primary key for the organization member that can be used for
// storing, updating, or deleting the organization member in DynamoDB.
func (m *OrganizationMember) PrimaryKey() (map[string]types.AttributeValue, error) {
	if m.GetOrganizationId() == "" {
		return nil, fmt.Errorf("organization id is required to generate primary key")
	}
	if m.GetUserId() == "" {
		return nil, fmt.Errorf("user id is required to generate primary key")
	}

	var (
		partitionKey = fmt.Sprintf("ORGANIZATION#%s", m.GetOrganizationId())
		sortKey      = fmt.Sprintf("ORGANIZATION_MEMBER#%s", m.GetUserId())
	)

	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: partitionKey},
		"sk": &types.AttributeValueMemberS{Value: sortKey},
	}, nil
}
