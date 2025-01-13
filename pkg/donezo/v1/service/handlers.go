package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/picatz/dynabuf"
	"github.com/picatz/dynabuf/expr"
	v1 "github.com/picatz/go-connect-aws-lambda-dynamodb/pkg/donezo/v1"
)

func decodePageToken(pageToken string) (map[string]types.AttributeValue, error) {
	if pageToken == "" {
		return nil, nil
	}

	decodedToken, err := base64.URLEncoding.DecodeString(pageToken)
	if err != nil {
		return nil, fmt.Errorf("invalid page token: %w", err)
	}

	var exclusiveStartKey map[string]types.AttributeValue
	err = json.Unmarshal(decodedToken, &exclusiveStartKey)
	if err != nil {
		return nil, fmt.Errorf("invalid page token: %w", err)
	}

	return exclusiveStartKey, nil
}

func encodePageToken(exclusiveStartKey map[string]types.AttributeValue) (string, error) {
	if len(exclusiveStartKey) == 0 {
		return "", nil
	}

	encodedKey, err := json.Marshal(exclusiveStartKey)
	if err != nil {
		return "", fmt.Errorf("failed to encode next page token: %w", err)
	}

	return base64.URLEncoding.EncodeToString(encodedKey), nil
}

// CreateTask creates a new task, stores it in DynamoDB, and returns the created task.
func (s *Server) CreateTask(ctx context.Context, req *connect.Request[v1.CreateTaskRequest]) (*connect.Response[v1.CreateTaskResponse], error) {
	task := &v1.Task{
		Id:             uuid.New().String(),
		OrganizationId: req.Msg.GetOrganizationId(),
		ProjectId:      req.Msg.GetProjectId(),
		Title:          req.Msg.GetTitle(),
		Description:    req.Msg.Description,
		Completed:      req.Msg.GetCompleted(),
	}

	taskItem, err := task.Marshal()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to marshal task")
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: Table,
		Item:      taskItem,
	})

	s.logger.DebugContext(ctx, "created task", "task_id", task.Id)

	return connect.NewResponse(&v1.CreateTaskResponse{
		Task: task,
	}), nil
}

// DeleteTask deletes a task from DynamoDB based on the provided task ID.
func (s *Server) DeleteTask(ctx context.Context, req *connect.Request[v1.DeleteTaskRequest]) (*connect.Response[v1.DeleteTaskResponse], error) {
	task := &v1.Task{
		Id:             req.Msg.GetId(),
		OrganizationId: req.Msg.GetOrganizationId(),
		ProjectId:      req.Msg.GetProjectId(),
	}

	primaryKey, err := task.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	_, err = s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to delete task")
	}

	s.logger.DebugContext(ctx, "deleted task", "task_id", task.Id)

	return connect.NewResponse(&v1.DeleteTaskResponse{}), nil
}

// GetTask retrieves a task from DynamoDB based on the provided task ID.
func (s *Server) GetTask(ctx context.Context, req *connect.Request[v1.GetTaskRequest]) (*connect.Response[v1.GetTaskResponse], error) {
	task := &v1.Task{
		Id:             req.Msg.GetId(),
		OrganizationId: req.Msg.GetOrganizationId(),
		ProjectId:      req.Msg.GetProjectId(),
	}

	primaryKey, err := task.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to get task")
	}

	if resp.Item == nil {
		return nil, s.errorf(ctx, connect.CodeNotFound, fmt.Errorf("unable to find task with ID %q", task.Id), "task not found")
	}

	err = dynabuf.Unmarshal(resp.Item, task)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal task")
	}

	s.logger.DebugContext(ctx, "got task", "task_id", task.Id)

	return connect.NewResponse(&v1.GetTaskResponse{
		Task: task,
	}), nil
}

// ListTasks retrieves a list of tasks from DynamoDB with optional pagination and filtering.
func (s *Server) ListTasks(ctx context.Context, req *connect.Request[v1.ListTasksRequest]) (*connect.Response[v1.ListTasksResponse], error) {
	// Set default page size if not provided
	pageSize := req.Msg.GetPageSize()
	if pageSize == 0 {
		pageSize = 10
	}

	// Setup page token for pagination if nessessary
	var (
		exclusiveStartKey map[string]types.AttributeValue
		err               error
	)
	if pageToken := req.Msg.GetPageToken(); pageToken != "" {
		exclusiveStartKey, err = decodePageToken(pageToken)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to decode page token")
		}
	}

	keyCond := expression.Key("pk").Equal(
		expression.Value(fmt.Sprintf("ORGANIZATION#%s#PROJECT#%s", req.Msg.GetOrganizationId(), req.Msg.GetProjectId())),
	).And(
		expression.Key("sk").BeginsWith("TASK#"),
	)

	builder := expression.NewBuilder().WithKeyCondition(keyCond)

	env, err := expr.NewEnv(expr.MessageFieldVariables(&v1.Task{})...)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to create CEL environment")
	}

	if req.Msg.Filter != nil {
		filterAst, issues := env.Compile(req.Msg.GetFilter())
		if issues != nil && issues.Err() != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, issues.Err(), "failed to compile CEL expression")
		}

		filterCond, err := expr.Filter(filterAst)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to create filter expression")
		}

		builder = builder.WithFilter(filterCond)
	}

	queryExpr, err := builder.Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build query expression %s", err)
	}

	// keyCondExpr, err := expression.NewBuilder().WithKeyCondition(keyCond).Build()
	// if err != nil {
	// 	return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build key condition expression")
	// }

	queryInput := &dynamodb.QueryInput{
		TableName:                 Table,
		Limit:                     aws.Int32(pageSize),
		ExclusiveStartKey:         exclusiveStartKey,
		KeyConditionExpression:    queryExpr.KeyCondition(),
		FilterExpression:          queryExpr.Filter(),
		ExpressionAttributeNames:  queryExpr.Names(),
		ExpressionAttributeValues: queryExpr.Values(),
	}
	resp, err := s.db.Query(ctx, queryInput)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to list tasks")
	}

	tasks := make([]*v1.Task, 0, len(resp.Items))
	err = dynabuf.Unmarshal(resp.Items, &tasks)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal tasks")
	}

	// Prepare the next page token if there are more items
	var nextPageTokenResp *string
	if resp.LastEvaluatedKey != nil {
		nextPageToken, err := encodePageToken(resp.LastEvaluatedKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to encode next page token")
		}
		nextPageTokenResp = &nextPageToken
	}

	s.logger.DebugContext(ctx, "listed tasks", "num_tasks", len(tasks), "page_size", pageSize)

	// Construct and return the response
	return connect.NewResponse(&v1.ListTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextPageTokenResp,
	}), nil
}

// UpdateTask updates a task in DynamoDB based on the provided task ID and field mask.
func (s *Server) UpdateTask(ctx context.Context, req *connect.Request[v1.UpdateTaskRequest]) (*connect.Response[v1.UpdateTaskResponse], error) {
	// Extract the task and update mask from the request
	updatedTask := req.Msg.GetTask()
	updateMask := req.Msg.GetUpdateMask()

	// Prepare the update expression based on the field mask
	var updateExpr expression.UpdateBuilder
	for _, path := range updateMask.Paths {
		switch path {
		case "title":
			updateExpr = updateExpr.Set(expression.Name("title"), expression.Value(updatedTask.Title))
		case "description":
			updateExpr = updateExpr.Set(expression.Name("description"), expression.Value(updatedTask.Description))
		case "completed":
			updateExpr = updateExpr.Set(expression.Name("completed"), expression.Value(updatedTask.Completed))
		default:
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("unsupported field in update mask: %s", path), "unsupported field in update mask")
		}
	}

	// Build the DynamoDB expression
	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build update expression")
	}

	primaryKey, err := updatedTask.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	// Perform the update operation
	input := &dynamodb.UpdateItemInput{
		TableName:                 Table,
		Key:                       primaryKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueNone,
	}

	_, err = s.db.UpdateItem(ctx, input)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to update task")
	}

	s.logger.DebugContext(ctx, "updated task", "task_id", updatedTask.Id)

	// Return the updated task in the response
	return connect.NewResponse(&v1.UpdateTaskResponse{}), nil
}

func (s *Server) CreateOrganization(ctx context.Context, req *connect.Request[v1.CreateOrganizationRequest]) (*connect.Response[v1.CreateOrganizationResponse], error) {
	org := &v1.Organization{
		Id:   uuid.New().String(),
		Name: req.Msg.GetName(),
	}

	orgItem, err := org.Marshal()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to marshal organization")
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: Table,
		Item:      orgItem,
	})

	s.logger.DebugContext(ctx, "created organization", "organization_id", org.Id)
	return connect.NewResponse(&v1.CreateOrganizationResponse{
		Organization: org,
	}), nil
}

func (s *Server) DeleteOrganization(ctx context.Context, req *connect.Request[v1.DeleteOrganizationRequest]) (*connect.Response[v1.DeleteOrganizationResponse], error) {
	org := &v1.Organization{
		Id: req.Msg.GetId(),
	}

	primaryKey, err := org.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	_, err = s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to delete organization")
	}

	s.logger.DebugContext(ctx, "deleted organization", "organization_id", org.Id)

	return connect.NewResponse(&v1.DeleteOrganizationResponse{}), nil
}

func (s *Server) GetOrganization(ctx context.Context, req *connect.Request[v1.GetOrganizationRequest]) (*connect.Response[v1.GetOrganizationResponse], error) {
	org := &v1.Organization{
		Id: req.Msg.GetId(),
	}

	primaryKey, err := org.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to get organization")
	}

	if resp.Item == nil {
		return nil, s.errorf(ctx, connect.CodeNotFound, fmt.Errorf("unable to find organization with ID %q", org.Id), "organization not found")
	}

	err = dynabuf.Unmarshal(resp.Item, org)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal organization")
	}

	s.logger.DebugContext(ctx, "got organization", "organization_id", org.Id)

	return connect.NewResponse(&v1.GetOrganizationResponse{
		Organization: org,
	}), nil
}

func (s *Server) ListOrganizations(ctx context.Context, req *connect.Request[v1.ListOrganizationsRequest]) (*connect.Response[v1.ListOrganizationsResponse], error) {
	// Set default page size if not provided
	pageSize := req.Msg.GetPageSize()
	if pageSize == 0 {
		pageSize = 10
	}

	// Setup page token for pagination if nessessary
	var (
		exclusiveStartKey map[string]types.AttributeValue
		err               error
	)
	if pageToken := req.Msg.GetPageToken(); pageToken != "" {
		exclusiveStartKey, err = decodePageToken(pageToken)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to decode page token")
		}
	}

	scanInput := &dynamodb.ScanInput{
		TableName:         Table,
		Limit:             aws.Int32(pageSize),
		ExclusiveStartKey: exclusiveStartKey,
	}
	resp, err := s.db.Scan(ctx, scanInput)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to list organizations")
	}

	orgs := make([]*v1.Organization, 0, len(resp.Items))
	err = dynabuf.Unmarshal(resp.Items, &orgs)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal organizations")
	}

	var nextPageTokenResp *string

	if resp.LastEvaluatedKey != nil {
		nextPageToken, err := encodePageToken(resp.LastEvaluatedKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to encode next page token")
		}
		nextPageTokenResp = &nextPageToken
	}

	s.logger.DebugContext(ctx, "listed organizations", "num_organizations", len(orgs), "page_size", pageSize)

	return connect.NewResponse(&v1.ListOrganizationsResponse{
		Organizations: orgs,
		NextPageToken: nextPageTokenResp,
	}), nil
}

func (s *Server) UpdateOrganization(ctx context.Context, req *connect.Request[v1.UpdateOrganizationRequest]) (*connect.Response[v1.UpdateOrganizationResponse], error) {
	// Extract the organization and update mask from the request
	updatedOrg := req.Msg.GetOrganization()
	updateMask := req.Msg.GetUpdateMask()

	// Prepare the update expression based on the field mask
	var updateExpr expression.UpdateBuilder
	for _, path := range updateMask.Paths {
		switch path {
		case "name":
			updateExpr = updateExpr.Set(expression.Name("name"), expression.Value(updatedOrg.Name))
		default:
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("unsupported field in update mask: %s", path), "unsupported field in update mask")
		}
	}

	// Build the DynamoDB expression
	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build update expression")
	}

	primaryKey, err := updatedOrg.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	// Perform the update operation
	input := &dynamodb.UpdateItemInput{
		TableName:                 Table,
		Key:                       primaryKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueNone,
	}

	_, err = s.db.UpdateItem(ctx, input)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to update organization")
	}

	s.logger.DebugContext(ctx, "updated organization", "organization_id", updatedOrg.Id)

	return connect.NewResponse(&v1.UpdateOrganizationResponse{}), nil
}

/*
func (donezov1connect.UnimplementedOrganizationsServiceHandler) GetOrganizationMember(context.Context, *connect.Request[donezov1.GetOrganizationMemberRequest]) (*connect.Response[donezov1.GetOrganizationMemberResponse], error)
func (donezov1connect.UnimplementedOrganizationsServiceHandler) ListOrganizationMembers(context.Context, *connect.Request[donezov1.ListOrganizationMembersRequest]) (*connect.Response[donezov1.ListOrganizationMembersResponse], error)
func (donezov1connect.UnimplementedOrganizationsServiceHandler) ListOrganizations(context.Context, *connect.Request[donezov1.ListOrganizationsRequest]) (*connect.Response[donezov1.ListOrganizationsResponse], error)
func (donezov1connect.UnimplementedOrganizationsServiceHandler) UpdateOrganization(context.Context, *connect.Request[donezov1.UpdateOrganizationRequest]) (*connect.Response[donezov1.UpdateOrganizationResponse], error)
func (donezov1connect.UnimplementedOrganizationsServiceHandler) UpdateOrganizationMember(context.Context, *connect.Request[donezov1.UpdateOrganizationMemberRequest]) (*connect.Response[donezov1.UpdateOrganizationMemberResponse], error)
*/

func (s *Server) CreateOrganizationMember(ctx context.Context, req *connect.Request[v1.CreateOrganizationMemberRequest]) (*connect.Response[v1.CreateOrganizationMemberResponse], error) {
	member := &v1.OrganizationMember{
		OrganizationId: req.Msg.GetOrganizationId(),
		UserId:         req.Msg.GetUserId(),
		Role:           req.Msg.GetRole(),
	}

	memberItem, err := member.Marshal()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to marshal organization member")
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: Table,
		Item:      memberItem,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to create organization member")
	}

	s.logger.DebugContext(ctx, "created organization member", "organization_id", member.OrganizationId, "user_id", member.UserId)

	return connect.NewResponse(&v1.CreateOrganizationMemberResponse{
		OrganizationMember: member,
	}), nil
}

func (s *Server) DeleteOrganizationMember(ctx context.Context, req *connect.Request[v1.DeleteOrganizationMemberRequest]) (*connect.Response[v1.DeleteOrganizationMemberResponse], error) {
	member := &v1.OrganizationMember{
		OrganizationId: req.Msg.GetOrganizationId(),
		UserId:         req.Msg.GetUserId(),
	}

	primaryKey, err := member.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	_, err = s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to delete organization member")
	}

	s.logger.DebugContext(ctx, "deleted organization member", "organization_id", member.OrganizationId, "user_id", member.UserId)

	return connect.NewResponse(&v1.DeleteOrganizationMemberResponse{}), nil
}

func (s *Server) GetOrganizationMember(ctx context.Context, req *connect.Request[v1.GetOrganizationMemberRequest]) (*connect.Response[v1.GetOrganizationMemberResponse], error) {
	member := &v1.OrganizationMember{
		OrganizationId: req.Msg.GetOrganizationId(),
		UserId:         req.Msg.GetUserId(),
	}

	primaryKey, err := member.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to get organization member")
	}

	if resp.Item == nil {
		return nil, s.errorf(ctx, connect.CodeNotFound, fmt.Errorf("unable to find organization %q member with ID %q", member.OrganizationId, member.UserId), "organization member not found")
	}

	err = dynabuf.Unmarshal(resp.Item, member)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal organization member")
	}

	s.logger.DebugContext(ctx, "got organization member", "organization_id", member.OrganizationId, "user_id", member.UserId)

	return connect.NewResponse(&v1.GetOrganizationMemberResponse{
		OrganizationMember: member,
	}), nil
}

func (s *Server) ListOrganizationMembers(ctx context.Context, req *connect.Request[v1.ListOrganizationMembersRequest]) (*connect.Response[v1.ListOrganizationMembersResponse], error) {
	// Set default page size if not provided
	pageSize := req.Msg.GetPageSize()
	if pageSize == 0 {
		pageSize = 10
	}

	// Setup page token for pagination if nessessary
	var (
		exclusiveStartKey map[string]types.AttributeValue
		err               error
	)

	if pageToken := req.Msg.GetPageToken(); pageToken != "" {
		exclusiveStartKey, err = decodePageToken(pageToken)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to decode page token")
		}
	}

	scanInput := &dynamodb.ScanInput{
		TableName:         Table,
		Limit:             aws.Int32(pageSize),
		ExclusiveStartKey: exclusiveStartKey,
	}

	resp, err := s.db.Scan(ctx, scanInput)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to list organization members")
	}

	members := make([]*v1.OrganizationMember, 0, len(resp.Items))

	err = dynabuf.Unmarshal(resp.Items, &members)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal organization members")
	}

	var nextPageTokenResp *string

	if resp.LastEvaluatedKey != nil {
		nextPageToken, err := encodePageToken(resp.LastEvaluatedKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to encode next page token")
		}
		nextPageTokenResp = &nextPageToken
	}

	s.logger.DebugContext(ctx, "listed organization members", "num_organization_members", len(members), "page_size", pageSize)

	return connect.NewResponse(&v1.ListOrganizationMembersResponse{
		OrganizationMembers: members,
		NextPageToken:       nextPageTokenResp,
	}), nil
}

func (s *Server) UpdateOrganizationMember(ctx context.Context, req *connect.Request[v1.UpdateOrganizationMemberRequest]) (*connect.Response[v1.UpdateOrganizationMemberResponse], error) {
	// Extract the organization member and update mask from the request
	updatedMember := req.Msg.GetOrganizationMember()
	updateMask := req.Msg.GetUpdateMask()

	// Prepare the update expression based on the field mask
	var updateExpr expression.UpdateBuilder
	for _, path := range updateMask.Paths {
		switch path {
		case "role":
			updateExpr = updateExpr.Set(expression.Name("role"), expression.Value(updatedMember.Role))
		default:
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("unsupported field in update mask: %s", path), "unsupported field in update mask")
		}
	}

	// Build the DynamoDB expression
	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build update expression")
	}

	primaryKey, err := updatedMember.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	// Perform the update operation
	input := &dynamodb.UpdateItemInput{
		TableName:                 Table,
		Key:                       primaryKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueNone,
	}

	result, err := s.db.UpdateItem(ctx, input)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to update organization member")
	}

	err = dynabuf.Unmarshal(result.Attributes, updatedMember)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal updated organization member")
	}

	s.logger.DebugContext(ctx, "updated organization member", "organization_id", updatedMember.OrganizationId, "user_id", updatedMember.UserId)

	return connect.NewResponse(&v1.UpdateOrganizationMemberResponse{}), nil
}

func (s *Server) CreateUser(ctx context.Context, req *connect.Request[v1.CreateUserRequest]) (*connect.Response[v1.CreateUserResponse], error) {
	user := &v1.User{
		Id:   uuid.New().String(),
		Name: req.Msg.GetName(),
	}

	userItem, err := user.Marshal()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to marshal user")
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: Table,
		Item:      userItem,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to create user")
	}

	s.logger.DebugContext(ctx, "created user", "user_id", user.Id)

	return connect.NewResponse(&v1.CreateUserResponse{
		User: user,
	}), nil
}

func (s *Server) DeleteUser(ctx context.Context, req *connect.Request[v1.DeleteUserRequest]) (*connect.Response[v1.DeleteUserResponse], error) {
	user := &v1.User{
		Id: req.Msg.GetId(),
	}

	primaryKey, err := user.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	_, err = s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to delete user")
	}

	s.logger.DebugContext(ctx, "deleted user", "user_id", user.Id)

	return connect.NewResponse(&v1.DeleteUserResponse{}), nil
}

func (s *Server) GetUser(ctx context.Context, req *connect.Request[v1.GetUserRequest]) (*connect.Response[v1.GetUserResponse], error) {
	user := &v1.User{
		Id: req.Msg.GetId(),
	}

	primaryKey, err := user.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to get user")
	}

	if resp.Item == nil {
		return nil, s.errorf(ctx, connect.CodeNotFound, fmt.Errorf("unable to find user with ID %q", user.Id), "user not found")
	}

	err = dynabuf.Unmarshal(resp.Item, user)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal user")
	}

	s.logger.DebugContext(ctx, "got user", "user_id", user.Id)

	return connect.NewResponse(&v1.GetUserResponse{
		User: user,
	}), nil
}

func (s *Server) ListUsers(ctx context.Context, req *connect.Request[v1.ListUsersRequest]) (*connect.Response[v1.ListUsersResponse], error) {
	// Set default page size if not provided
	pageSize := req.Msg.GetPageSize()
	if pageSize == 0 {
		pageSize = 10
	}

	// Setup page token for pagination if nessessary
	var (
		exclusiveStartKey map[string]types.AttributeValue
		err               error
	)

	if pageToken := req.Msg.GetPageToken(); pageToken != "" {
		exclusiveStartKey, err = decodePageToken(pageToken)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to decode page token")
		}
	}

	scanInput := &dynamodb.ScanInput{
		TableName:         Table,
		Limit:             aws.Int32(pageSize),
		ExclusiveStartKey: exclusiveStartKey,
	}

	resp, err := s.db.Scan(ctx, scanInput)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to list users")
	}

	users := make([]*v1.User, 0, len(resp.Items))

	err = dynabuf.Unmarshal(resp.Items, &users)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal users")
	}

	var nextPageTokenResp *string
	if resp.LastEvaluatedKey != nil {
		nextPageToken, err := encodePageToken(resp.LastEvaluatedKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to encode next page token")
		}
		nextPageTokenResp = &nextPageToken
	}

	s.logger.DebugContext(ctx, "listed users", "num_users", len(users), "page_size", pageSize)

	return connect.NewResponse(&v1.ListUsersResponse{
		Users:         users,
		NextPageToken: nextPageTokenResp,
	}), nil
}

func (s *Server) UpdateUser(ctx context.Context, req *connect.Request[v1.UpdateUserRequest]) (*connect.Response[v1.UpdateUserResponse], error) {
	// Extract the user and update mask from the request
	updatedUser := req.Msg.GetUser()
	updateMask := req.Msg.GetUpdateMask()

	// Prepare the update expression based on the field mask
	var updateExpr expression.UpdateBuilder
	for _, path := range updateMask.Paths {
		switch path {
		case "name":
			updateExpr = updateExpr.Set(expression.Name("name"), expression.Value(updatedUser.Name))
		default:
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("unsupported field in update mask: %s", path), "unsupported field in update mask")
		}
	}

	// Build the DynamoDB expression
	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build update expression")
	}

	primaryKey, err := updatedUser.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	// Perform the update operation
	input := &dynamodb.UpdateItemInput{
		TableName:                 Table,
		Key:                       primaryKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueNone,
	}

	_, err = s.db.UpdateItem(ctx, input)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to update user")
	}

	s.logger.DebugContext(ctx, "updated user", "user_id", updatedUser.Id)

	return connect.NewResponse(&v1.UpdateUserResponse{}), nil
}

func (s *Server) CreateProject(ctx context.Context, req *connect.Request[v1.CreateProjectRequest]) (*connect.Response[v1.CreateProjectResponse], error) {
	project := &v1.Project{
		Id:             uuid.New().String(),
		OrganizationId: req.Msg.GetOrganizationId(),
		Name:           req.Msg.GetName(),
	}

	projectItem, err := project.Marshal()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to marshal project")
	}

	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: Table,
		Item:      projectItem,
	})

	s.logger.DebugContext(ctx, "created project", "project_id", project.Id)

	return connect.NewResponse(&v1.CreateProjectResponse{
		Project: project,
	}), nil
}

func (s *Server) DeleteProject(ctx context.Context, req *connect.Request[v1.DeleteProjectRequest]) (*connect.Response[v1.DeleteProjectResponse], error) {
	project := &v1.Project{
		Id:             req.Msg.GetId(),
		OrganizationId: req.Msg.GetOrganizationId(),
	}

	primaryKey, err := project.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	_, err = s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to delete project")
	}

	s.logger.DebugContext(ctx, "deleted project", "project_id", project.Id)

	return connect.NewResponse(&v1.DeleteProjectResponse{}), nil
}

func (s *Server) GetProject(ctx context.Context, req *connect.Request[v1.GetProjectRequest]) (*connect.Response[v1.GetProjectResponse], error) {
	project := &v1.Project{
		Id:             req.Msg.GetId(),
		OrganizationId: req.Msg.GetOrganizationId(),
	}

	primaryKey, err := project.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	resp, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: Table,
		Key:       primaryKey,
	})
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to get project")
	}

	if resp.Item == nil {
		return nil, s.errorf(ctx, connect.CodeNotFound, fmt.Errorf("unable to find project with ID %q", project.Id), "project not found")
	}

	err = dynabuf.Unmarshal(resp.Item, project)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal project")
	}

	s.logger.DebugContext(ctx, "got project", "project_id", project.Id)

	return connect.NewResponse(&v1.GetProjectResponse{
		Project: project,
	}), nil
}

func (s *Server) ListProjects(ctx context.Context, req *connect.Request[v1.ListProjectsRequest]) (*connect.Response[v1.ListProjectsResponse], error) {
	// Set default page size if not provided
	pageSize := req.Msg.GetPageSize()
	if pageSize == 0 {
		pageSize = 10
	}

	// Setup page token for pagination if nessessary
	var (
		exclusiveStartKey map[string]types.AttributeValue
		err               error
	)

	if pageToken := req.Msg.GetPageToken(); pageToken != "" {
		exclusiveStartKey, err = decodePageToken(pageToken)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to decode page token")
		}
	}

	scanInput := &dynamodb.ScanInput{
		TableName:         Table,
		Limit:             aws.Int32(pageSize),
		ExclusiveStartKey: exclusiveStartKey,
	}

	resp, err := s.db.Scan(ctx, scanInput)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to list projects")
	}

	projects := make([]*v1.Project, 0, len(resp.Items))
	err = dynabuf.Unmarshal(resp.Items, &projects)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to unmarshal projects")
	}

	var nextPageTokenResp *string
	if resp.LastEvaluatedKey != nil {
		nextPageToken, err := encodePageToken(resp.LastEvaluatedKey)
		if err != nil {
			return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to encode next page token")
		}
		nextPageTokenResp = &nextPageToken
	}

	s.logger.DebugContext(ctx, "listed projects", "num_projects", len(projects), "page_size", pageSize)

	return connect.NewResponse(&v1.ListProjectsResponse{
		Projects:      projects,
		NextPageToken: nextPageTokenResp,
	}), nil
}

func (s *Server) UpdateProject(ctx context.Context, req *connect.Request[v1.UpdateProjectRequest]) (*connect.Response[v1.UpdateProjectResponse], error) {
	// Extract the project and update mask from the request
	updatedProject := req.Msg.GetProject()
	updateMask := req.Msg.GetUpdateMask()

	// Prepare the update expression based on the field mask
	var updateExpr expression.UpdateBuilder
	for _, path := range updateMask.Paths {
		switch path {
		case "name":
			updateExpr = updateExpr.Set(expression.Name("name"), expression.Value(updatedProject.Name))
		default:
			return nil, s.errorf(ctx, connect.CodeInvalidArgument, fmt.Errorf("unsupported field in update mask: %s", path), "unsupported field in update mask")
		}
	}

	// Build the DynamoDB expression
	expr, err := expression.NewBuilder().WithUpdate(updateExpr).Build()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to build update expression")
	}

	primaryKey, err := updatedProject.PrimaryKey()
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInvalidArgument, err, "failed to get primary key")
	}

	// Perform the update operation
	input := &dynamodb.UpdateItemInput{
		TableName:                 Table,
		Key:                       primaryKey,
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              types.ReturnValueNone,
	}

	_, err = s.db.UpdateItem(ctx, input)
	if err != nil {
		return nil, s.errorf(ctx, connect.CodeInternal, err, "failed to update project")
	}

	s.logger.DebugContext(ctx, "updated project", "project_id", updatedProject.Id)

	return connect.NewResponse(&v1.UpdateProjectResponse{}), nil
}
