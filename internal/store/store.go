package store

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/mdamonte/asyncjobs/internal/models"
)

type TaskStore interface {
	PutTask(ctx context.Context, t models.Task) error
	GetTask(ctx context.Context, taskID string) (models.Task, error)
	MarkProcessing(ctx context.Context, taskID string) (bool, error)
	MarkSucceeded(ctx context.Context, taskID, result string, attempts int) error
	MarkFailed(ctx context.Context, taskID, errMsg string, attempts int) error
}

type DynamoTaskStore struct {
	db        *dynamodb.Client
	tableName string
}

func NewDynamoTaskStore(db *dynamodb.Client, tableName string) *DynamoTaskStore {
	return &DynamoTaskStore{db: db, tableName: tableName}
}

func (s *DynamoTaskStore) PutTask(ctx context.Context, t models.Task) error {
	item, err := attributevalue.MarshalMap(t)
	if err != nil {
		return err
	}
	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.tableName,
		Item:      item,
	})
	return err
}

func (s *DynamoTaskStore) GetTask(ctx context.Context, taskID string) (models.Task, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"TaskID": taskID})
	if err != nil {
		return models.Task{}, err
	}
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.tableName,
		Key:       key,
	})
	if err != nil {
		return models.Task{}, err
	}
	if out.Item == nil {
		return models.Task{}, errors.New("not found")
	}
	var t models.Task
	if err := attributevalue.UnmarshalMap(out.Item, &t); err != nil {
		return models.Task{}, err
	}
	return t, nil
}

// MarkProcessing hace transición PENDING -> PROCESSING de forma condicional.
// Devuelve ok=false si ya no estaba en PENDING.
func (s *DynamoTaskStore) MarkProcessing(ctx context.Context, taskID string) (bool, error) {
	now := time.Now().UTC()
	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.tableName,
		Key: map[string]types.AttributeValue{
			"TaskID": &types.AttributeValueMemberS{Value: taskID},
		},
		UpdateExpression:         strPtr("SET #S = :processing, UpdatedAt = :now ADD Attempts :one"),
		ConditionExpression:      strPtr("#S = :pending"),
		ExpressionAttributeNames: map[string]string{"#S": "Status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":processing": &types.AttributeValueMemberS{Value: string(models.StatusProcessing)},
			":pending":    &types.AttributeValueMemberS{Value: string(models.StatusPending)},
			":now":        &types.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
			":one":        &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		var cfe *types.ConditionalCheckFailedException
		if errors.As(err, &cfe) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *DynamoTaskStore) MarkSucceeded(ctx context.Context, taskID, result string, attempts int) error {
	now := time.Now().UTC()
	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.tableName,
		Key: map[string]types.AttributeValue{
			"TaskID": &types.AttributeValueMemberS{Value: taskID},
		},
		UpdateExpression: strPtr("SET #S = :s, Result = :r, #E = :e, UpdatedAt = :now"),
		ExpressionAttributeNames: map[string]string{
			"#S": "Status",
			"#E": "Error",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s":   &types.AttributeValueMemberS{Value: string(models.StatusSucceeded)},
			":r":   &types.AttributeValueMemberS{Value: result},
			":e":   &types.AttributeValueMemberS{Value: ""},
			":now": &types.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
		},
	})
	return err
}

func (s *DynamoTaskStore) MarkFailed(ctx context.Context, taskID, errMsg string, attempts int) error {
	now := time.Now().UTC()
	_, err := s.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.tableName,
		Key: map[string]types.AttributeValue{
			"TaskID": &types.AttributeValueMemberS{Value: taskID},
		},
		UpdateExpression: strPtr("SET #S = :s, #E = :e, UpdatedAt = :now"),
		ExpressionAttributeNames: map[string]string{
			"#S": "Status",
			"#E": "Error",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s":   &types.AttributeValueMemberS{Value: string(models.StatusFailed)},
			":e":   &types.AttributeValueMemberS{Value: errMsg},
			":now": &types.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
		},
	})
	return err
}

func strPtr(s string) *string { return &s }
