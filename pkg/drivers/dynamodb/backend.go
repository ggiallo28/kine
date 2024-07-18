package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/k3s-io/kine/pkg/server"
)

type Backend struct {
	dynamoDB *dynamodb.DynamoDB
	table    string
}

func New(ctx context.Context, tableName string, region string) (server.Backend, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	dynamoDB := dynamodb.New(sess)

	// Create table if it doesn't exist
	err = createTableIfNotExist(dynamoDB, tableName)
	if err != nil {
		return nil, err
	}

	return &Backend{
		dynamoDB: dynamoDB,
		table:    tableName,
	}, nil
}

func createTableIfNotExist(dynamoDB *dynamodb.DynamoDB, tableName string) error {
	_, err := dynamoDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err == nil {
		return nil
	}

	_, err = dynamoDB.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("SK"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("SK"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})

	return err
}

func (b *Backend) Put(ctx context.Context, key []byte, value []byte) error {
	item := map[string]*dynamodb.AttributeValue{
		"PK": {S: aws.String(fmt.Sprintf("key#%s", key))},
		"SK": {S: aws.String(fmt.Sprintf("meta#%d", time.Now().UnixNano()))},
		"Value": {B: value},
	}

	_, err := b.dynamoDB.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(b.table),
		Item:      item,
	})
	return err
}

func (b *Backend) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	// Scan to find the items in the range and delete them
	start := fmt.Sprintf("key#%s", startKey)
	end := fmt.Sprintf("key#%s", endKey)

	input := &dynamodb.QueryInput{
		TableName:              aws.String(b.table),
		KeyConditionExpression: aws.String("PK BETWEEN :start AND :end"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":start": {S: aws.String(start)},
			":end":   {S: aws.String(end)},
		},
	}

	result, err := b.dynamoDB.QueryWithContext(ctx, input)
	if err != nil {
		return err
	}

	for _, item := range result.Items {
		pk := item["PK"].S
		sk := item["SK"].S
		_, err = b.dynamoDB.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(b.table),
			Key: map[string]*dynamodb.AttributeValue{
				"PK": {S: pk},
				"SK": {S: sk},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *server.KeyValue, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(b.table),
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("key#%s", key))},
			"SK": {S: aws.String(fmt.Sprintf("meta#%d", revision))},
		},
	}

	result, err := b.dynamoDB.GetItemWithContext(ctx, input)
	if err != nil {
		return 0, nil, err
	}

	if result.Item == nil {
		return 0, nil, nil
	}

	var kv server.KeyValue
	err = dynamodbattribute.UnmarshalMap(result.Item, &kv)
	if err != nil {
		return 0, nil, err
	}

	return revision, &kv, nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(b.table),
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String(fmt.Sprintf("key#%s", key))},
			"SK": {S: aws.String(fmt.Sprintf("meta#%d", revision))},
		},
		ReturnValues: aws.String("ALL_OLD"),
	}

	result, err := b.dynamoDB.DeleteItemWithContext(ctx, input)
	if err != nil {
		return 0, nil, false, err
	}

	if result.Attributes == nil {
		return 0, nil, true, nil
	}

	var kv server.KeyValue
	err = dynamodbattribute.UnmarshalMap(result.Attributes, &kv)
	if err != nil {
		return 0, nil, false, err
	}

	return revision, &kv, true, nil
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*server.KeyValue, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(b.table),
		KeyConditionExpression: aws.String("PK = :prefix AND SK >= :startKey"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":prefix":   {S: aws.String(fmt.Sprintf("key#%s", prefix))},
			":startKey": {S: aws.String(fmt.Sprintf("meta#%d", startKey))},
		},
		Limit: aws.Int64(limit),
	}

	result, err := b.dynamoDB.QueryWithContext(ctx, input)
	if err != nil {
		return 0, nil, err
	}

	var kvs []*server.KeyValue
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &kvs)
	if err != nil {
		return 0, nil, err
	}

	return revision, kvs, nil
}

func (b *Backend) Start(ctx context.Context) error {
	// Initialize backend if needed
	return nil
}

func (b *Backend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(b.table),
		KeyConditionExpression: aws.String("PK = :prefix AND SK >= :startKey"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":prefix":   {S: aws.String(fmt.Sprintf("key#%s", prefix))},
			":startKey": {S: aws.String(fmt.Sprintf("meta#%d", startKey))},
		},
		Select: aws.String("COUNT"),
	}

	result, err := b.dynamoDB.QueryWithContext(ctx, input)
	if err != nil {
		return 0, 0, err
	}

	return revision, *result.Count, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	_, err := b.Put(ctx, []byte(key), value)
	if err != nil {
		return 0, nil, false, err
	}

	return revision, &server.KeyValue{
		Key:   key,
		Value: value,
		Lease: lease,
	}, true, nil
}

func (b *Backend) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	// Implement the Watch method if needed
	return server.WatchResult{}
}

func (b *Backend) DbSize(ctx context.Context) (int64, error) {
	// DynamoDB does not provide an easy way to get the size of the table
	return 0, nil
}

func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	// Implement the CurrentRevision method if needed
	return 0, nil
}

func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	// Implement the Compact method if needed
	return revision, nil
}
