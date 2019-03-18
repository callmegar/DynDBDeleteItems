package main

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"fmt"
)

// Item has the key and sort key for a table, change those to match the table you want to empty
type Item struct {
	KeyID     string `json:"keyId"`
	SortKeyID string `json:"sortKeyId"`
}

func main() {
	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	proj := expression.NamesList(expression.Name("keyId"), expression.Name("sortKeyId"))
	expr, err := expression.NewBuilder().WithProjection(proj).Build()

	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String("tableName"),
	}

	numItems := 0
	// Make the first DynamoDB Query API call
	result, err := svc.Scan(params)

	if err != nil {
		fmt.Println("Query API call failed:")
		fmt.Println((err.Error()))
		os.Exit(1)
	}

	for {
		for _, i := range result.Items {
			input := &dynamodb.DeleteItemInput{
				Key:       i,
				TableName: aws.String("tableName"),
			}

			_, err = svc.DeleteItem(input)
			if err != nil {
				fmt.Println("Got error calling DeleteItem")
				fmt.Println(err.Error())
				return
			}

			numItems++
		}
    fmt.Println("Deleted", numItems)
    // Check if this is the last page
		if result.LastEvaluatedKey == nil {
			break
		}
    
    // Read the next page
		params.ExclusiveStartKey = result.LastEvaluatedKey
		result, err = svc.Scan(params)

		if err != nil {
			fmt.Println("Query API call failed:")
			fmt.Println((err.Error()))
			os.Exit(1)
		}
	}
}
