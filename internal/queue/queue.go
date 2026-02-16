package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Queue interface {
	Send(ctx context.Context, body string) error
}

type SQSQueue struct {
	sqs      *sqs.Client
	queueURL string
}

func NewSQSQueue(sqsClient *sqs.Client, queueURL string) *SQSQueue {
	return &SQSQueue{sqs: sqsClient, queueURL: queueURL}
}

func (q *SQSQueue) Send(ctx context.Context, body string) error {
	_, err := q.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &q.queueURL,
		MessageBody: &body,
	})
	return err
}
