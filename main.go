package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/mdamonte/asyncjobs/internal/models"
	"github.com/mdamonte/asyncjobs/internal/queue"
	"github.com/mdamonte/asyncjobs/internal/store"
)

type createTaskReq struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type createTaskResp struct {
	TaskID string            `json:"taskId"`
	Status models.TaskStatus `json:"status"`
}

func main() {
	awsEndpoint := mustEnv("AWS_ENDPOINT") // http://localhost:4566
	region := envDefault("AWS_REGION", "us-east-1")
	tableName := mustEnv("TASKS_TABLE")
	queueURL := mustEnv("TASKS_QUEUE_URL")

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: awsEndpoint, SigningRegion: region, HostnameImmutable: true}, nil
			},
		)),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	taskStore := store.NewDynamoTaskStore(db, tableName)
	taskQueue := queue.NewSQSQueue(sqsClient, queueURL)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); w.Write([]byte("ok")) })
	mux.HandleFunc("/tasks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var req createTaskReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", 400)
			return
		}
		if req.Type == "" || len(req.Payload) == 0 {
			http.Error(w, "type and payload required", 400)
			return
		}

		taskID := newID()
		now := time.Now().UTC()

		t := models.Task{
			TaskID:    taskID,
			Type:      req.Type,
			Status:    models.StatusPending,
			Payload:   string(req.Payload),
			Attempts:  0,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := taskStore.PutTask(r.Context(), t); err != nil {
			http.Error(w, "db error", 500)
			return
		}
		if err := taskQueue.Send(r.Context(), taskID); err != nil {
			http.Error(w, "queue error", 500)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(createTaskResp{TaskID: taskID, Status: models.StatusPending})
	})

	mux.HandleFunc("/tasks/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", 405)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/tasks/")
		if id == "" {
			http.Error(w, "missing id", 400)
			return
		}
		t, err := taskStore.GetTask(r.Context(), id)
		if err != nil {
			http.Error(w, "not found", 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(t)
	})

	addr := envDefault("API_ADDR", ":8080")
	log.Printf("api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("missing env %s", k)
	}
	return v
}
func envDefault(k, d string) string {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	return v
}
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
