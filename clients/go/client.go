// Package client provides a Go HTTP client for the durable-execution API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
)

// Client is an HTTP client for the durable-execution API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// New creates a Client targeting the given base URL (e.g. "http://localhost:8080").
func New(baseURL string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		HTTPClient: http.DefaultClient,
	}
}

// Error is returned when the server responds with a non-2xx status.
type Error struct {
	StatusCode int
	Body       apiv1.ErrorResponse
}

func (e *Error) Error() string {
	if e.Body.Error != "" {
		return fmt.Sprintf("%d: %s", e.StatusCode, e.Body.Error)
	}
	return fmt.Sprintf("unexpected status %d", e.StatusCode)
}

func (c *Client) do(ctx context.Context, method, path string, body, result any) error {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		apiErr := &Error{StatusCode: resp.StatusCode}
		json.Unmarshal(respBody, &apiErr.Body)
		return apiErr
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// Queues

func (c *Client) CreateQueue(ctx context.Context, req apiv1.CreateQueueRequest) (*apiv1.CreateQueueResponse, error) {
	var resp apiv1.CreateQueueResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/queues", req, &resp)
	return &resp, err
}

func (c *Client) ListQueues(ctx context.Context) (*apiv1.ListQueuesResponse, error) {
	var resp apiv1.ListQueuesResponse
	err := c.do(ctx, http.MethodGet, "/api/v1/queues", nil, &resp)
	return &resp, err
}

func (c *Client) DeleteQueue(ctx context.Context, queue string) error {
	return c.do(ctx, http.MethodDelete, "/api/v1/queues/"+queue, nil, nil)
}

func (c *Client) GetQueueStats(ctx context.Context, queue string) (*apiv1.QueueStatsResponse, error) {
	var resp apiv1.QueueStatsResponse
	err := c.do(ctx, http.MethodGet, "/api/v1/queues/"+queue+"/stats", nil, &resp)
	return &resp, err
}

// Tasks

func (c *Client) CreateTask(ctx context.Context, queue string, req apiv1.CreateTaskRequest) (*apiv1.CreateTaskResponse, error) {
	var resp apiv1.CreateTaskResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/queues/"+queue+"/tasks", req, &resp)
	return &resp, err
}

func (c *Client) ClaimTasks(ctx context.Context, queue string, req apiv1.ClaimTasksRequest) (*apiv1.ClaimTasksResponse, error) {
	var resp apiv1.ClaimTasksResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/queues/"+queue+"/tasks/claim", req, &resp)
	return &resp, err
}

func (c *Client) ListTasks(ctx context.Context, queueName, status, taskName, cursor *string, limit int32) (*apiv1.ListTasksResponse, error) {
	v := url.Values{}
	if queueName != nil {
		v.Set("queue_name", *queueName)
	}
	if status != nil {
		v.Set("status", *status)
	}
	if taskName != nil {
		v.Set("task_name", *taskName)
	}
	if cursor != nil {
		v.Set("cursor", *cursor)
	}
	if limit > 0 {
		v.Set("limit", fmt.Sprintf("%d", limit))
	}

	path := "/api/v1/tasks"
	if encoded := v.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var resp apiv1.ListTasksResponse
	err := c.do(ctx, http.MethodGet, path, nil, &resp)
	return &resp, err
}

// Runs

func (c *Client) ListRuns(ctx context.Context, taskID, status, cursor *string, limit int32) (*apiv1.ListRunsResponse, error) {
	v := url.Values{}
	if taskID != nil {
		v.Set("task_id", *taskID)
	}
	if status != nil {
		v.Set("status", *status)
	}
	if cursor != nil {
		v.Set("cursor", *cursor)
	}
	if limit > 0 {
		v.Set("limit", fmt.Sprintf("%d", limit))
	}

	path := "/api/v1/runs"
	if encoded := v.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var resp apiv1.ListRunsResponse
	err := c.do(ctx, http.MethodGet, path, nil, &resp)
	return &resp, err
}

func (c *Client) CompleteRun(ctx context.Context, runID string, req apiv1.CompleteRunRequest) (*apiv1.CompleteRunResponse, error) {
	var resp apiv1.CompleteRunResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/runs/"+runID+"/complete", req, &resp)
	return &resp, err
}

func (c *Client) FailRun(ctx context.Context, runID string, req apiv1.FailRunRequest) (*apiv1.FailRunResponse, error) {
	var resp apiv1.FailRunResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/runs/"+runID+"/fail", req, &resp)
	return &resp, err
}

func (c *Client) ScheduleRun(ctx context.Context, runID string, req apiv1.ScheduleRunRequest) (*apiv1.ScheduleRunResponse, error) {
	var resp apiv1.ScheduleRunResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/runs/"+runID+"/schedule", req, &resp)
	return &resp, err
}

func (c *Client) WaitForEvent(ctx context.Context, runID string, req apiv1.WaitForEventRequest) (*apiv1.WaitForEventResponse, error) {
	var resp apiv1.WaitForEventResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/runs/"+runID+"/wait-for-event", req, &resp)
	return &resp, err
}

// Checkpoints

func (c *Client) SetCheckpoint(ctx context.Context, taskID, stepName string, req apiv1.SetCheckpointRequest) (*apiv1.SetCheckpointResponse, error) {
	var resp apiv1.SetCheckpointResponse
	err := c.do(ctx, http.MethodPut, "/api/v1/tasks/"+taskID+"/checkpoints/"+stepName, req, &resp)
	return &resp, err
}

func (c *Client) GetCheckpoint(ctx context.Context, taskID, stepName string) (*apiv1.GetCheckpointResponse, error) {
	var resp apiv1.GetCheckpointResponse
	err := c.do(ctx, http.MethodGet, "/api/v1/tasks/"+taskID+"/checkpoints/"+stepName, nil, &resp)
	return &resp, err
}

// Events

func (c *Client) EmitEvent(ctx context.Context, req apiv1.EmitEventRequest) (*apiv1.EmitEventResponse, error) {
	var resp apiv1.EmitEventResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/events", req, &resp)
	return &resp, err
}

// Workflow Runs

func (c *Client) CreateWorkflowRun(ctx context.Context, req apiv1.CreateWorkflowRunRequest) (*apiv1.CreateWorkflowRunResponse, error) {
	var resp apiv1.CreateWorkflowRunResponse
	err := c.do(ctx, http.MethodPost, "/api/v1/workflow-runs", req, &resp)
	return &resp, err
}

func (c *Client) UpdateWorkflowRun(ctx context.Context, workflowRunID string, req apiv1.UpdateWorkflowRunRequest) (*apiv1.UpdateWorkflowRunResponse, error) {
	var resp apiv1.UpdateWorkflowRunResponse
	err := c.do(ctx, http.MethodPatch, "/api/v1/workflow-runs/"+workflowRunID, req, &resp)
	return &resp, err
}
