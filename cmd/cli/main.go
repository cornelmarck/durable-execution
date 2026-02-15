package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cornelmarck/durable-execution/client"
	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	"github.com/urfave/cli/v3"
)

var apiClient *client.Client

func main() {
	app := &cli.Command{
		Name:  "durable",
		Usage: "CLI for the durable-execution API",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server",
				Aliases: []string{"s"},
				Value:   "http://localhost:8080",
				Usage:   "server base URL",
				Sources: cli.EnvVars("DURABLE_SERVER"),
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			apiClient = client.New(cmd.String("server"))
			return ctx, nil
		},
		Commands: []*cli.Command{
			queuesCmd(),
			tasksCmd(),
			runsCmd(),
			eventsCmd(),
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func queuesCmd() *cli.Command {
	return &cli.Command{
		Name:  "queues",
		Usage: "Manage queues",
		Commands: []*cli.Command{
			{
				Name:  "create",
				Usage: "Create a queue",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "name", Required: true, Usage: "queue name"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					resp, err := apiClient.CreateQueue(ctx, apiv1.CreateQueueRequest{
						Name: cmd.String("name"),
					})
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
		},
	}
}

func tasksCmd() *cli.Command {
	return &cli.Command{
		Name:  "tasks",
		Usage: "Manage tasks",
		Commands: []*cli.Command{
			{
				Name:  "list",
				Usage: "List tasks",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Usage: "filter by queue name"},
					&cli.StringFlag{Name: "status", Usage: "filter by status (pending, completed, failed, cancelled)"},
					&cli.StringFlag{Name: "name", Usage: "filter by task name"},
					&cli.StringFlag{Name: "cursor", Usage: "pagination cursor from previous response"},
					&cli.IntFlag{Name: "limit", Value: 50, Usage: "max tasks to return"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					var queueName, status, taskName, cursor *string
					if v := cmd.String("queue"); v != "" {
						queueName = &v
					}
					if v := cmd.String("status"); v != "" {
						status = &v
					}
					if v := cmd.String("name"); v != "" {
						taskName = &v
					}
					if v := cmd.String("cursor"); v != "" {
						cursor = &v
					}
					resp, err := apiClient.ListTasks(ctx, queueName, status, taskName, cursor, int32(cmd.Int("limit")))
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
			{
				Name:  "spawn",
				Usage: "Spawn a task on a queue",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Required: true, Usage: "queue name"},
					&cli.StringFlag{Name: "name", Required: true, Usage: "task name"},
					&cli.StringFlag{Name: "params", Usage: "task params as JSON"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					req := apiv1.SpawnTaskRequest{TaskName: cmd.String("name")}
					if p := cmd.String("params"); p != "" {
						req.Params = json.RawMessage(p)
					}
					resp, err := apiClient.SpawnTask(ctx, cmd.String("queue"), req)
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
			{
				Name:  "claim",
				Usage: "Claim tasks from a queue",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Required: true, Usage: "queue name"},
					&cli.IntFlag{Name: "limit", Value: 1, Usage: "max tasks to claim"},
					&cli.IntFlag{Name: "timeout", Value: 30, Usage: "claim timeout in seconds"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					resp, err := apiClient.ClaimTasks(ctx, cmd.String("queue"), apiv1.ClaimTasksRequest{
						Limit:        int32(cmd.Int("limit")),
						ClaimTimeout: int32(cmd.Int("timeout")),
					})
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
		},
	}
}

func runsCmd() *cli.Command {
	return &cli.Command{
		Name:  "runs",
		Usage: "Manage runs",
		Commands: []*cli.Command{
			{
				Name:      "complete",
				Usage:     "Complete a run",
				ArgsUsage: "<run_id>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "result", Usage: "result as JSON"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					runID := cmd.Args().First()
					if runID == "" {
						return fmt.Errorf("run_id is required")
					}
					req := apiv1.CompleteRunRequest{}
					if r := cmd.String("result"); r != "" {
						req.Result = json.RawMessage(r)
					}
					resp, err := apiClient.CompleteRun(ctx, runID, req)
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
			{
				Name:      "fail",
				Usage:     "Fail a run",
				ArgsUsage: "<run_id>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "error", Required: true, Usage: "error message"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					runID := cmd.Args().First()
					if runID == "" {
						return fmt.Errorf("run_id is required")
					}
					resp, err := apiClient.FailRun(ctx, runID, apiv1.FailRunRequest{
						Error: cmd.String("error"),
					})
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
		},
	}
}

func eventsCmd() *cli.Command {
	return &cli.Command{
		Name:  "events",
		Usage: "Manage events",
		Commands: []*cli.Command{
			{
				Name:  "emit",
				Usage: "Emit an event",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "name", Required: true, Usage: "event name"},
					&cli.StringFlag{Name: "payload", Usage: "event payload as JSON"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					req := apiv1.EmitEventRequest{EventName: cmd.String("name")}
					if p := cmd.String("payload"); p != "" {
						req.Payload = json.RawMessage(p)
					}
					resp, err := apiClient.EmitEvent(ctx, req)
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
		},
	}
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
