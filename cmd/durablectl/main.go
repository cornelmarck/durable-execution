package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	apiv1 "github.com/cornelmarck/durable-execution/api/v1"
	client "github.com/cornelmarck/durable-execution/clients/go"
	"github.com/urfave/cli/v3"
)

var apiClient *client.Client

type config struct {
	Server string `json:"server,omitempty"`
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".durablectl", "config.json")
}

func loadConfig() config {
	data, err := os.ReadFile(configPath())
	if err != nil {
		return config{}
	}
	var cfg config
	json.Unmarshal(data, &cfg)
	return cfg
}

func saveConfig(cfg config) error {
	dir := filepath.Dir(configPath())
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath(), data, 0600)
}

func resolveServer(cmd *cli.Command) string {
	// --server flag or DURABLE_SERVER env was explicitly set
	if cmd.IsSet("server") {
		return cmd.String("server")
	}
	// Config file
	if cfg := loadConfig(); cfg.Server != "" {
		return cfg.Server
	}
	// Default
	return "http://localhost:8080"
}

func main() {
	app := &cli.Command{
		Name:  "durablectl",
		Usage: "CLI for the durable-execution API",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server",
				Aliases: []string{"s"},
				Usage:   "server base URL",
				Sources: cli.EnvVars("DURABLE_SERVER"),
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			apiClient = client.New(resolveServer(cmd))
			return ctx, nil
		},
		Commands: []*cli.Command{
			configCmd(),
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

func configCmd() *cli.Command {
	return &cli.Command{
		Name:  "config",
		Usage: "Manage CLI configuration",
		Commands: []*cli.Command{
			{
				Name:      "set-server",
				Usage:     "Set the default server URL",
				ArgsUsage: "<url>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					url := cmd.Args().First()
					if url == "" {
						return fmt.Errorf("url is required")
					}
					cfg := loadConfig()
					cfg.Server = url
					if err := saveConfig(cfg); err != nil {
						return err
					}
					fmt.Printf("Server set to %s\n", url)
					return nil
				},
			},
			{
				Name:  "get-server",
				Usage: "Show the current server URL",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					cfg := loadConfig()
					if cfg.Server == "" {
						fmt.Println("http://localhost:8080 (default)")
					} else {
						fmt.Println(cfg.Server)
					}
					return nil
				},
			},
		},
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
			{
				Name:  "list",
				Usage: "List all queues",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					resp, err := apiClient.ListQueues(ctx)
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
			{
				Name:      "delete",
				Usage:     "Delete a queue",
				ArgsUsage: "<queue_name>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					name := cmd.Args().First()
					if name == "" {
						return fmt.Errorf("queue_name is required")
					}
					if err := apiClient.DeleteQueue(ctx, name); err != nil {
						return err
					}
					fmt.Println("deleted")
					return nil
				},
			},
			{
				Name:      "stats",
				Usage:     "Get queue statistics",
				ArgsUsage: "<queue_name>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					name := cmd.Args().First()
					if name == "" {
						return fmt.Errorf("queue_name is required")
					}
					resp, err := apiClient.GetQueueStats(ctx, name)
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
					&cli.StringFlag{Name: "status", Usage: "filter by status (pending, completed, failed, canceled)"},
					&cli.StringFlag{Name: "name", Usage: "filter by task name"},
					&cli.StringFlag{Name: "cursor", Usage: "pagination cursor from previous response"},
					&cli.IntFlag{Name: "limit", Value: 50, Usage: "max tasks to return"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Int("limit") < 0 {
						return fmt.Errorf("limit must be non-negative")
					}
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
				Name:  "create",
				Usage: "Create a task on a queue",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "queue", Required: true, Usage: "queue name"},
					&cli.StringFlag{Name: "name", Required: true, Usage: "task name"},
					&cli.StringFlag{Name: "params", Usage: "task params as JSON"},
					&cli.IntFlag{Name: "max-attempts", Value: 1, Usage: "maximum number of attempts"},
					&cli.FloatFlag{Name: "retry-delay", Value: 0, Usage: "base retry delay in seconds"},
					&cli.StringFlag{Name: "retry-strategy", Value: "fixed", Usage: "retry strategy: fixed or exponential"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					req := apiv1.CreateTaskRequest{TaskName: cmd.String("name")}
					if p := cmd.String("params"); p != "" {
						req.Params = json.RawMessage(p)
					}
					if n := int32(cmd.Int("max-attempts")); n > 1 {
						req.MaxAttempts = &n
						delay := cmd.Float("retry-delay")
						if delay <= 0 {
							delay = 1
						}
						rs := &apiv1.RetryStrategy{
							Kind:        apiv1.RetryStrategyKind(cmd.String("retry-strategy")),
							BaseSeconds: delay,
						}
						req.RetryStrategy = rs
					}
					resp, err := apiClient.CreateTask(ctx, cmd.String("queue"), req)
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
					&cli.IntFlag{Name: "poll", Value: 0, Usage: "long poll seconds (0 to disable)"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					req := apiv1.ClaimTasksRequest{
						Limit:        int32(cmd.Int("limit")),
						ClaimTimeout: int32(cmd.Int("timeout")),
					}
					if p := int32(cmd.Int("poll")); p > 0 {
						req.LongPollSeconds = &p
					}
					resp, err := apiClient.ClaimTasks(ctx, cmd.String("queue"), req)
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
				Name:  "list",
				Usage: "List runs",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "task", Usage: "filter by task ID"},
					&cli.StringFlag{Name: "status", Usage: "filter by status (pending, claimed, completed, failed, sleeping)"},
					&cli.StringFlag{Name: "cursor", Usage: "pagination cursor from previous response"},
					&cli.IntFlag{Name: "limit", Value: 50, Usage: "max runs to return"},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.Int("limit") < 0 {
						return fmt.Errorf("limit must be non-negative")
					}
					var taskID, status, cursor *string
					if v := cmd.String("task"); v != "" {
						taskID = &v
					}
					if v := cmd.String("status"); v != "" {
						status = &v
					}
					if v := cmd.String("cursor"); v != "" {
						cursor = &v
					}
					resp, err := apiClient.ListRuns(ctx, taskID, status, cursor, int32(cmd.Int("limit")))
					if err != nil {
						return err
					}
					return printJSON(resp)
				},
			},
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
