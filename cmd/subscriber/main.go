package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GarupanOjisan/pubsub-cli/pkg/subscriber"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "pubsub-cli",
		Usage: "A CLI for Google Cloud Pub/Sub",
		Commands: []*cli.Command{
			{
				Name:  "subscriber",
				Usage: "Subscriber commands",
				Subcommands: []*cli.Command{
					{
						Name:  "subscribe",
						Usage: "Subscribe to a subscription",
						Flags: []cli.Flag{
							&cli.StringFlag{
								Name:     "project",
								Usage:    "Google Cloud project ID",
								Required: true,
							},
							&cli.StringFlag{
								Name:     "subscription",
								Usage:    "Subscription ID to subscribe to",
								Required: true,
							},
							&cli.BoolFlag{
								Name:  "ack",
								Usage: "Acknowledge received messages",
								Value: false,
							},
						},
						Action: func(c *cli.Context) error {
							projectID := c.String("project")
							subscriptionID := c.String("subscription")
							shouldAck := c.Bool("ack")
							return subscribeToSubscription(c.Context, projectID, subscriptionID, shouldAck)
						},
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func subscribeToSubscription(ctx context.Context, projectID string, subscriptionID string, shouldAck bool) error {
	// Create a subscriber client
	sub, err := subscriber.NewSubscriber(projectID)
	if err != nil {
		return fmt.Errorf("failed to create subscriber: %v", err)
	}
	defer sub.Close()

	// Create a context that can be cancelled with Ctrl+C
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Set up signal handling for graceful shutdown
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		fmt.Println("\nReceived interrupt signal. Shutting down...")
		cancel()
	}()

	// Subscribe to the subscription
	return sub.Subscribe(ctx, subscriptionID, shouldAck, os.Stdout)
}
