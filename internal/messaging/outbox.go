package messaging

import (
	"context"
	"github.com/Sobig-F/orderflow/internal/metrics"
	"log"
	"log/slog"
	"time"

	"github.com/Sobig-F/orderflow/internal/order"
)

type OutboxStore interface {
	PendingOutboxEvents(context.Context, int) ([]order.OutboxEvent, error)
	MarkOutboxEventPublished(context.Context, int64) error
	RecordOutboxFailure(context.Context, int64, string) error
}

func RunOutboxPublisher(ctx context.Context, store OutboxStore, publisher Publisher) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		publishBatch(ctx, store, publisher)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func publishBatch(ctx context.Context, store OutboxStore, publisher Publisher) {
	events, err := store.PendingOutboxEvents(ctx, 100)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("read Kafka outbox: %v", err)
		}
		return
	}

	for _, event := range events {
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := publisher.Publish(publishCtx, event)
		cancel()
		if err != nil {
			metrics.KafkaWrites.WithLabelValues("error").Inc()
			if ctx.Err() == nil {
				log.Printf("publish Kafka event %d: %v", event.ID, err)
				if recordErr := store.RecordOutboxFailure(ctx, event.ID, err.Error()); recordErr != nil {
					log.Printf("record Kafka outbox failure %d: %v", event.ID, recordErr)
				}
			}
			return
		}
		metrics.KafkaWrites.WithLabelValues("success").Inc()
		slog.Info("published event", "event_id", event.ID, "order_id", event.OrderID, "event_type", event.Type)
		if err := store.MarkOutboxEventPublished(ctx, event.ID); err != nil {
			log.Printf("mark Kafka event %d published: %v", event.ID, err)
			return
		}
	}
}
