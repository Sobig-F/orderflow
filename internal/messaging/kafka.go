package messaging

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Sobig-F/orderflow/internal/order"
)

type Publisher interface {
	Publish(context.Context, order.OutboxEvent) error
	Close() error
}

type Kafka struct {
	writer messageWriter
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

func NewKafka(brokers, topic string) *Kafka {
	addresses := strings.Split(brokers, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}

	return &Kafka{writer: &kafka.Writer{
		Addr:         kafka.TCP(addresses...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}}
}

func newKafka(writer messageWriter) *Kafka {
	return &Kafka{writer: writer}
}

func (k *Kafka) Publish(ctx context.Context, event order.OutboxEvent) error {
	return k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.OrderID),
		Value: event.Payload,
		Headers: []kafka.Header{
			{Key: "event-id", Value: []byte(stringID(event.ID))},
			{Key: "event-type", Value: []byte(event.Type)},
			{Key: "occurred-at", Value: []byte(event.CreatedAt.UTC().Format(time.RFC3339Nano))},
		},
	})
}

func (k *Kafka) Close() error {
	return k.writer.Close()
}

func stringID(id int64) string {
	return strconv.FormatInt(id, 10)
}
