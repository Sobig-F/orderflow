package messaging

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Sobig-F/orderflow/internal/order"
)

type writerStub struct {
	messages []kafka.Message
	err      error
	closed   bool
}

func (w *writerStub) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	return w.err
}
func (w *writerStub) Close() error {
	w.closed = true
	return w.err
}

func TestKafkaPublisherMapsOutboxEvent(t *testing.T) {
	writer := &writerStub{}
	publisher := newKafka(writer)
	event := order.OutboxEvent{ID: 27, Type: "order.created", OrderID: "order-1", Payload: []byte(`{"id":"order-1"}`), CreatedAt: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(writer.messages))
	}
	message := writer.messages[0]
	if string(message.Key) != event.OrderID || string(message.Value) != string(event.Payload) {
		t.Fatalf("message key/value = %q/%s", message.Key, message.Value)
	}
	gotHeaders := map[string]string{}
	for _, header := range message.Headers {
		gotHeaders[header.Key] = string(header.Value)
	}
	if gotHeaders["event-id"] != strconv.FormatInt(event.ID, 10) || gotHeaders["event-type"] != event.Type || gotHeaders["occurred-at"] != event.CreatedAt.Format(time.RFC3339Nano) {
		t.Fatalf("headers = %#v", gotHeaders)
	}

	wantErr := errors.New("broker unavailable")
	writer.err = wantErr
	if err := publisher.Publish(context.Background(), event); !errors.Is(err, wantErr) {
		t.Fatalf("publish error = %v, want %v", err, wantErr)
	}
	if err := publisher.Close(); !errors.Is(err, wantErr) || !writer.closed {
		t.Fatalf("close error=%v closed=%v", err, writer.closed)
	}
}

func TestNewKafkaTrimsBrokerAddresses(t *testing.T) {
	publisher := NewKafka(" broker-1:9092, broker-2:9092 ", "events")
	if publisher.writer == nil {
		t.Fatal("Kafka writer is nil")
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
}
