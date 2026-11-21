package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Sobig-F/orderflow/internal/order"
)

type outboxStoreStub struct {
	events       []order.OutboxEvent
	pendingErr   error
	markErr      error
	recordErr    error
	marked       []int64
	recorded     []int64
	requestLimit int
	onPending    func()
}

func (s *outboxStoreStub) PendingOutboxEvents(context.Context, int) ([]order.OutboxEvent, error) {
	s.requestLimit++
	if s.onPending != nil {
		s.onPending()
	}
	return s.events, s.pendingErr
}
func (s *outboxStoreStub) MarkOutboxEventPublished(_ context.Context, id int64) error {
	s.marked = append(s.marked, id)
	return s.markErr
}
func (s *outboxStoreStub) RecordOutboxFailure(_ context.Context, id int64, _ string) error {
	s.recorded = append(s.recorded, id)
	return s.recordErr
}

type publisherStub struct {
	err   error
	ids   []int64
	calls int
}

func (p *publisherStub) Publish(_ context.Context, event order.OutboxEvent) error {
	p.calls++
	p.ids = append(p.ids, event.ID)
	return p.err
}
func (p *publisherStub) Close() error { return nil }

func TestPublishBatchSuccessAndFailurePaths(t *testing.T) {
	event := order.OutboxEvent{ID: 1, Type: "order.created", OrderID: "id", CreatedAt: time.Now()}
	store := &outboxStoreStub{events: []order.OutboxEvent{event}}
	publisher := &publisherStub{}
	publishBatch(context.Background(), store, publisher)
	if publisher.calls != 1 || len(store.marked) != 1 || store.marked[0] != event.ID {
		t.Fatalf("published=%d marked=%v", publisher.calls, store.marked)
	}

	store = &outboxStoreStub{pendingErr: errors.New("database down")}
	publishBatch(context.Background(), store, &publisherStub{})

	store = &outboxStoreStub{events: []order.OutboxEvent{event}, recordErr: errors.New("record failed")}
	publisher = &publisherStub{err: errors.New("Kafka down")}
	publishBatch(context.Background(), store, publisher)
	if len(store.recorded) != 1 || len(store.marked) != 0 {
		t.Fatalf("publish failure recorded=%v marked=%v", store.recorded, store.marked)
	}

	store = &outboxStoreStub{events: []order.OutboxEvent{event}, markErr: errors.New("mark failed")}
	publishBatch(context.Background(), store, &publisherStub{})
	if len(store.marked) != 1 {
		t.Fatalf("mark call count = %d, want 1", len(store.marked))
	}
}

func TestRunOutboxPublisherStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &outboxStoreStub{onPending: cancel}
	RunOutboxPublisher(ctx, store, &publisherStub{})
	if store.requestLimit != 1 {
		t.Fatalf("pending event requests = %d, want 1", store.requestLimit)
	}
}

func TestPublishBatchDoesNotRecordAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := &outboxStoreStub{events: []order.OutboxEvent{{ID: 4}}, recordErr: errors.New("unused")}
	publisher := &publisherStub{err: errors.New("publish canceled")}
	publishBatch(ctx, store, publisher)
	if len(store.recorded) != 0 {
		t.Fatalf("recorded = %v, want no write after cancellation", store.recorded)
	}
}
