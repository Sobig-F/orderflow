package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sobig-F/orderflow/internal/order"
)

const testID = "8af426ed-3040-4f05-88ff-0a89846bbcee"

type poolStub struct {
	*pgxpool.Pool
	pingErr  error
	beginErr error
	queryErr error
	queryRow pgx.Row
	rows     pgx.Rows
	execErr  error
	tx       *txStub
	closed   bool
}

func (p *poolStub) Ping(context.Context) error { return p.pingErr }
func (p *poolStub) Close()                     { p.closed = true }
func (p *poolStub) Begin(context.Context) (pgx.Tx, error) {
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return p.tx, nil
}
func (p *poolStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return p.rows, p.queryErr
}
func (p *poolStub) QueryRow(context.Context, string, ...any) pgx.Row { return p.queryRow }
func (p *poolStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("UPDATE 1"), p.execErr
}

type txStub struct {
	pgx.Tx
	queryRow   pgx.Row
	execErr    error
	commitErr  error
	committed  bool
	rolledBack bool
}

func (tx *txStub) QueryRow(context.Context, string, ...any) pgx.Row { return tx.queryRow }
func (tx *txStub) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), tx.execErr
}
func (tx *txStub) Commit(context.Context) error {
	tx.committed = true
	return tx.commitErr
}
func (tx *txStub) Rollback(context.Context) error {
	tx.rolledBack = true
	return nil
}

type rowStub struct {
	values []any
	err    error
}

func (r rowStub) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(destinations) != len(r.values) {
		return fmt.Errorf("scan destination count %d != value count %d", len(destinations), len(r.values))
	}
	for i, value := range r.values {
		switch destination := destinations[i].(type) {
		case *bool:
			*destination = value.(bool)
		case *string:
			*destination = value.(string)
		case *time.Time:
			*destination = value.(time.Time)
		case *[]byte:
			*destination = []byte(value.(string))
		case *int64:
			*destination = value.(int64)
		default:
			return fmt.Errorf("unsupported scan destination %T", destination)
		}
	}
	return nil
}

type rowsStub struct {
	pgx.Rows
	values  [][]any
	index   int
	scanErr error
	rowsErr error
}

func (r *rowsStub) Next() bool {
	if r.index < len(r.values) {
		return true
	}
	return false
}
func (r *rowsStub) Scan(destinations ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.index >= len(r.values) {
		return errors.New("no current row")
	}
	if err := (rowStub{values: r.values[r.index]}).Scan(destinations...); err != nil {
		return err
	}
	r.index++
	return nil
}
func (r *rowsStub) Err() error { return r.rowsErr }
func (r *rowsStub) Close()     {}

func populatedRow(status string) pgx.Row {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	return rowStub{values: []any{testID, "12.50", "RUB", status, now, now}}
}

func populatedRows(status string) pgx.Rows {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	return &rowsStub{values: [][]any{{testID, "12.50", "RUB", status, now, now}}}
}

func TestNewPostgresAndPoolLifecycle(t *testing.T) {
	previous := openPool
	t.Cleanup(func() { openPool = previous })

	pool := &poolStub{}
	openPool = func(context.Context, string) (databasePool, error) { return pool, nil }
	db, err := NewPostgres(context.Background(), "postgres://test")
	if err != nil || db == nil {
		t.Fatalf("NewPostgres db=%v err=%v", db, err)
	}
	db.Close()
	if !pool.closed {
		t.Fatal("database pool was not closed")
	}

	openErr := errors.New("pool creation failed")
	openPool = func(context.Context, string) (databasePool, error) { return nil, openErr }
	if _, err := NewPostgres(context.Background(), "bad"); !errors.Is(err, openErr) {
		t.Fatalf("open error = %v", err)
	}

	pingErr := errors.New("ping failed")
	pool = &poolStub{pingErr: pingErr}
	openPool = func(context.Context, string) (databasePool, error) { return pool, nil }
	if _, err := NewPostgres(context.Background(), "postgres://test"); !errors.Is(err, pingErr) || !pool.closed {
		t.Fatalf("ping error=%v poolClosed=%v", err, pool.closed)
	}
}

func TestRepositoryPing(t *testing.T) {
	wantErr := errors.New("ping unavailable")
	pool := &poolStub{pingErr: wantErr}
	if err := (&Postgres{pool: pool}).Ping(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("ping error = %v", err)
	}
}

func TestCreateOrderTransaction(t *testing.T) {
	tx := &txStub{queryRow: populatedRow("pending")}
	db := &Postgres{pool: &poolStub{tx: tx}}
	created, err := db.CreateOrder(context.Background(), order.CreateOrderRequest{Amount: "12.50", Currency: "RUB"})
	if err != nil || created.ID != testID || created.Amount != "12.50" || !tx.committed {
		t.Fatalf("created=%+v err=%v committed=%v", created, err, tx.committed)
	}
}

func TestCreateOrderErrors(t *testing.T) {
	wantErr := errors.New("begin failed")
	if _, err := (&Postgres{pool: &poolStub{beginErr: wantErr}}).CreateOrder(context.Background(), order.CreateOrderRequest{}); !errors.Is(err, wantErr) {
		t.Fatalf("begin error = %v", err)
	}

	tx := &txStub{queryRow: rowStub{err: errors.New("insert failed")}}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).CreateOrder(context.Background(), order.CreateOrderRequest{}); err == nil {
		t.Fatal("expected insert failure")
	}

	tx = &txStub{queryRow: populatedRow("pending"), execErr: errors.New("outbox failed")}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).CreateOrder(context.Background(), order.CreateOrderRequest{}); err == nil {
		t.Fatal("expected outbox failure")
	}

	tx = &txStub{queryRow: populatedRow("pending"), commitErr: errors.New("commit failed")}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).CreateOrder(context.Background(), order.CreateOrderRequest{}); err == nil {
		t.Fatal("expected commit failure")
	}
}

func TestGetOrderByID(t *testing.T) {
	db := &Postgres{pool: &poolStub{queryRow: populatedRow("pending")}}
	got, err := db.GetOrderByID(context.Background(), testID)
	if err != nil || got.ID != testID {
		t.Fatalf("got=%+v err=%v", got, err)
	}

	db = &Postgres{pool: &poolStub{queryRow: rowStub{err: pgx.ErrNoRows}}}
	if _, err := db.GetOrderByID(context.Background(), testID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("not found error = %v", err)
	}

	wantErr := errors.New("select failed")
	db = &Postgres{pool: &poolStub{queryRow: rowStub{err: wantErr}}}
	if _, err := db.GetOrderByID(context.Background(), testID); !errors.Is(err, wantErr) {
		t.Fatalf("query error = %v", err)
	}
}

func TestListOrders(t *testing.T) {
	db := &Postgres{pool: &poolStub{rows: populatedRows("pending")}}
	got, err := db.ListOrders(context.Background())
	if err != nil || len(got) != 1 || got[0].ID != testID {
		t.Fatalf("got=%+v err=%v", got, err)
	}

	db = &Postgres{pool: &poolStub{rows: &rowsStub{}}}
	got, err = db.ListOrders(context.Background())
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty list=%v err=%v", got, err)
	}

	queryErr := errors.New("query failed")
	db = &Postgres{pool: &poolStub{queryErr: queryErr}}
	if _, err := db.ListOrders(context.Background()); !errors.Is(err, queryErr) {
		t.Fatalf("query error = %v", err)
	}

	db = &Postgres{pool: &poolStub{rows: &rowsStub{values: [][]any{{testID, "12.50", "RUB", "pending", time.Now(), time.Now()}}, scanErr: errors.New("scan failed")}}}
	if _, err := db.ListOrders(context.Background()); err == nil {
		t.Fatal("expected row scan error")
	}

	rows := &rowsStub{rowsErr: errors.New("iteration failed")}
	db = &Postgres{pool: &poolStub{rows: rows}}
	if _, err := db.ListOrders(context.Background()); err == nil {
		t.Fatal("expected rows iteration error")
	}
}

func TestUpdateOrderStatus(t *testing.T) {
	tx := &txStub{queryRow: populatedRow("processing")}
	got, err := (&Postgres{pool: &poolStub{tx: tx}}).UpdateOrderStatus(context.Background(), testID, "pending", "processing")
	if err != nil || got.Status != "processing" || !tx.committed {
		t.Fatalf("got=%+v err=%v committed=%v", got, err, tx.committed)
	}
}

func TestUpdateOrderStatusErrors(t *testing.T) {
	wantErr := errors.New("begin failed")
	if _, err := (&Postgres{pool: &poolStub{beginErr: wantErr}}).UpdateOrderStatus(context.Background(), testID, "pending", "processing"); !errors.Is(err, wantErr) {
		t.Fatalf("begin error = %v", err)
	}

	tx := &txStub{queryRow: rowStub{err: pgx.ErrNoRows}}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).UpdateOrderStatus(context.Background(), testID, "pending", "processing"); !errors.Is(err, order.ErrStatusChanged) {
		t.Fatalf("no rows error = %v", err)
	}

	tx = &txStub{queryRow: rowStub{err: errors.New("update failed")}}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).UpdateOrderStatus(context.Background(), testID, "pending", "processing"); err == nil {
		t.Fatal("expected update error")
	}

	tx = &txStub{queryRow: populatedRow("processing"), execErr: errors.New("outbox failed")}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).UpdateOrderStatus(context.Background(), testID, "pending", "processing"); err == nil {
		t.Fatal("expected outbox error")
	}

	tx = &txStub{queryRow: populatedRow("processing"), commitErr: errors.New("commit failed")}
	if _, err := (&Postgres{pool: &poolStub{tx: tx}}).UpdateOrderStatus(context.Background(), testID, "pending", "processing"); err == nil {
		t.Fatal("expected commit error")
	}
}

func TestOutboxRepositoryMethods(t *testing.T) {
	now := time.Now()
	rows := &rowsStub{values: [][]any{{int64(5), "order.created", testID, `{"id":"order"}`, now}}}
	events, err := (&Postgres{pool: &poolStub{rows: rows}}).PendingOutboxEvents(context.Background(), 10)
	if err != nil || len(events) != 1 || events[0].ID != 5 || string(events[0].Payload) != `{"id":"order"}` {
		t.Fatalf("events=%+v err=%v", events, err)
	}

	if events, err := (&Postgres{pool: &poolStub{rows: &rowsStub{}}}).PendingOutboxEvents(context.Background(), 10); err != nil || len(events) != 0 {
		t.Fatalf("empty events=%v err=%v", events, err)
	}
	queryErr := errors.New("query failed")
	if _, err := (&Postgres{pool: &poolStub{queryErr: queryErr}}).PendingOutboxEvents(context.Background(), 10); !errors.Is(err, queryErr) {
		t.Fatalf("query error = %v", err)
	}
	if _, err := (&Postgres{pool: &poolStub{rows: &rowsStub{scanErr: errors.New("scan failed"), values: [][]any{{int64(5), "order.created", testID, "{}", now}}}}}).PendingOutboxEvents(context.Background(), 10); err == nil {
		t.Fatal("expected outbox scan error")
	}
	if _, err := (&Postgres{pool: &poolStub{rows: &rowsStub{rowsErr: errors.New("iteration failed")}}}).PendingOutboxEvents(context.Background(), 10); err == nil {
		t.Fatal("expected outbox iteration error")
	}

	if err := (&Postgres{pool: &poolStub{}}).MarkOutboxEventPublished(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	if err := (&Postgres{pool: &poolStub{execErr: queryErr}}).MarkOutboxEventPublished(context.Background(), 5); !errors.Is(err, queryErr) {
		t.Fatalf("mark error = %v", err)
	}
	if err := (&Postgres{pool: &poolStub{}}).RecordOutboxFailure(context.Background(), 5, "broker failed"); err != nil {
		t.Fatal(err)
	}
	if err := (&Postgres{pool: &poolStub{execErr: queryErr}}).RecordOutboxFailure(context.Background(), 5, "broker failed"); !errors.Is(err, queryErr) {
		t.Fatalf("record error = %v", err)
	}
}

func TestReplay(t *testing.T) {
	for _, tc := range []struct {
		matches bool
		body    string
		success bool
	}{{true, `{"id":"original"}`, true}, {false, "{}", false}, {true, "invalid", false}} {
		db := Postgres{pool: &poolStub{tx: &txStub{queryRow: rowStub{values: []any{tc.body, tc.matches}}}}}
		result, err := db.CreateOrder(context.Background(), order.CreateOrderRequest{IdempotencyKey: "test"})
		if tc.success {
			if err != nil || result.ID != "original" {
				t.Fatal(result, err)
			}
		} else if err == nil {
			t.Fatal("expected replay error")
		}
	}
}
