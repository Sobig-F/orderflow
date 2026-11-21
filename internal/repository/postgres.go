package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sobig-F/orderflow/internal/order"
)

type Postgres struct {
	pool databasePool
}

type databasePool interface {
	Ping(context.Context) error
	Close()
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

var openPool = func(ctx context.Context, databaseURL string) (databasePool, error) {
	return pgxpool.New(ctx, databaseURL)
}

func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	pool, err := openPool(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	return newPostgres(ctx, pool)
}

func newPostgres(ctx context.Context, pool databasePool) (*Postgres, error) {
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}

func (p *Postgres) CreateOrder(ctx context.Context, req order.CreateOrderRequest) (*order.Order, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin create order transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var result order.Order
	if req.IdempotencyKey != "" {
		// Serialize requests for the same key until the transaction ends.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, req.IdempotencyKey); err != nil {
			return nil, err
		}
		var response []byte
		var matches bool
		err := tx.QueryRow(ctx, `SELECT response,amount=$2::numeric AND currency=$3 FROM order_requests WHERE key=$1`, req.IdempotencyKey, req.Amount, req.Currency).Scan(&response, &matches)
		switch {
		case err == nil:
			if !matches {
				return nil, order.ErrKeyConflict
			}
			if err := json.Unmarshal(response, &result); err != nil {
				return nil, err
			}
			return &result, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, err
		}
	}

	query := `
		INSERT INTO orders (amount, currency)
		VALUES ($1, $2)
		RETURNING
			id::text,
			amount::text,
			currency,
			status,
			created_at,
			updated_at
	`

	err = tx.QueryRow(ctx, query, req.Amount, req.Currency).Scan(
		&result.ID,
		&result.Amount,
		&result.Currency,
		&result.Status,
		&result.CreatedAt,
		&result.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert order: %w", err)
	}
	if err := insertOutboxEvent(ctx, tx, "order.created", &result); err != nil {
		return nil, err
	}
	if req.IdempotencyKey != "" {
		if _, err := tx.Exec(ctx, `INSERT INTO order_requests(key,amount,currency,response) VALUES ($1,$2,$3,$4)`, req.IdempotencyKey, req.Amount, req.Currency, result); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit create order transaction: %w", err)
	}

	return &result, nil
}

func (p *Postgres) GetOrderByID(ctx context.Context, id string) (*order.Order, error) {
	var result order.Order

	query := `
		SELECT
			id::text,
			amount::text,
			currency,
			status,
			created_at,
			updated_at
		FROM orders
		WHERE id = $1::uuid
	`

	err := p.pool.QueryRow(ctx, query, id).Scan(
		&result.ID,
		&result.Amount,
		&result.Currency,
		&result.Status,
		&result.CreatedAt,
		&result.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, order.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select order by id: %w", err)
	}

	return &result, nil
}

func (p *Postgres) ListOrders(ctx context.Context, pages ...order.Page) ([]order.Order, error) {

	page := order.Page{Limit: 20}
	if len(pages) > 0 {
		page = pages[0]
	}
	rows, err := p.pool.Query(ctx, `SELECT id::text,amount::text,currency,status,created_at,updated_at FROM orders
 WHERE ($1::text='' OR status=$1) AND ($2::uuid IS NULL OR (created_at,id)<(SELECT created_at,id FROM orders WHERE id=$2))
 ORDER BY created_at DESC,id DESC LIMIT $3`, page.Status, page.Cursor, page.Limit)
	if err != nil {
		return nil, fmt.Errorf("select orders: %w", err)
	}
	defer rows.Close()

	orders := make([]order.Order, 0)
	for rows.Next() {
		var result order.Order
		if err := rows.Scan(
			&result.ID,
			&result.Amount,
			&result.Currency,
			&result.Status,
			&result.CreatedAt,
			&result.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan order: %w", err)
		}
		orders = append(orders, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate orders: %w", err)
	}

	return orders, nil
}

func (p *Postgres) UpdateOrderStatus(ctx context.Context, id, expectedStatus, newStatus string) (*order.Order, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin update order transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var result order.Order

	query := `
		UPDATE orders
		SET status = $3, updated_at = NOW()
		WHERE id = $1::uuid AND status = $2
		RETURNING
			id::text,
			amount::text,
			currency,
			status,
			created_at,
			updated_at
	`

	err = tx.QueryRow(ctx, query, id, expectedStatus, newStatus).Scan(
		&result.ID,
		&result.Amount,
		&result.Currency,
		&result.Status,
		&result.CreatedAt,
		&result.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, order.ErrStatusChanged
	}
	if err != nil {
		return nil, fmt.Errorf("update order status: %w", err)
	}
	if err := insertOutboxEvent(ctx, tx, "order.status_changed", &result); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit update order transaction: %w", err)
	}

	return &result, nil
}

func (p *Postgres) PendingOutboxEvents(ctx context.Context, limit int) ([]order.OutboxEvent, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, event_type, order_id::text, payload::text, created_at
		FROM order_outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("select outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]order.OutboxEvent, 0)
	for rows.Next() {
		var event order.OutboxEvent
		if err := rows.Scan(&event.ID, &event.Type, &event.OrderID, &event.Payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return events, nil
}

func (p *Postgres) MarkOutboxEventPublished(ctx context.Context, id int64) error {
	_, err := p.pool.Exec(ctx, `UPDATE order_outbox SET published_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

func (p *Postgres) RecordOutboxFailure(ctx context.Context, id int64, message string) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE order_outbox
		SET attempts = attempts + 1, last_error = $2
		WHERE id = $1
	`, id, message)
	if err != nil {
		return fmt.Errorf("record outbox failure: %w", err)
	}
	return nil
}

func insertOutboxEvent(ctx context.Context, tx pgx.Tx, eventType string, value *order.Order) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode outbox event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO order_outbox (event_type, order_id, payload)
		VALUES ($1, $2::uuid, $3::jsonb)
	`, eventType, value.ID, payload)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
