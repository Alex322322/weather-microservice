package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

const createTableQuery = `
		CREATE TABLE IF NOT EXISTS forecast
		(
			id SERIAL PRIMARY KEY,
			name          text      NOT NULL,
			forecast_time timestamp NOT NULL,
			temperature   float8    NOT NULL
		);
	`

// init Schema
func CreateTableQuery(ctx context.Context, p *pgxpool.Pool) error {
	_, err := p.Exec(ctx, createTableQuery)
	return err
}
