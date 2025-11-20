package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Data struct {
	Id          int       `db:"id"`
	Name        string    `db:"name"`
	Timestamp   time.Time `db:"forecast_time"`
	Temperature float64   `db:"temperature"`
}

type Repository interface {
	Insert(ctx context.Context, d Data) error
	GetLatest(ctx context.Context, city string) (Data, error)
}

type repo struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) Repository {
	return &repo{pool: pool}
}

const (
	insertQuery = `
	INSERT INTO public.forecast (name, forecast_time, temperature)
	VALUES ($1, $2, $3);
	`
	selectLastQuery = `
	SELECT id, name, forecast_time, temperature
	FROM public.forecast
	WHERE name = $1
	ORDER BY forecast_time DESC
	LIMIT 1;
	`
)

func (r *repo) Insert(ctx context.Context, d Data) error {
	_, err := r.pool.Exec(ctx, insertQuery, d.Name, d.Timestamp, d.Temperature)
	return err
}

func (r *repo) GetLatest(ctx context.Context, city string) (Data, error) {
	var d Data
	err := r.pool.QueryRow(ctx, selectLastQuery, city).
		Scan(&d.Id, &d.Name, &d.Timestamp, &d.Temperature)
	if err != nil {
		return Data{}, err
	}
	return d, nil
}

/*
func InsertQuery(ctx context.Context, p *pgxpool.Pool, name string, timestamp time.Time, temperature float64) {
	_, err := p.Exec(ctx, insertQuery, name, timestamp, temperature)
	if err != nil {
		log.Fatal("Error while inserting value into the table")
	}
}

func SelectLastQuery(ctx context.Context, p *pgxpool.Pool, w http.ResponseWriter, cityName string) Data {
	var data Data
	err := p.QueryRow(ctx, selectLastQuery, cityName).Scan(&data.Id, &data.Name, &data.Timestamp, &data.Temperature)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("not found"))
			return Data{}
		}
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		log.Fatal("Error while selecting value from the table")
		return Data{}
	}
	return data
}*/
