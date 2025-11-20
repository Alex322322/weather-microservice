package postgres

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	createTableQuery = `
		CREATE TABLE forecast IF NOT EXISTS
		(
			id SERIAL PRIMARY KEY,
			name          text      NOT NULL,
			forecast_time timestamp NOT NULL,
			temperature   float8    NOT NULL
		);
	`

	insertQuery = `
		INSERT INTO public.forecast (name, forecast_time, temperature) 
		VALUES ($1, $2, $3);
	`

	selectLastQuery = `
		SELECT name,
       		forecast_time,
       		temperature
		FROM public.forecast
		WHERE name = $1
		ORDER BY forecast_time DESC
		LIMIT 1;
	`
)

type Data struct {
	Name        string    `db:"name"`
	Timestamp   time.Time `db:"forecast_time"`
	Temperature float64   `db:"temperature"`
}

func CreateTableQuery(ctx context.Context, p *pgxpool.Pool) {
	_, err := p.Exec(ctx, createTableQuery)
	if err != nil {
		log.Fatal("Error while creating the table")
	}
}

func InsertQuery(ctx context.Context, p *pgxpool.Pool, name string, timestamp time.Time, temperature float64) {
	_, err := p.Exec(ctx, insertQuery, name, timestamp, temperature)
	if err != nil {
		log.Fatal("Error while inserting value into the table")
	}
}

func SelectLastQuery(ctx context.Context, p *pgxpool.Pool, w http.ResponseWriter, cityName string) Data {
	var data Data
	err := p.QueryRow(ctx, selectLastQuery, cityName).Scan(&data.Name, &data.Timestamp, &data.Temperature)
	if err != nil {
		//if errors.Is(err, p.) {
		//	w.WriteHeader(http.StatusNotFound)
		//	w.Write([]byte("not found"))
		//	return
		//}

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
		log.Fatal("Error while selecting value from the table")
		return Data{}
	}
	return data
}
