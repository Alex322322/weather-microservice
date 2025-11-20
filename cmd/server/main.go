package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Alex322322/weather-microservice/internal/client/http/geocoding"
	"github.com/Alex322322/weather-microservice/internal/client/http/openmeteo"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5"
)

const (
	httpPort = ":3000"
	city = "moscow"
)

type Data struct {
	Name string `db:"name"`
	Timestamp time.Time `db:"forecast_time"`
	Temperature float64 `db:"temperature"`
}



func main() {
	// настройка маршрутизатора
	r := chi.NewRouter()
	// настройка middleware обработчика, работающая для всех endpoint
	r.Use(middleware.Logger)

	ctx := context.Background()

	// urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := pgx.Connect(ctx, "postgres://user:pass@localhost:5432/weather_db")
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		//os.Exit(1)
		panic(err)
	}
	defer conn.Close(ctx)

	// настройка обработки по пути city
	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		// достаем city из маршрутизатора
		cityName := chi.URLParam(r, "city")

		fmt.Printf("requested city: %s\n", cityName)
		
		query := `
			SELECT name,
       			forecast_time,
       			temperature
			FROM public.forecast
			WHERE name = $1
			ORDER BY forecast_time DESC
			LIMIT 1;
		`
		var data Data
		err = conn.QueryRow(ctx, query, city).Scan(&data.Name, &data.Timestamp, &data.Temperature)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("not found"))
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal server error"))
			return
		}


		// 
		raw, err := json.Marshal(data)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal server error"))
			return
		}

		_, err = w.Write(raw)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("internal server error"))
			return
		}
	})

	// создаем scheduler
	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}

	jobs, err := createJobs(ctx, s, conn)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		// поднимаем сервер, блокирующий запуск
		fmt.Println("Starting HTTP server on port:", httpPort)
		err := http.ListenAndServe(httpPort, r)
		if err != nil {
			panic(err) // топ приложения
		}
	})

	//fmt.Println(jobs)

	wg.Go(func() {
		fmt.Printf("Starting job: %s\n", jobs[0].ID())
		s.Start()

		// block until you are ready to shut down
		select {
		case <-time.After(time.Minute):
		}

		// when you're done, shut it down
		err = s.Shutdown()
		if err != nil {
			// handle error
		}
	})

	wg.Wait()

}

// функция создания Job
func createJobs(ctx context.Context, sheduler gocron.Scheduler, conn *pgx.Conn) ([]gocron.Job, error) {
	// создаем httpClient с таймаутом
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
	}
	geocodingClient := geocoding.NewClient(httpClient)
	openmeteoClient := openmeteo.NewClient(httpClient)

	j, err := sheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				//
				geoResp, err := geocodingClient.GetCords(city)
				if err != nil {
					log.Println(err)
					return
				}

				//
				openResp, err := openmeteoClient.GetTemp(geoResp.Longitude, geoResp.Latitude)
				if err != nil {
					log.Println(err)
					return
				}

				//storage.mu.Lock()
				//defer storage.mu.Unlock()
				
				timeStamp, err :=  time.Parse("2006-01-02T15:04", openResp.Current.Time)
				if err != nil {
					log.Println(err)
					return 
				}

				query := `
					INSERT INTO public.forecast (name, forecast_time, temperature) 
					VALUES ($1, $2, $3);
				`
				_, err = conn.Exec(ctx, query, city, timeStamp, openResp.Current.Temperature2m)
				if err != nil {
					log.Println(err)
					return
				}

				fmt.Printf("%v updated data in storage: %s\n", time.Now(), city)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{j}, nil
}
