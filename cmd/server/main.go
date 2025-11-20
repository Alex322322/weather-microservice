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
	"github.com/Alex322322/weather-microservice/internal/repository/postgres"
	"github.com/Alex322322/weather-microservice/internal/repository/postgres/pgMethods"
	"github.com/Alex322322/weather-microservice/internal/repository/postgres/pgxconfig"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	httpPort = ":3000"
)


func main() {
	// настройка маршрутизатора
	r := chi.NewRouter()
	// настройка middleware обработчика, работающая для всех endpoint
	r.Use(middleware.Logger)

	ctx := context.Background()

	// urlExample := "postgres://username:password@localhost:5432/database_name"
	connPool, err := pgxpool.NewWithConfig(ctx, postgres.Config())
	if err != nil {
		log.Fatal("Error while creating connection to the database!")
	}

	conn, err := connPool.Acquire(ctx)
	if err != nil {
		log.Fatal("Error while acquiring connection from the database pool!")
	}
	defer conn.Release()

	err = conn.Ping(ctx)
	if err!=nil{
		log.Fatal("Could not ping database")
	}

 	fmt.Println("Connected to the database!!")

	postgres.CreateTableQuery(ctx, connPool)

	defer connPool.Close()

	// настройка обработки по пути city
	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		// достаем city из маршрутизатора
		cityName := chi.URLParam(r, "city")
		
		data := postgres.SelectLastQuery(ctx, connPool, w, cityName)

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

	jobs, err := createJobs(ctx, s, connPool, cityName)
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
func createJobs(ctx context.Context, sheduler gocron.Scheduler, p *pgxpool.Pool, city string) ([]gocron.Job, error) {
	// создаем httpClient с таймаутом
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	geocodingClient := geocoding.NewClient(httpClient)
	openmeteoClient := openmeteo.NewClient(httpClient)

	j, err := sheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				geoResp, err := geocodingClient.GetCords(city)
				if err != nil {
					log.Println(err)
					return
				}

				openResp, err := openmeteoClient.GetTemp(geoResp.Longitude, geoResp.Latitude)
				if err != nil {
					log.Println(err)
					return
				}
				
				timeStamp, err :=  time.Parse("2006-01-02T15:04", openResp.Current.Time)
				if err != nil {
					log.Println(err)
					return 
				}

				postgres.InsertQuery(ctx, p, city, timeStamp, openResp.Current.Temperature2m)

				fmt.Printf("%v updated data in storage: %s\n", time.Now(), city)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{j}, nil
}


func GetCityHandler(repo postgres.Repository) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        cityName := chi.URLParam(r, "city")

        data, err := repo.SelectLastQuery(r.Context(), cityName)
        if err != nil {
            http.Error(w, "internal error", 500)
            return
        }

        json.NewEncoder(w).Encode(data)
    }
}