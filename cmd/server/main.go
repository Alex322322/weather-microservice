package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpapi "github.com/Alex322322/weather-microservice/internal/client/http"
	"github.com/Alex322322/weather-microservice/internal/client/http/geocoding"
	"github.com/Alex322322/weather-microservice/internal/client/http/openmeteo"
	scheduler "github.com/Alex322322/weather-microservice/internal/cron"
	"github.com/Alex322322/weather-microservice/internal/repository/postgres"
	"github.com/Alex322322/weather-microservice/internal/service/weather"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	httpPort = ":3000"
)

func main() {
	// загрузка переменных окружения из .env файла
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file %s", err)
	}

	ctx := context.Background()

	// urlExample := "postgres://username:password@localhost:5432/database_name"
	connPool, err := pgxpool.NewWithConfig(ctx, postgres.Config())
	if err != nil {
		log.Fatalf("Error while creating connection db pool: %s", err)
	}
	defer connPool.Close()

	fmt.Println("Connected to the database!!")

	err = postgres.CreateTableQuery(ctx, connPool)
	if err != nil {
		log.Fatalf("schema init error: %v", err)
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	geocodingClient := geocoding.NewClient(httpClient)
	openmeteoClient := openmeteo.NewClient(httpClient)

	repo := postgres.NewRepository(connPool)

	svc := weather.NewService(geocodingClient, openmeteoClient, repo)
	s := scheduler.NewScheduler(svc)
	//err := gocron.NewScheduler()
	//if err != nil {
	//	panic(err)
	//}

	// настройка маршрутизатора
	r := chi.NewRouter()
	// настройка middleware обработчика, работающая для всех endpoint
	r.Use(middleware.Logger)

	h := httpapi.NewHandler(svc)
	h.RegisterRoutes(r)

	// настройка обработки по пути city
	/*
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
		})*/

	// создаем scheduler
	//s, err := gocron.NewScheduler()
	//if err != nil {
	//	panic(err)
	//}

	//s.Start()
	//fmt.Println("Scheduler started")

	//js := make([]gocron.Job, 0)

	/*
		r.Post("/schedule/{city}", func(w http.ResponseWriter, r *http.Request) {
			city := chi.URLParam(r, "city")

			fmt.Println("Jobs before: ", js)
			jobs, err := createJobs(ctx, s, connPool, city)
			if err != nil {
				http.Error(w, "could not schedule job", 500)
				return
			}
			// Optionally store jobs somewhere
			fmt.Println("Jobs created: ", jobs)
			js = append(js, jobs...)
			fmt.Println("Jobs after: ", js)
			w.Write([]byte(fmt.Sprintf("Scheduled job for %s", city)))
		})*/

	//jobs, err := createJobs(ctx, s, connPool, cityName)
	//if err != nil {
	//	panic(err)
	//}

	//var wg sync.WaitGroup

	//wg.Go(func() {
	// поднимаем сервер, блокирующий запуск
	//	fmt.Println("Starting HTTP server on port:", httpPort)
	//	err := http.ListenAndServe(httpPort, r)
	//	if err != nil {
	//		panic(err) // топ приложения
	//	}
	//})

	//fmt.Println(jobs)

	//wg.Go(func() {
	//	fmt.Printf("Starting job: %s\n", js)
	//	s.Start()
	//	fmt.Println("after start")
	// block until you are ready to shut down
	//select {
	//case <-time.After(time.Minute):
	//}

	// when you're done, shut it down
	//err = s.Shutdown()
	//if err != nil {
	// handle error
	//}
	//})

	//wg.Wait()
	//defer s.Shutdown()

	srv := &http.Server{
		Addr:    ":3000",
		Handler: r,
	}

	// graceful shutdown
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()
	log.Println("server started on :3000")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")

	ctxShut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctxShut); err != nil {
		log.Fatalf("server shutdown failed:%+v", err)
	}

	s.Stop()
	log.Println("server exiting")
}

/*
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
			30*time.Second,
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

				timeStamp, err := time.Parse("2006-01-02T15:04", openResp.Current.Time)
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
*/
