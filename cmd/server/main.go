package main

import (
	"encoding/json"
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
)

const (
	httpPort = ":3000"
	city = "moscow"
)

type Data struct {
	Timestamp time.Time
	Temperature float64
}

type Storage struct {
	data map[string][]Data
	mu sync.RWMutex
}

func main() {
	// настройка маршрутизатора
	r := chi.NewRouter()
	// настройка middleware обработчика, работающая для всех endpoint
	r.Use(middleware.Logger)

	//
	storage := &Storage{
		data: make(map[string][]Data),
	}

	// настройка обработки по пути city
	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		// достаем city из маршрутизатора
		cityName := chi.URLParam(r, "city")

		fmt.Printf("requested city: %s\n", cityName)

		storage.mu.RLock()
		defer storage.mu.RUnlock()

		data, ok := storage.data[cityName]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// 
		raw, err := json.Marshal(data)
		if err != nil {
			log.Println(err)
		}

		_, err = w.Write(raw)
		if err != nil {
			log.Println(err)
		}
	})

	// создаем scheduler
	s, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}

	jobs, err := createJobs(s, storage)
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
func createJobs(sheduler gocron.Scheduler, storage *Storage) ([]gocron.Job, error) {
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

				storage.mu.Lock()
				defer storage.mu.Unlock()

				timeStamp, err :=  time.Parse("2006-01-02T15:04 ", openResp.Current.Time)
				if err != nil {
					log.Println(err)
					return 
				}


				storage.data[city] = append(storage.data[city], Data{Timestamp: timeStamp, Temperature: openResp.Current.Temperature2m})

				fmt.Printf("%v updated data in storage: %s\n", time.Now(), city)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{j}, nil
}
