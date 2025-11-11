package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-co-op/gocron/v2"
)

const httpPort = ":3000"

func main() {
	// server
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/{city}", func(w http.ResponseWriter, r *http.Request) {
		city := chi.URLParam(r, "city")

		fmt.Printf("requested city: %s\n", city)

		_, err := w.Write([]byte("welcome 1231231"))
		if err != nil {
			log.Println(err)
		}
	})

	// scheduler
	s, err := gocron.NewScheduler()
	if err != nil {
		log.Println(err)
	}

	jobs, err := createJobs(s)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	wg.Go(func() {
		fmt.Println("Starting HTTP server on port:", httpPort)
		err := http.ListenAndServe(httpPort, r)
		if err != nil {
			panic(err)
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

func createJobs(sheduler gocron.Scheduler) ([]gocron.Job, error) {
	j, err := sheduler.NewJob(
		gocron.DurationJob(
			10*time.Second,
		),
		gocron.NewTask(
			func() {
				fmt.Println("check cron")
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return []gocron.Job{j}, nil
}
