package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/Alex322322/weather-microservice/internal/service/weather"
	"github.com/go-co-op/gocron/v2"
)

type Scheduler struct {
	s   gocron.Scheduler
	svc weather.Service
}

func NewScheduler(svc weather.Service) *Scheduler {
	sc, err := gocron.NewScheduler()
	if err != nil {
		panic(err)
	}
	sc.Start()
	return &Scheduler{s: sc, svc: svc}
}

func (sch *Scheduler) AddCityEvery(city string, interval time.Duration) error {
	// you can pass function with parameters
	jobFn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := sch.svc.UpdateCity(ctx, city); err != nil {
			fmt.Printf("update city %s error: %v\n", city, err)
		} else {
			fmt.Printf("updated city %s\n", city)
		}
	}

	_, err := sch.s.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(jobFn),
	)

	return err
}

func (sch *Scheduler) Stop() {
	sch.s.Shutdown()
}
