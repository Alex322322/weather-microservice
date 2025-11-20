package weather

import (
	"context"
	"fmt"
	"time"

	geo "github.com/Alex322322/weather-microservice/internal/client/http/geocoding"
	meteo "github.com/Alex322322/weather-microservice/internal/client/http/openmeteo"
	repo "github.com/Alex322322/weather-microservice/internal/repository/postgres"
)

type Service interface {
	UpdateCity(ctx context.Context, city string) error
	GetLatest(ctx context.Context, city string) (repo.Data, error)
}

type service struct {
	geoClient   geo.GeoClient
	meteoClient meteo.MeteoClient
	repo        repo.Repository
}

func NewService(g geo.GeoClient, m meteo.MeteoClient, r repo.Repository) Service {
	return &service{
		geoClient:   g,
		meteoClient: m,
		repo:        r,
	}
}

func (s *service) UpdateCity(ctx context.Context, city string) error {
	// get coordinates
	geoResp, err := s.geoClient.GetCords(city)
	if err != nil {
		return fmt.Errorf("geo: %w", err)
	}

	// get weather forecast
	openResp, err := s.meteoClient.GetTemp(geoResp.Longitude, geoResp.Latitude)
	if err != nil {
		return fmt.Errorf("meteo: %w", err)
	}

	timeStamp, err := time.Parse("2006-01-02T15:04", openResp.Current.Time)
	if err != nil {
		return fmt.Errorf("error parsing time: %w", err)
	}

	d := repo.Data{
		Name:        city,
		Timestamp:   timeStamp,
		Temperature: openResp.Current.Temperature2m,
	}

	if err := s.repo.Insert(ctx, d); err != nil {
		return fmt.Errorf("repo insert: %w", err)
	}

	return nil
}

func (s *service) GetLatest(ctx context.Context, city string) (repo.Data, error) {
	return s.repo.GetLatest(ctx, city)
}
