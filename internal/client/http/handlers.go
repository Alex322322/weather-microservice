package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Alex322322/weather-microservice/internal/service/weather"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

type Handler struct {
	svc weather.Service
}

func NewHandler(s weather.Service) *Handler {
	return &Handler{svc: s}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/{city}", h.GetLatest)
	r.Post("/schedule/{city}", h.Schedule)
}

func (h *Handler) GetLatest(w http.ResponseWriter, r *http.Request) {
	city := chi.URLParam(r, "city")
	ctx := r.Context()

	data, err := h.svc.GetLatest(ctx, city)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) Schedule(w http.ResponseWriter, r *http.Request) {
	// we will schedule job via separate scheduler
	city := chi.URLParam(r, "city")
	// For this simple handler we can put the scheduling logic in scheduler package,
	// and here just call scheduler.Add(city)
	// but for now we just return accepted
	// TODO: call scheduler.AddJob(city)
	res := fmt.Sprintf("scheduling job for city: %s", city)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(res))
}
