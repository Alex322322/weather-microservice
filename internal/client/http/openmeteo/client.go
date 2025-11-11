package openmeteo

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type client struct {
	httpClient *http.Client
}

type Response struct {
	Current struct {
		Time          string  `json:"time"`
		Temperature2m float64 `json:"temperature_2m"`
	} //`json:"current"`
}

//const geoUrl = "https://geocoding-api.open-meteo.com/v1/search?name=%s&count=1&language=en&format=json"

func NewClient(httpClient *http.Client) *client {
	return &client{
		httpClient: httpClient,
	}
}

func (c *client) GetTemp(longitude, latitude float64) (Response, error) {
	resp, err := c.httpClient.Get(
		fmt.Sprintf("https://api.open-meteo.com/v1/forecast?latitude=%f&longitude=%f&current=temperature_2m", latitude, longitude),
	)
	if err != nil {
		return Response{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Response{}, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	var response Response
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return Response{}, err
	}

	return response, nil
}
