package geocoding

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// создаем обертку
type client struct {
	httpClient *http.Client
}

// возвращаем экземпляр клиента
func NewClient(httpClient *http.Client) *client {
	return &client{
		httpClient: httpClient,
	}
}

// структура для декодирования
type Response struct {
	Name      string  `json:"name"`
	Country   string  `json:"country"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}



func (c *client) GetCords(city string) (Response, error) {
	resp, err := c.httpClient.Get(
		fmt.Sprintf("https://geocoding-api.open-meteo.com/v1/search?name=%s&count=1&language=en&format=json", city),
	)
	if err != nil {
		return Response{}, err
	}

	defer resp.Body.Close()

	// проверка кода ответа
	if resp.StatusCode != http.StatusOK {
		return Response{}, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	// парсим верхнеуровнево results
	var geoResp struct {
		Results []Response `json:"results"`
	}

	// преобразуем JSON
	err = json.NewDecoder(resp.Body).Decode(&geoResp)
	if err != nil {
		return Response{}, err
	}

	return geoResp.Results[0], nil
}
