package mtsm

import (
	"fmt"
)

type api_query_range struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string     `json:"resultType"`
		Result     []TsMatrix `json:"result"`
	} `json:"data"`
	Stats struct {
		SeriesFetched     string `json:"seriesFetched"`
		ExecutionTimeMsec int    `json:"executionTimeMsec"`
	} `json:"stats"`
	client *TsClient
}

func (t *api_query_range) request(params map[string]string) error {
	url := "/prometheus/api/v1/query_range"
	resp, err := t.client.resty.R().
		SetHeader("Accept", "application/json").
		SetQueryParams(params).
		SetResult(t).
		Get(url)

	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode(), resp.String())
	}

	return nil
}
