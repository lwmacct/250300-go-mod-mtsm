package mtsm

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-resty/resty/v2"
)

type TsMetrics struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (m *TsMetrics) Init(metric map[string]string) {
	if m.Metric == nil || m.Values == nil || m.Timestamps == nil {
		m.Metric = make(map[string]string)
		m.Values = make([]float64, 0)
		m.Timestamps = make([]int64, 0)
	}
}

// timestamp 为毫秒 UnixMilli
func (m *TsMetrics) AddValue(value float64, timestamp int64) {
	m.Values = append(m.Values, value)
	m.Timestamps = append(m.Timestamps, timestamp)
}

// timestamp 为毫秒 UnixMilli
func (t *TsMetrics) AddMulti(values []float64, timestamps []int64) error {
	if len(values) != len(timestamps) {
		return errors.New("values and timestamps length mismatch")
	}
	for i, value := range values {
		t.AddValue(value, timestamps[i])
	}
	return nil
}

func (m *TsMetrics) JsonMarshal() []byte {
	data, _ := json.Marshal(m)
	return data
}

func (t *TsMetrics) Push(client *resty.Client, url ...string) (resp *resty.Response, err error) {
	if len(url) == 0 {
		url = []string{"/api/v1/import"}
	}
	resp, err = client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(t).
		Post(url[0])
	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 204 {
		return resp, fmt.Errorf("push metrics failed, status code: %d", resp.StatusCode())
	}
	return resp, nil
}
