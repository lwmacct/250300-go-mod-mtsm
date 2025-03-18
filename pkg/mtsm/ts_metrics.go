package mtsm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-resty/resty/v2"
)

type TsMetrics struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
	Conf       *metricsConf      `json:"-"`
}

type metricsConf struct {
	Err     error
	Client  *resty.Client
	ApiPath string
}

type metricsOpts func(*TsMetrics)

func NewMetrics(label map[string]string, client *resty.Client, opts ...metricsOpts) *TsMetrics {
	t := &TsMetrics{
		Conf:       &metricsConf{Client: client, ApiPath: "/api/v1/import"},
		Metric:     label,
		Values:     make([]float64, 0),
		Timestamps: make([]int64, 0),
	}
	for _, opt := range opts {
		opt(t)
	}

	return t
}

// 单个值, 单个时间戳
func (m *TsMetrics) AddValue(value float64, timestamp int64) {
	m.Values = append(m.Values, value)
	m.Timestamps = append(m.Timestamps, timestamp)
}

// 多个值, 多个时间戳
func (t *TsMetrics) AddMulti(values []float64, timestamps []int64) error {
	if len(values) != len(timestamps) {
		return errors.New("values and timestamps length mismatch")
	}
	for i, value := range values {
		t.AddValue(value, timestamps[i])
	}
	return nil
}

// 序列化 Metric 为 JSON 字符串
func (m *TsMetrics) ToJSON() []byte {
	data, _ := json.Marshal(m)
	return data
}

// 多个值, 多个时间戳
func (t *TsMetrics) SetValues(values []float64, timestamps []int64) {
	t.Values = values
	t.Timestamps = timestamps
}

func (t *TsMetrics) SetLabel(key string, value string) {
	t.Metric[key] = value
}

func (t *TsMetrics) Push(ctx context.Context) error {

	resp, err := t.Conf.Client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(t). // 使用字符串形式的 JSON
		Post(t.Conf.ApiPath)
	if err != nil {
		return err
	}

	if resp.StatusCode() != 204 {
		return fmt.Errorf("push metrics failed, status code: %d", resp.StatusCode())
	}

	return nil
}

func WithMetricsApiPath(apiPath string) metricsOpts {
	return func(t *TsMetrics) {
		t.Conf.ApiPath = apiPath
	}
}
