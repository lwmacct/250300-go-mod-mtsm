package mtsm

import (
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/lwmacct/250300-go-mod-murl/pkg/murl"
)

type TsClient struct {
	resty *resty.Client
}

// ApiQuery 执行 Prometheus 查询
func (c *TsClient) ApiQuery(params map[string]string) (*api_query, error) {
	t := &api_query{
		client: c,
	}
	err := t.request(params)
	return t, err
}

// ApiQueryRange 执行 Prometheus 范围查询
func (c *TsClient) ApiQueryRange(params map[string]string) (*api_query_range, error) {
	t := &api_query_range{
		client: c,
	}
	err := t.request(params)
	return t, err
}

// ApiLabelValues 执行 Prometheus 标签值查询
func (c *TsClient) ApiLabelValues(labelName string, params map[string]string) (*api_label_values, error) {
	t := &api_label_values{
		client: c,
	}
	err := t.request(labelName, params)
	return t, err
}

// ApiDeleteSeries 执行 Prometheus 删除系列
//
// 参数示例:
//   - match[]={__name__="m_250312_test_2148"}
//   - match[]={__name__=~"m_250312_test_.*"}
func (c *TsClient) ApiDeleteSeries(body string) (*api_delete_series, error) {
	t := &api_delete_series{
		client: c,
	}
	err := t.request(body)
	return t, err
}

// Close 关闭客户端并释放资源
func (c *TsClient) Close() {
	c.resty.Clone()
}

func (t *TsClient) GetResty() *resty.Client {
	return t.resty
}

// NewClient 创建一个新的客户端实例
func NewClient(URL string) (*TsClient, error) {

	info, err := murl.NewParse(URL)
	if err != nil {
		return nil, err
	}

	r := resty.New()
	r.SetTimeout(30 * time.Second)
	r.SetRetryCount(3)
	r.SetRetryWaitTime(5 * time.Second)
	r.SetRetryMaxWaitTime(30 * time.Second)
	r.SetDebug(false)
	r.SetDisableWarn(true)
	r.SetBaseURL(info.GetBaseURL())
	if info.Username != "" && info.Password != "" {
		r.SetBasicAuth(info.Username, info.Password)
	}

	return &TsClient{
		resty: r,
	}, nil
}
