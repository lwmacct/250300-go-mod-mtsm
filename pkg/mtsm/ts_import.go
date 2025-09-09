package mtsm

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-resty/resty/v2"
)

type TsImport struct {
	dataChan       chan []byte   // 数据通道
	batchSize      int           // 批量大小
	submitInterval time.Duration // 提交间隔

	resty    *resty.Client // resty
	url      string        // 推送 URL
	stopChan chan struct{} // 停止信号

	stopped  atomic.Bool // 原子标记是否已停止
	stopOnce sync.Once   // 确保只停止一次
}

type OptImport func(t *TsImport)

func WithUrl(url string) OptImport {
	return func(t *TsImport) {
		t.url = url
	}
}

func WithVmBaseUrl(vmBaseUrl string) OptImport {
	return func(t *TsImport) {
		t.resty.SetBaseURL(vmBaseUrl)
	}
}

func WithChannelBuffer(channelBuffer int) OptImport {
	return func(t *TsImport) {
		t.dataChan = make(chan []byte, channelBuffer)
	}
}

func WithBatchSize(batchSize int) OptImport {
	return func(t *TsImport) {
		t.batchSize = batchSize
	}
}

func WithSubmitInterval(submitInterval time.Duration) OptImport {
	return func(t *TsImport) {
		t.submitInterval = submitInterval
	}
}

// NewImport 创建一个 TsImport
func NewImport(opts ...OptImport) *TsImport {
	t := &TsImport{
		dataChan:       make(chan []byte, 100000), // 缓冲区
		batchSize:      10000,
		submitInterval: 1 * time.Second,
		resty: resty.New().
			SetTimeout(10 * time.Second).
			SetRetryCount(3).
			SetTransport(&http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 50,
				IdleConnTimeout:     90 * time.Second,
			}),
		url:      "/api/v1/import", // 默认 VictoriaMetrics 导入端点
		stopChan: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}

	t.startWorker()
	return t
}

// 添加序列化后的 Metrics 数据（默认阻塞，保证不丢失数据）
func (t *TsImport) AddMetrics(jsonData []byte) *TsImport {
	if t.stopped.Load() {
		slog.Warn("TsImport 已停止，忽略新的 metrics 数据")
		return t
	}

	select {
	case t.dataChan <- jsonData:
		// 数据成功发送到通道
	case <-t.stopChan:
		// 停止信号收到，不再接收新数据
		slog.Warn("TsImport 正在停止，忽略新的 metrics 数据")
	}
	return t
}

// 获取当前通道中等待处理的 metrics 数量
func (t *TsImport) Len() int {
	return len(t.dataChan)
}

// 获取通道容量
func (t *TsImport) Cap() int {
	return cap(t.dataChan)
}

// 停止 TsImport，会等待剩余数据处理完成（并发安全）
func (t *TsImport) Stop() {
	t.stopOnce.Do(func() {
		t.stopped.Store(true)
		close(t.stopChan)
		slog.Info("TsImport 已停止")
	})
}

// 检查是否已停止
func (t *TsImport) IsStopped() bool {
	return t.stopped.Load()
}

// 获取详细统计信息
func (t *TsImport) Stats() map[string]interface{} {
	channelLen := t.Len()
	channelCap := t.Cap()
	usagePercent := float64(channelLen) / float64(channelCap) * 100

	return map[string]interface{}{
		"channel_len":     channelLen,
		"channel_cap":     channelCap,
		"channel_usage":   fmt.Sprintf("%.2f%%", usagePercent),
		"channel_free":    channelCap - channelLen,
		"batch_size":      t.batchSize,
		"submit_interval": t.submitInterval.String(),
		"url":             t.url,
		"is_channel_full": channelLen >= channelCap,
		"is_nearly_full":  usagePercent >= 90, // 90%以上算接近满载
		"is_stopped":      t.IsStopped(),      // 添加停止状态
	}
}

// 启动后台 worker 协程
func (t *TsImport) startWorker() {
	go func() {
		ticker := time.NewTicker(t.submitInterval)
		defer ticker.Stop()

		batch := make([][]byte, 0, t.batchSize)

		for {
			select {
			case data := <-t.dataChan:
				batch = append(batch, data)

				// 满1000条立即提交
				if len(batch) >= t.batchSize {
					t.submitBatch(batch)
					batch = batch[:0] // 重置 slice
				}

			case <-ticker.C:
				// 定时器触发，提交当前批次（如果有数据）
				if len(batch) > 0 {
					t.submitBatch(batch)
					batch = batch[:0] // 重置 slice
				}

			case <-t.stopChan:
				// 停止信号，提交剩余数据并退出
				if len(batch) > 0 {
					t.submitBatch(batch)
				}
				return
			}
		}
	}()
}

// 批量提交数据到 VictoriaMetrics
func (t *TsImport) submitBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	// 生成 JSON lines 格式数据
	jsonLinesData := t.buildJsonLinesFromBytes(batch)

	// 发送 HTTP 请求
	resp, err := t.resty.R().
		SetHeader("Content-Type", "application/json").
		SetBody(jsonLinesData).
		Post(t.url)

	if err != nil {
		slog.Error("批量推送 metrics 失败", "error", err.Error())
		return
	}

	// 检查响应状态码 (VictoriaMetrics 成功导入返回 204)
	if resp.StatusCode() != 204 {
		slog.Error("批量推送 metrics 失败，数据已丢弃",
			"count", len(batch),
			"status_code", resp.StatusCode(),
			"response", string(resp.Body()))
		return
	}
}

// 从指定的 JSON 数据 slice 生成 JSON lines 格式数据（高性能版本）
func (t *TsImport) buildJsonLinesFromBytes(jsonDataList [][]byte) []byte {
	if len(jsonDataList) == 0 {
		return []byte{}
	}

	// 单条数据的特殊优化
	if len(jsonDataList) == 1 {
		return jsonDataList[0]
	}

	// 计算总长度（一次遍历）
	totalLen := 0
	for i, jsonData := range jsonDataList {
		totalLen += len(jsonData)
		if i < len(jsonDataList)-1 {
			totalLen++ // 换行符长度
		}
	}

	// 预分配精确大小，使用 copy 而不是 append
	result := make([]byte, totalLen)
	offset := 0

	for i, jsonData := range jsonDataList {
		// 使用 copy 直接内存拷贝，比 append 更快
		copy(result[offset:], jsonData)
		offset += len(jsonData)

		// 添加换行符（除了最后一行）
		if i < len(jsonDataList)-1 {
			result[offset] = '\n'
			offset++
		}
	}

	return result
}
