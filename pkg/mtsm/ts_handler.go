package mtsm

type TsHandler struct {
	Global  map[string]string `json:"global" note:"全局标签"`
	Metrics string            `json:"metrics"`
	Vector  []TsVector        `json:"vector"`
	Matrix  []TsMatrix        `json:"matrix"`
}

type handlerOpts func(*TsHandler)

func NewHandler(opts ...handlerOpts) *TsHandler {
	t := &TsHandler{}
	t.Global = map[string]string{}
	for _, opt := range opts {
		opt(t)
	}

	return t
}

func WithCleanVector(vector []TsVector) handlerOpts {
	return func(t *TsHandler) {
		t.Vector = vector
	}
}

func WithCleanMatrix(matrix []TsMatrix) handlerOpts {
	return func(t *TsHandler) {
		t.Matrix = matrix
	}
}

func (t *TsHandler) ToTv() *TsHandler {
	for i := range t.Matrix {
		t.Matrix[i].ValueToTv()
	}
	for i := range t.Vector {
		t.Vector[i].ValueToTv()
	}
	return t
}

func (t *TsHandler) ClipLabels() *TsHandler {
	for i := range t.Vector {
		t.Metrics = t.Vector[i].Metric["__name__"]
		t.Vector[i].Metric = t.mergeGlobalLabel(t.Vector[i].Metric)
	}

	for i := range t.Matrix {
		if i == 0 {
			t.Metrics = t.Matrix[i].Metric["__name__"]
		}
		t.Matrix[i].Metric = t.mergeGlobalLabel(t.Matrix[i].Metric)
	}
	return t
}

// 将前缀 "g_" 标签加入到全局, 并移除 __name__ 标签
func (t *TsHandler) mergeGlobalLabel(maps map[string]string) map[string]string {
	findName := false
	for key, value := range maps {
		// 如果是 __name__ 标签, 则移除
		if !findName && key == "__name__" {
			delete(maps, key)
			findName = true
		}
		// 如果是全局标签, 则加入到全局, 并移除
		if key[0:2] == "g_" {
			t.Global[key[2:]] = value
			delete(maps, key)
			continue
		}
	}
	return maps
}
