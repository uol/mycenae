package collector

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/uol/mycenae/lib/tserr"
)

type TSDBpoints []TSDBpoint

type TSDBpoint struct {
	Metric    string            `json:"metric,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	Value     *float64          `json:"value,omitempty"`
	Text      string            `json:"text,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
}

func (p TSDBpoints) Validate() gobol.Error {
	if len(p) == 0 {
		return tserr.New(
			errors.New("no points"),
			"no points",
			http.StatusBadRequest,
			[]zapcore.Field{
				zap.String("package", "collector"),
				zap.String("struct", "TSDBpoint"),
			},
		)
	}
	return nil
}

type RestError struct {
	Datapoint TSDBpoint   `json:"datapoint"`
	Gerr      gobol.Error `json:"error"`
}

type RestErrorUser struct {
	Datapoint TSDBpoint   `json:"datapoint"`
	Error     interface{} `json:"error"`
}

type RestErrors struct {
	Errors  []RestErrorUser `json:"errors"`
	Failed  int             `json:"failed"`
	Success int             `json:"success"`
}

type Point struct {
	Message   TSDBpoint
	ID        string
	Bucket    string
	Keyset    string
	Timestamp int64
	Number    bool
	TTL       uint8
}

type StructV2Error struct {
	Key    string `json:"key"`
	Metric string `json:"metric"`
	Tags   []Tag  `json:"tagsError"`
}

type Tag struct {
	Key   string `json:"tagKey"`
	Value string `json:"tagValue"`
}

type MetaInfo struct {
	Metric string `json:"metric"`
	ID     string `json:"id"`
	Tags   []Tag  `json:"tagsNested"`
}

type LogMeta struct {
	Action string   `json:"action"`
	Meta   MetaInfo `json:"meta"`
}

type EsIndex struct {
	EsID    string `json:"_id"`
	EsType  string `json:"_type"`
	EsIndex string `json:"_index"`
}

type BulkType struct {
	ID EsIndex `json:"index"`
}

type EsMetric struct {
	Metric string `json:"metric"`
}

type EsTagKey struct {
	Key string `json:"key"`
}

type EsTagValue struct {
	Value string `json:"value"`
}
