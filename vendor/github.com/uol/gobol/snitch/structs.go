package snitch

//Settings represents default package-wide settings.
type Settings struct {
	Address             string
	Port                int
	Protocol            string
	HTTPTimeout         string
	HTTPPostInterval    string
	Tags                map[string]string
	Interval            string
	Runtime             bool
	RaiseDebugVerbosity bool
}

//Point holds information required by timeseries and also control variables for the stats package.
type Point struct {
	Name         string
	Metric       string
	Tags         map[string]string
	Aggregation  string
	SendInterval string
	KeepValue    bool
	SendOnNull   bool
	Disabled     bool
}

type message struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
}

type filePoints struct {
	Metric       string
	Tags         map[string]string
	Aggregation  string
	SendInterval string
	SendOnNull   bool
	KeepValue    bool
	Disabled     bool
}

//PreTransform is called just before the point is send.
type PreTransform func(p *CustomPoint) bool

//PostTransform is called just after the point is send.
type PostTransform func(p *CustomPoint)
