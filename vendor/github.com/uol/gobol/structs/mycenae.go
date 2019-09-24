package structs

/**
* All common structs used by the timeline library.
* @author rnojiri
**/

// Point - the base point
type Point struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp int64             `json:"timestamp"`
}

// NumberPoint - a point with number type value
type NumberPoint struct {
	Point
	Value float64 `json:"value"`
}

// TextPoint - a point with text type value
type TextPoint struct {
	Point
	Text string `json:"text"`
}
