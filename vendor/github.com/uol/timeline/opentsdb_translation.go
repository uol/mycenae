package timeline

import (
	"fmt"
	"time"

	serializer "github.com/uol/serializer/opentsdb"
)

// extractData - extracts the hash from the instance
func (t *OpenTSDBTransport) extractData(instance interface{}, operation *FlatOperation) (*serializer.ArrayItem, []interface{}, error) {

	item, ok := instance.(*serializer.ArrayItem)
	if !ok {
		return nil, nil, fmt.Errorf("error casting instance to data channel item: %+v", instance)
	}

	hashParameters := []interface{}{}
	hashParameters = append(hashParameters, item.Metric)
	hashParameters = append(hashParameters, item.Tags...)

	if operation != nil {
		hashParameters = append(hashParameters, *operation)
	}

	return item, hashParameters, nil
}

// DataChannelItemToFlattenerPoint - converts the data channel item to the flattened point one
func (t *OpenTSDBTransport) DataChannelItemToFlattenerPoint(configuration *DataTransformerConfig, instance interface{}, operation FlatOperation) (Hashable, error) {

	item, hashParameters, err := t.extractData(instance, &operation)

	if item.Timestamp <= 0 {
		item.Timestamp = time.Now().Unix()
	}

	hash, err := getHash(configuration, hashParameters...)
	if err != nil {
		return nil, err
	}

	return &FlattenerPoint{
		value: item.Value,
		hash:  hash,
		flattenerPointData: flattenerPointData{
			operation:       operation,
			timestamp:       item.Timestamp,
			dataChannelItem: item,
		},
	}, nil
}

// FlattenerPointToDataChannelItem - converts the flattened point to the data channel one
func (t *OpenTSDBTransport) FlattenerPointToDataChannelItem(point *FlattenerPoint) (interface{}, error) {

	item, ok := point.dataChannelItem.(*serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting point's data channel item: %+v", point)
	}

	item.Value = point.value

	return item, nil
}

// DataChannelItemToAccumulatedData - converts the data channel item to the accumulated data
func (t *OpenTSDBTransport) DataChannelItemToAccumulatedData(configuration *DataTransformerConfig, instance interface{}, calculateHash bool) (Hashable, error) {

	item, hashParameters, err := t.extractData(instance, nil)

	var hash string

	if calculateHash {
		hash, err = getHash(configuration, hashParameters...)
		if err != nil {
			return nil, err
		}
	}

	return &accumulatedData{
		count: 0,
		hash:  hash,
		data:  item,
	}, nil
}

// AccumulatedDataToDataChannelItem - converts the accumulated data to the data channel item
func (t *OpenTSDBTransport) AccumulatedDataToDataChannelItem(point *accumulatedData) (interface{}, error) {

	item, ok := point.data.(*serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting accumulated data to data channel item: %+v", point)
	}

	item.Timestamp = time.Now().Unix()
	item.Value = float64(point.count)

	return item, nil
}

// Serialize - renders the text using the configured serializer
func (t *OpenTSDBTransport) Serialize(item interface{}) (string, error) {

	return t.serializer.SerializeGeneric(item)
}
