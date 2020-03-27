package timeline

import (
	"fmt"
	"time"

	serializer "github.com/uol/serializer/json"
)

// extractData - extracts the instance data
func (t *HTTPTransport) extractData(instance interface{}, operation *FlatOperation, removeTSAndValue bool) (item *serializer.ArrayItem, value float64, timestamp int64, hashParameters []interface{}, err error) {

	var ok bool
	item, ok = instance.(*serializer.ArrayItem)
	if !ok {
		err = fmt.Errorf("error casting instance to data channel item")
		return
	}

	hashParameters = []interface{}{}
	hashParameters = append(hashParameters, item.Name)

	if operation != nil {
		hashParameters = append(hashParameters, *operation)
	}

	valueFound := false
	timestampFound := false
	var selectedParameters []interface{}
	j := 0

	if removeTSAndValue {
		selectedParameters = make([]interface{}, len(item.Parameters)-4)
	}

	for i := 0; i < len(item.Parameters); i++ {

		if i%2 == 0 && (!valueFound || !timestampFound) {

			key, ok := item.Parameters[i].(string)
			if !ok {
				err = fmt.Errorf("expecting a property name in parameter item: %s", item.Parameters[i])
				return
			}

			if !valueFound && key == t.configuration.ValueProperty {
				valueFound = true
				value, ok = item.Parameters[i+1].(float64)
				if !ok {
					err = fmt.Errorf("expecting a float64 as value for parameter: %s", item.Parameters[i+1])
					return
				}

				i++
				continue
			}

			if !timestampFound && key == t.configuration.TimestampProperty {
				timestampFound = true
				timestamp, ok = item.Parameters[i+1].(int64)
				if !ok {
					err = fmt.Errorf("expecting a int64 as value for parameter: %s", item.Parameters[i+1])
					return
				}

				i++
				continue
			}
		}

		if removeTSAndValue {
			selectedParameters[j] = item.Parameters[i]
			j++
		}

		hashParameters = append(hashParameters, item.Parameters[i])
	}

	if removeTSAndValue {
		item.Parameters = selectedParameters
	}

	if !timestampFound {
		timestamp = time.Now().Unix()
	}

	return
}

// DataChannelItemToFlattenerPoint - converts the data channel item to the flattened point one
func (t *HTTPTransport) DataChannelItemToFlattenerPoint(configuration *DataTransformerConf, instance interface{}, operation FlatOperation) (Hashable, error) {

	item, value, timestamp, hashParameters, err := t.extractData(instance, &operation, false)
	if err != nil {
		return nil, err
	}

	hash, err := getHash(configuration, hashParameters...)
	if err != nil {
		return nil, err
	}

	return &FlattenerPoint{
		value: value,
		hash:  hash,
		flattenerPointData: flattenerPointData{
			operation: operation,
			timestamp: timestamp,
			dataChannelItem: serializer.ArrayItem{
				Name:       item.Name,
				Parameters: hashParameters[2:],
			},
		},
	}, nil
}

// FlattenerPointToDataChannelItem - converts the flattened point to the data channel one
func (t *HTTPTransport) FlattenerPointToDataChannelItem(point *FlattenerPoint) (interface{}, error) {

	item, ok := point.dataChannelItem.(serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting flattener point to data channel item")
	}

	item.Parameters = append(item.Parameters, t.configuration.TimestampProperty, point.timestamp, t.configuration.ValueProperty, point.value)

	return item, nil
}

// DataChannelItemToAccumulatedData - converts the data channel item to the accumulated data
func (t *HTTPTransport) DataChannelItemToAccumulatedData(configuration *DataTransformerConf, instance interface{}) (Hashable, error) {

	casted, _, _, hashParameters, err := t.extractData(instance, nil, true)
	if err != nil {
		return nil, err
	}

	hash, err := getHash(configuration, hashParameters...)
	if err != nil {
		return nil, err
	}

	return &AccumulatedData{
		count: 0,
		hash:  hash,
		data:  *casted,
	}, nil
}

// AccumulatedDataToDataChannelItem - converts the accumulated data to the data channel item
func (t *HTTPTransport) AccumulatedDataToDataChannelItem(point *AccumulatedData) (interface{}, error) {

	item, ok := point.data.(serializer.ArrayItem)
	if !ok {
		return nil, fmt.Errorf("error casting accumulated data to data channel item")
	}

	item.Parameters = append(item.Parameters, t.configuration.TimestampProperty, time.Now().Unix(), t.configuration.ValueProperty, float64(point.count))

	return item, nil
}
