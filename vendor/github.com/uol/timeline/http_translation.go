package timeline

// DataChannelItemToFlattenerPoint - converts the data channel item to the flattened point one
func (t *HTTPTransport) DataChannelItemToFlattenerPoint(configuration *DataTransformerConfig, instance interface{}, operation FlatOperation) (Hashable, error) {

	return t.serializerTransport.dataChannelItemToFlattenerPoint(configuration, instance, operation)
}

// FlattenerPointToDataChannelItem - converts the flattened point to the data channel one
func (t *HTTPTransport) FlattenerPointToDataChannelItem(point *FlattenerPoint) (interface{}, error) {

	return t.serializerTransport.flattenerPointToDataChannelItem(point)
}

// DataChannelItemToAccumulatedData - converts the data channel item to the accumulated data
func (t *HTTPTransport) DataChannelItemToAccumulatedData(configuration *DataTransformerConfig, instance interface{}, calculateHash bool) (Hashable, error) {

	return t.serializerTransport.dataChannelItemToAccumulatedData(configuration, instance, calculateHash)
}

// AccumulatedDataToDataChannelItem - converts the accumulated data to the data channel item
func (t *HTTPTransport) AccumulatedDataToDataChannelItem(point *AccumulatedData) (interface{}, error) {

	return t.serializerTransport.accumulatedDataToDataChannelItem(point)
}

// Serialize - renders the text using the configured serializer
func (t *HTTPTransport) Serialize(item interface{}) (string, error) {

	return t.serializer.SerializeGeneric(item)
}
