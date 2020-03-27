package json

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

/**
* Has all struct parsing methods from the JSON serializer.
* @author rnojiri
**/

// Add - adds a new JSON mapping
func (s *Serializer) Add(name string, item interface{}, variablePath ...string) error {

	variablePathMap := map[string]struct{}{}
	if len(variablePath) > 0 {
		for _, path := range variablePath {
			variablePathMap[path] = struct{}{}
		}
	}

	m, err := s.mapJSON(item, variablePathMap)
	if err != nil {
		return err
	}

	s.mapping[name] = m

	return nil
}

// mapJSON - maps a new JSON struct
func (s *Serializer) mapJSON(item interface{}, variablePaths map[string]struct{}) (*mappedJSON, error) {

	varSequence := []string{}

	var b strings.Builder
	b.Grow(s.bufferSize)

	b.WriteString("{")

	err := s.mapStruct(item, &b, &varSequence, variablePaths, "")
	if err != nil {
		return nil, err
	}

	b.WriteString("}")

	variableMap := map[string]int{}
	for i, variable := range varSequence {
		variableMap[variable] = i
	}

	return &mappedJSON{
		format:       b.String(),
		formatSize:   b.Len(),
		numVariables: len(varSequence),
		variableMap:  variableMap,
	}, nil
}

// writeMapInStringFormat - writes the map string format
func (s *Serializer) writeMapInStringFormat(field *reflect.StructField, value *reflect.Value, b *strings.Builder, varSequence *[]string, variablePaths map[string]struct{}, path string) (bool, error) {

	keep, variableType, currentPath := s.fieldToProperty(field, b, varSequence, variablePaths, path)
	if !keep {
		return false, nil
	}

	if variableType == propertyVariable {
		b.WriteString("{%s}")
		return true, nil
	}

	b.WriteString("{")
	it := value.MapRange()

	hasNext := it.Next()
	for hasNext {

		key := it.Key().String()
		keyPath := s.buildPath(currentPath, key)

		s.writePropertyString(key, b)

		val := it.Value()

		if _, ok := variablePaths[keyPath]; ok {

			formatSymbol, err := s.getFormatSymbol(val.Type().Kind())
			if err != nil {
				return false, err
			}

			b.WriteString(formatSymbol)
			*varSequence = append(*varSequence, keyPath)

		} else {

			strVal, err := s.getValueFromField(nil, &val)
			if err != nil {
				return false, err
			}

			b.WriteString(strVal)
		}

		hasNext = it.Next()
		if hasNext {
			b.WriteString(",")
		}
	}

	b.WriteString("}")

	return true, nil
}

// writeArrayInStringFormat - writes in array string format
func (s *Serializer) writeArrayInStringFormat(field *reflect.StructField, value *reflect.Value, b *strings.Builder, varSequence *[]string, variablePaths map[string]struct{}, path string) (bool, error) {

	keep, variableType, currentPath := s.fieldToProperty(field, b, varSequence, variablePaths, path)
	if !keep {
		return false, nil
	}

	if variableType == propertyVariable {
		b.WriteString("[%s]")
		return true, nil
	}

	arraySize := value.Len()

	b.WriteString("[")

	var indexBuilder strings.Builder

	for i := 0; i < arraySize; i++ {

		indexBuilder.Grow(len(currentPath) + 5)
		indexBuilder.WriteString(currentPath)
		indexBuilder.WriteString("[")
		indexBuilder.WriteString(strconv.Itoa(i))
		indexBuilder.WriteString("]")

		val := value.Index(i)

		if _, ok := variablePaths[indexBuilder.String()]; ok {

			formatSymbol, err := s.getFormatSymbol(val.Type().Kind())
			if err != nil {
				return false, err
			}

			b.WriteString(formatSymbol)
			*varSequence = append(*varSequence, indexBuilder.String())

		} else {

			strVal, err := s.getValueFromField(nil, &val)
			if err != nil {
				return false, err
			}

			b.WriteString(strVal)
		}

		indexBuilder.Reset()

		if i < arraySize-1 {
			b.WriteString(",")
		}
	}

	b.WriteString("]")

	return true, nil
}

// mapStruct - maps all variables contained in the JSON struct
func (s *Serializer) mapStruct(item interface{}, b *strings.Builder, varSequence *[]string, variablePaths map[string]struct{}, path string) error {

	v := reflect.ValueOf(item)
	t := reflect.TypeOf(item)
	numFields := t.NumField()

	for i := 0; i < numFields; i++ {

		field := t.Field(i)

		if field.Type.Kind() == reflect.Struct {

			isSubObject, _, currentPath := s.fieldToProperty(&field, b, varSequence, variablePaths, path)
			if isSubObject {
				b.WriteString("{")
			}

			x := v.Field(i).Interface()
			err := s.mapStruct(x, b, varSequence, variablePaths, currentPath)
			if err != nil {
				return err
			}

			if isSubObject {
				b.WriteString("}")
			}

			if i < numFields-1 {
				b.WriteString(",")
			}

			continue

		} else if field.Type.Kind() == reflect.Map {

			fv := v.Field(i)
			s.writeMapInStringFormat(&field, &fv, b, varSequence, variablePaths, path)
			if i < numFields-1 {
				b.WriteString(",")
			}

			continue

		} else if field.Type.Kind() == reflect.Array || field.Type.Kind() == reflect.Slice {

			fv := v.Field(i)
			s.writeArrayInStringFormat(&field, &fv, b, varSequence, variablePaths, path)
			if i < numFields-1 {
				b.WriteString(",")
			}

			continue
		}

		keep, varType, _ := s.fieldToProperty(&field, b, varSequence, variablePaths, path)
		if !keep {
			continue
		}

		if varType == propertyVariable {

			format, err := s.getFormatSymbol(field.Type.Kind())
			if err != nil {
				return err
			}

			b.WriteString(format)

		} else {

			vf := v.Field(i)
			value, err := s.getValueFromField(&field, &vf)
			if err != nil {
				return err
			}

			b.WriteString(value)
		}

		if i < numFields-1 {
			b.WriteString(",")
		}
	}

	return nil
}

// getFormatSymbol - returns the format from the struct field
func (s *Serializer) getFormatSymbol(k reflect.Kind) (string, error) {

	switch k {
	case reflect.String:
		return "%s", nil
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8, reflect.Uintptr, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		return "%d", nil
	case reflect.Float32, reflect.Float64:
		return "%f", nil
	case reflect.Bool:
		return "%t", nil
	default:
		return "", fmt.Errorf("type not mapped: %d", k)
	}
}

// getValueFromField - returns the value from the struct field
func (s *Serializer) getValueFromField(field *reflect.StructField, value *reflect.Value) (string, error) {

	var kind reflect.Kind
	if field == nil {
		kind = value.Type().Kind()
	} else {
		kind = field.Type.Kind()
	}

	switch kind {
	case reflect.String:
		var b strings.Builder
		str := value.String()
		b.Grow(len(str) + 2 + (strings.Count(str, `"`) * 2))
		s.writeStringValue(str, &b)
		return b.String(), nil
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8, reflect.Uintptr, reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		return strconv.FormatInt(value.Int(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(value.Float(), 'f', -1, 64), nil
	case reflect.Bool:
		return strconv.FormatBool(value.Bool()), nil
	case reflect.Interface:
		iface := value.Interface()
		internalValue := reflect.ValueOf(iface)
		return s.getValueFromField(nil, &internalValue)
	default:
		return "", fmt.Errorf("kind not mapped: %s", kind.String())
	}
}

// writeStringValue - writes a string in JSON format
func (s *Serializer) writeStringValue(value string, b *strings.Builder) {

	b.WriteByte(doubleQuote)

	for _, c := range []byte(value) {
		if c == doubleQuote {
			b.WriteString(escapedDoubleQuote)
		} else {
			b.WriteByte(c)
		}
	}

	b.WriteByte(doubleQuote)
}

// writePropertyString - writes a string in JSON format
func (s *Serializer) writePropertyString(name string, b *strings.Builder) {

	s.writeStringValue(name, b)
	b.WriteString(":")
}

// fieldToProperty - try to write a property, returns if it's a json property, the type and the current path
func (s *Serializer) fieldToProperty(field *reflect.StructField, b *strings.Builder, varSequence *[]string, variablePaths map[string]struct{}, path string) (bool, variableType, string) {

	tag, ok := field.Tag.Lookup("json")
	if !ok {
		return false, normalValue, path
	}

	tagValues := strings.Split(tag, ",")

	s.writePropertyString(tagValues[0], b)

	propertyPath := s.buildPath(path, tagValues[0])
	varType := normalValue

	if _, ok := variablePaths[propertyPath]; ok {
		varType = propertyVariable
		*varSequence = append(*varSequence, propertyPath)
	}

	return true, varType, propertyPath
}

// buildPath - builds a new path
func (s *Serializer) buildPath(path, new string) string {

	var temp strings.Builder
	temp.Grow(len(path) + len(new) + 1)
	temp.WriteString(path)
	if len(path) > 0 {
		temp.WriteString(".")
	}
	temp.WriteString(new)

	return temp.String()
}
