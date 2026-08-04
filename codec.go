package masterkeeper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

type StructPlaceholder struct {
	StructName string
	Fields     map[string]any
}

// Marshal serializes a struct to binary format.
func Marshal(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	reflectValue := reflect.ValueOf(value)
	for reflectValue.Kind() == reflect.Ptr || reflectValue.Kind() == reflect.Interface {
		if reflectValue.IsNil() {
			return nil, nil
		}
		reflectValue = reflectValue.Elem()
	}
	if reflectValue.Kind() != reflect.Struct {
		return nil, errors.New("only structs can be serialized at the top level")
	}

	var buffer bytes.Buffer
	if error := writeStructFields(&buffer, reflectValue); error != nil {
		return nil, error
	}
	return buffer.Bytes(), nil
}

// Unmarshal deserializes binary data into a struct pointer.
func Unmarshal(data []byte, targetPointer any) error {
	if len(data) == 0 {
		return nil
	}
	targetReflectValue := reflect.ValueOf(targetPointer)
	if targetReflectValue.Kind() != reflect.Ptr || targetReflectValue.IsNil() {
		return errors.New("unmarshal target must be a non-nil pointer")
	}
	baseReflectValue := targetReflectValue.Elem()
	if baseReflectValue.Kind() != reflect.Struct {
		return errors.New("unmarshal target must be a pointer to a struct")
	}

	reader := bytes.NewReader(data)
	var count int32
	if error := binary.Read(reader, binary.BigEndian, &count); error != nil {
		return error
	}

	fields := make(map[string]any)
	for index := 0; index < int(count); index++ {
		name, error := readString(reader)
		if error != nil {
			return error
		}
		fieldValue, error := readValue(reader)
		if error != nil {
			return error
		}
		fields[name] = fieldValue
	}

	// Assign fields
	metadata := getStructMetadata(baseReflectValue.Type())
	if metadata == nil {
		return errors.New("failed to get metadata")
	}

	for keyName, fieldValue := range fields {
		fieldInfo, found := metadata.fields[strings.ToLower(keyName)]
		if found {
			castReflectValue, error := castValue(fieldValue, baseReflectValue.Field(fieldInfo.index).Type())
			if error != nil {
				return fmt.Errorf("failed to cast field %s: %w", fieldInfo.name, error)
			}
			baseReflectValue.Field(fieldInfo.index).Set(castReflectValue)
		}
	}
	return nil
}

func writeString(writer io.Writer, strValue string) error {
	byteSlice := []byte(strValue)
	if error := binary.Write(writer, binary.BigEndian, int32(len(byteSlice))); error != nil {
		return error
	}
	_, error := writer.Write(byteSlice)
	return error
}

func readString(reader io.Reader) (string, error) {
	var length int32
	if error := binary.Read(reader, binary.BigEndian, &length); error != nil {
		return "", error
	}
	if length < 0 || length > 1024*1024*16 {
		return "", fmt.Errorf("corrupt data: invalid string length %d", length)
	}
	byteSlice := make([]byte, length)
	if _, error := io.ReadFull(reader, byteSlice); error != nil {
		return "", error
	}
	return string(byteSlice), nil
}

func writeStructFields(writer io.Writer, reflectValue reflect.Value) error {
	metadata := getStructMetadata(reflectValue.Type())
	if metadata == nil {
		return errors.New("failed to get metadata")
	}

	if error := binary.Write(writer, binary.BigEndian, int32(len(metadata.marshalFields))); error != nil {
		return error
	}

	for _, fieldInfo := range metadata.marshalFields {
		if error := writeString(writer, fieldInfo.name); error != nil {
			return error
		}
		if error := writeValue(writer, reflectValue.Field(fieldInfo.index)); error != nil {
			return error
		}
	}
	return nil
}

func writeValue(writer io.Writer, reflectValue reflect.Value) error {
	if !reflectValue.IsValid() {
		return binary.Write(writer, binary.BigEndian, byte(0)) // TAG_NULL
	}

	for reflectValue.Kind() == reflect.Ptr || reflectValue.Kind() == reflect.Interface {
		if reflectValue.IsNil() {
			return binary.Write(writer, binary.BigEndian, byte(0)) // TAG_NULL
		}
		reflectValue = reflectValue.Elem()
	}

	switch reflectValue.Kind() {
	case reflect.String:
		if error := binary.Write(writer, binary.BigEndian, byte(1)); error != nil { // TAG_STRING
			return error
		}
		return writeString(writer, reflectValue.String())

	case reflect.Slice:
		if reflectValue.Type().Elem().Kind() == reflect.Uint8 {
			if error := binary.Write(writer, binary.BigEndian, byte(2)); error != nil { // TAG_BYTES
				return error
			}
			byteSlice := reflectValue.Bytes()
			if error := binary.Write(writer, binary.BigEndian, int32(len(byteSlice))); error != nil {
				return error
			}
			_, error := writer.Write(byteSlice)
			return error
		}
		return errors.New("unsupported slice type in codec")

	case reflect.Struct:
		if reflectValue.Type().String() == "time.Time" {
			timeValue := reflectValue.Interface().(time.Time)
			if error := binary.Write(writer, binary.BigEndian, byte(3)); error != nil { // TAG_TIME
				return error
			}
			if error := binary.Write(writer, binary.BigEndian, timeValue.Unix()); error != nil {
				return error
			}
			return binary.Write(writer, binary.BigEndian, int32(timeValue.Nanosecond()))
		}

		if error := binary.Write(writer, binary.BigEndian, byte(9)); error != nil { // TAG_STRUCT
			return error
		}
		structName := reflectValue.Type().String()
		if error := writeString(writer, structName); error != nil {
			return error
		}
		return writeStructFields(writer, reflectValue)

	case reflect.Int:
		if error := binary.Write(writer, binary.BigEndian, byte(5)); error != nil {
			return error
		}
		return binary.Write(writer, binary.BigEndian, reflectValue.Int())

	case reflect.Int8, reflect.Int16, reflect.Int32:
		if error := binary.Write(writer, binary.BigEndian, byte(4)); error != nil {
			return error
		}
		return binary.Write(writer, binary.BigEndian, int32(reflectValue.Int()))

	case reflect.Int64:
		if error := binary.Write(writer, binary.BigEndian, byte(5)); error != nil {
			return error
		}
		return binary.Write(writer, binary.BigEndian, reflectValue.Int())

	case reflect.Float32, reflect.Float64:
		if error := binary.Write(writer, binary.BigEndian, byte(6)); error != nil { // TAG_FLOAT64
			return error
		}
		return binary.Write(writer, binary.BigEndian, reflectValue.Float())

	case reflect.Bool:
		if error := binary.Write(writer, binary.BigEndian, byte(7)); error != nil { // TAG_BOOL
			return error
		}
		var byteValue byte = 0
		if reflectValue.Bool() {
			byteValue = 1
		}
		return binary.Write(writer, binary.BigEndian, byteValue)

	default:
		return fmt.Errorf("unsupported type %s in codec", reflectValue.Type().String())
	}
}

func readValue(reader io.Reader) (any, error) {
	var tag byte
	if error := binary.Read(reader, binary.BigEndian, &tag); error != nil {
		return nil, error
	}

	switch tag {
	case 0: // TAG_NULL
		return nil, nil
	case 1: // TAG_STRING
		return readString(reader)
	case 2: // TAG_BYTES
		var length int32
		if error := binary.Read(reader, binary.BigEndian, &length); error != nil {
			return nil, error
		}
		byteSlice := make([]byte, length)
		if _, error := io.ReadFull(reader, byteSlice); error != nil {
			return nil, error
		}
		return byteSlice, nil
	case 3: // TAG_TIME
		var seconds int64
		var nanoseconds int32
		if error := binary.Read(reader, binary.BigEndian, &seconds); error != nil {
			return nil, error
		}
		if error := binary.Read(reader, binary.BigEndian, &nanoseconds); error != nil {
			return nil, error
		}
		return time.Unix(seconds, int64(nanoseconds)), nil
	case 4: // TAG_INT32
		var integerValue int32
		if error := binary.Read(reader, binary.BigEndian, &integerValue); error != nil {
			return nil, error
		}
		return integerValue, nil
	case 5: // TAG_INT64
		var integerValue int64
		if error := binary.Read(reader, binary.BigEndian, &integerValue); error != nil {
			return nil, error
		}
		return integerValue, nil
	case 6: // TAG_FLOAT64
		var floatValue float64
		if error := binary.Read(reader, binary.BigEndian, &floatValue); error != nil {
			return nil, error
		}
		return floatValue, nil
	case 7: // TAG_BOOL
		var byteValue byte
		if error := binary.Read(reader, binary.BigEndian, &byteValue); error != nil {
			return nil, error
		}
		return byteValue != 0, nil
	case 9: // TAG_STRUCT
		structName, error := readString(reader)
		if error != nil {
			return nil, error
		}
		var count int32
		if error := binary.Read(reader, binary.BigEndian, &count); error != nil {
			return nil, error
		}
		fields := make(map[string]any)
		for index := 0; index < int(count); index++ {
			name, error := readString(reader)
			if error != nil {
				return nil, error
			}
			fieldValue, error := readValue(reader)
			if error != nil {
				return nil, error
			}
			fields[name] = fieldValue
		}
		return &StructPlaceholder{StructName: structName, Fields: fields}, nil
	default:
		return nil, fmt.Errorf("unknown tag %d in codec", tag)
	}
}

func castSignedInteger(value any, targetType reflect.Type) (reflect.Value, error) {
	var number int64

	switch intValue := value.(type) {
	case int32:
		number = int64(intValue)
	case int64:
		number = intValue
	case int:
		number = int64(intValue)
	default:
		return reflect.Value{}, fmt.Errorf(
			"cannot cast %T to %s",
			value,
			targetType,
		)
	}

	result := reflect.New(targetType).Elem()
	if result.OverflowInt(number) {
		return reflect.Value{}, fmt.Errorf(
			"integer %d overflows %s",
			number,
			targetType,
		)
	}

	result.SetInt(number)
	return result, nil
}

func castValue(value any, targetType reflect.Type) (reflect.Value, error) {
	if value == nil {
		return reflect.Zero(targetType), nil
	}

	isPointer := targetType.Kind() == reflect.Ptr
	var baseType reflect.Type = targetType
	if isPointer {
		baseType = targetType.Elem()
	}

	reflectValue := reflect.ValueOf(value)

	if placeholder, found := value.(*StructPlaceholder); found {
		if baseType.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("cannot cast struct placeholder to %s", baseType.String())
		}
		newStruct := reflect.New(baseType).Elem()
		metadata := getStructMetadata(baseType)
		if metadata == nil {
			return reflect.Value{}, fmt.Errorf("failed to get metadata for %s", baseType.String())
		}

		for keyName, fieldValue := range placeholder.Fields {
			fieldInfo, foundField := metadata.fields[strings.ToLower(keyName)]
			if foundField {
				castReflectValue, error := castValue(fieldValue, baseType.Field(fieldInfo.index).Type)
				if error != nil {
					return reflect.Value{}, error
				}
				newStruct.Field(fieldInfo.index).Set(castReflectValue)
			}
		}
		return returnValue(newStruct, isPointer), nil
	}

	switch baseType.Kind() {
	case reflect.String:
		stringValue := ""
		if val, found := value.(string); found {
			stringValue = val
		} else {
			stringValue = fmt.Sprintf("%v", value)
		}
		return returnValue(reflect.ValueOf(stringValue), isPointer), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result, error := castSignedInteger(value, baseType)
		if error != nil {
			return reflect.Value{}, error
		}
		return returnValue(result, isPointer), nil

	case reflect.Float64:
		var floatValue float64
		if val, found := value.(float64); found {
			floatValue = val
		} else if float32Value, found := value.(float32); found {
			floatValue = float64(float32Value)
		} else if int32Value, found := value.(int32); found {
			floatValue = float64(int32Value)
		} else if int64Value, found := value.(int64); found {
			floatValue = float64(int64Value)
		} else if integerValue, found := value.(int); found {
			floatValue = float64(integerValue)
		}
		return returnValue(reflect.ValueOf(floatValue), isPointer), nil

	case reflect.Bool:
		var boolValue bool
		if booleanValue, found := value.(bool); found {
			boolValue = booleanValue
		}
		return returnValue(reflect.ValueOf(boolValue), isPointer), nil

	case reflect.Slice:
		if baseType.Elem().Kind() == reflect.Uint8 {
			if byteSlice, found := value.([]byte); found {
				return returnValue(reflect.ValueOf(byteSlice), isPointer), nil
			}
		}

	case reflect.Struct:
		if baseType.String() == "time.Time" {
			if timeValue, found := value.(time.Time); found {
				return returnValue(reflect.ValueOf(timeValue), isPointer), nil
			}
		}
	}

	if reflectValue.Type().AssignableTo(baseType) {
		return returnValue(reflectValue, isPointer), nil
	}

	if reflectValue.Type().ConvertibleTo(baseType) {
		return returnValue(reflectValue.Convert(baseType), isPointer), nil
	}

	return reflect.Value{}, fmt.Errorf("cannot cast %T to %s", value, targetType.String())
}

func returnValue(value reflect.Value, isPointer bool) reflect.Value {
	if isPointer {
		pointerValue := reflect.New(value.Type())
		pointerValue.Elem().Set(value)
		return pointerValue
	}
	return value
}

type FieldMeta struct {
	FieldName string
	IsID      bool
	IsIndex   bool
	IsUnique  bool
	IsOrdered bool
}

func parseFieldTag(structField reflect.StructField) FieldMeta {
	fieldMetadata := FieldMeta{
		FieldName: structField.Name,
	}
	tag := structField.Tag.Get("keeper")
	if tag == "" {
		if strings.ToUpper(structField.Name) == "ID" {
			fieldMetadata.IsID = true
		}
		return fieldMetadata
	}

	parts := strings.Split(tag, ",")
	for index, part := range parts {
		part = strings.TrimSpace(part)
		if index == 0 && part != "id" && part != "index" && part != "unique" && part != "ordered" {
			fieldMetadata.FieldName = part
			continue
		}
		switch part {
		case "id":
			fieldMetadata.IsID = true
		case "index":
			fieldMetadata.IsIndex = true
		case "unique":
			fieldMetadata.IsUnique = true
			fieldMetadata.IsIndex = true
		case "ordered":
			fieldMetadata.IsOrdered = true
		}
	}
	return fieldMetadata
}

func getFieldName(structField reflect.StructField) string {
	fieldMetadata := parseFieldTag(structField)
	return fieldMetadata.FieldName
}
