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
	if err := writeStructFields(&buffer, reflectValue); err != nil {
		return nil, err
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
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return err
	}

	fields := make(map[string]any)
	for index := 0; index < int(count); index++ {
		name, err := readString(reader)
		if err != nil {
			return err
		}
		fieldValue, err := readValue(reader)
		if err != nil {
			return err
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
			castReflectValue, err := castValue(fieldValue, baseReflectValue.Field(fieldInfo.index).Type())
			if err != nil {
				return fmt.Errorf("failed to cast field %s: %w", fieldInfo.name, err)
			}
			baseReflectValue.Field(fieldInfo.index).Set(castReflectValue)
		}
	}
	return nil
}

func writeString(writer io.Writer, strValue string) error {
	byteSlice := []byte(strValue)
	if err := binary.Write(writer, binary.BigEndian, int32(len(byteSlice))); err != nil {
		return err
	}
	_, err := writer.Write(byteSlice)
	return err
}

func readString(reader io.Reader) (string, error) {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}
	byteSlice := make([]byte, length)
	if _, err := io.ReadFull(reader, byteSlice); err != nil {
		return "", err
	}
	return string(byteSlice), nil
}

func writeStructFields(writer io.Writer, reflectValue reflect.Value) error {
	metadata := getStructMetadata(reflectValue.Type())
	if metadata == nil {
		return errors.New("failed to get metadata")
	}

	if err := binary.Write(writer, binary.BigEndian, int32(len(metadata.marshalFields))); err != nil {
		return err
	}

	for _, fieldInfo := range metadata.marshalFields {
		if err := writeString(writer, fieldInfo.name); err != nil {
			return err
		}
		if err := writeValue(writer, reflectValue.Field(fieldInfo.index)); err != nil {
			return err
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
		if err := binary.Write(writer, binary.BigEndian, byte(1)); err != nil { // TAG_STRING
			return err
		}
		return writeString(writer, reflectValue.String())

	case reflect.Slice:
		if reflectValue.Type().Elem().Kind() == reflect.Uint8 {
			if err := binary.Write(writer, binary.BigEndian, byte(2)); err != nil { // TAG_BYTES
				return err
			}
			byteSlice := reflectValue.Bytes()
			if err := binary.Write(writer, binary.BigEndian, int32(len(byteSlice))); err != nil {
				return err
			}
			_, err := writer.Write(byteSlice)
			return err
		}
		return errors.New("unsupported slice type in codec")

	case reflect.Struct:
		if reflectValue.Type().String() == "time.Time" {
			timeVal := reflectValue.Interface().(time.Time)
			if err := binary.Write(writer, binary.BigEndian, byte(3)); err != nil { // TAG_TIME
				return err
			}
			if err := binary.Write(writer, binary.BigEndian, timeVal.Unix()); err != nil {
				return err
			}
			return binary.Write(writer, binary.BigEndian, int32(timeVal.Nanosecond()))
		}

		if err := binary.Write(writer, binary.BigEndian, byte(9)); err != nil { // TAG_STRUCT
			return err
		}
		structName := reflectValue.Type().String()
		if err := writeString(writer, structName); err != nil {
			return err
		}
		return writeStructFields(writer, reflectValue)

	case reflect.Int:
		if err := binary.Write(writer, binary.BigEndian, byte(5)); err != nil {
			return err
		}
		return binary.Write(writer, binary.BigEndian, reflectValue.Int())

	case reflect.Int8, reflect.Int16, reflect.Int32:
		if err := binary.Write(writer, binary.BigEndian, byte(4)); err != nil {
			return err
		}
		return binary.Write(writer, binary.BigEndian, int32(reflectValue.Int()))

	case reflect.Int64:
		if err := binary.Write(writer, binary.BigEndian, byte(5)); err != nil {
			return err
		}
		return binary.Write(writer, binary.BigEndian, reflectValue.Int())

	case reflect.Float32, reflect.Float64:
		if err := binary.Write(writer, binary.BigEndian, byte(6)); err != nil { // TAG_FLOAT64
			return err
		}
		return binary.Write(writer, binary.BigEndian, reflectValue.Float())

	case reflect.Bool:
		if err := binary.Write(writer, binary.BigEndian, byte(7)); err != nil { // TAG_BOOL
			return err
		}
		var byteVal byte = 0
		if reflectValue.Bool() {
			byteVal = 1
		}
		return binary.Write(writer, binary.BigEndian, byteVal)

	default:
		return fmt.Errorf("unsupported type %s in codec", reflectValue.Type().String())
	}
}

func readValue(reader io.Reader) (any, error) {
	var tag byte
	if err := binary.Read(reader, binary.BigEndian, &tag); err != nil {
		return nil, err
	}

	switch tag {
	case 0: // TAG_NULL
		return nil, nil
	case 1: // TAG_STRING
		return readString(reader)
	case 2: // TAG_BYTES
		var length int32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		byteSlice := make([]byte, length)
		if _, err := io.ReadFull(reader, byteSlice); err != nil {
			return nil, err
		}
		return byteSlice, nil
	case 3: // TAG_TIME
		var seconds int64
		var nanoseconds int32
		if err := binary.Read(reader, binary.BigEndian, &seconds); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &nanoseconds); err != nil {
			return nil, err
		}
		return time.Unix(seconds, int64(nanoseconds)), nil
	case 4: // TAG_INT32
		var intVal int32
		if err := binary.Read(reader, binary.BigEndian, &intVal); err != nil {
			return nil, err
		}
		return intVal, nil
	case 5: // TAG_INT64
		var intVal int64
		if err := binary.Read(reader, binary.BigEndian, &intVal); err != nil {
			return nil, err
		}
		return intVal, nil
	case 6: // TAG_FLOAT64
		var floatVal float64
		if err := binary.Read(reader, binary.BigEndian, &floatVal); err != nil {
			return nil, err
		}
		return floatVal, nil
	case 7: // TAG_BOOL
		var byteVal byte
		if err := binary.Read(reader, binary.BigEndian, &byteVal); err != nil {
			return nil, err
		}
		return byteVal != 0, nil
	case 9: // TAG_STRUCT
		structName, err := readString(reader)
		if err != nil {
			return nil, err
		}
		var count int32
		if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
			return nil, err
		}
		fields := make(map[string]any)
		for index := 0; index < int(count); index++ {
			name, err := readString(reader)
			if err != nil {
				return nil, err
			}
			fieldValue, err := readValue(reader)
			if err != nil {
				return nil, err
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
				castReflectValue, err := castValue(fieldValue, baseType.Field(fieldInfo.index).Type)
				if err != nil {
					return reflect.Value{}, err
				}
				newStruct.Field(fieldInfo.index).Set(castReflectValue)
			}
		}
		return returnVal(newStruct, isPointer), nil
	}

	switch baseType.Kind() {
	case reflect.String:
		stringValue := ""
		if strVal, found := value.(string); found {
			stringValue = strVal
		} else {
			stringValue = fmt.Sprintf("%v", value)
		}
		return returnVal(reflect.ValueOf(stringValue), isPointer), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result, err := castSignedInteger(value, baseType)
		if err != nil {
			return reflect.Value{}, err
		}
		return returnVal(result, isPointer), nil

	case reflect.Float64:
		var floatValue float64
		if floatVal, found := value.(float64); found {
			floatValue = floatVal
		} else if float32Val, found := value.(float32); found {
			floatValue = float64(float32Val)
		} else if int32Val, found := value.(int32); found {
			floatValue = float64(int32Val)
		} else if int64Val, found := value.(int64); found {
			floatValue = float64(int64Val)
		} else if intVal, found := value.(int); found {
			floatValue = float64(intVal)
		}
		return returnVal(reflect.ValueOf(floatValue), isPointer), nil

	case reflect.Bool:
		var boolValue bool
		if boolVal, found := value.(bool); found {
			boolValue = boolVal
		}
		return returnVal(reflect.ValueOf(boolValue), isPointer), nil

	case reflect.Slice:
		if baseType.Elem().Kind() == reflect.Uint8 {
			if byteSlice, found := value.([]byte); found {
				return returnVal(reflect.ValueOf(byteSlice), isPointer), nil
			}
		}

	case reflect.Struct:
		if baseType.String() == "time.Time" {
			if timeVal, found := value.(time.Time); found {
				return returnVal(reflect.ValueOf(timeVal), isPointer), nil
			}
		}
	}

	if reflectValue.Type().AssignableTo(baseType) {
		return returnVal(reflectValue, isPointer), nil
	}

	if reflectValue.Type().ConvertibleTo(baseType) {
		return returnVal(reflectValue.Convert(baseType), isPointer), nil
	}

	return reflect.Value{}, fmt.Errorf("cannot cast %T to %s", value, targetType.String())
}

func returnVal(value reflect.Value, isPointer bool) reflect.Value {
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
