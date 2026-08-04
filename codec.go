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
func Marshal(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return nil, nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, errors.New("only structs can be serialized at the top level")
	}

	var buf bytes.Buffer
	if err := writeStructFields(&buf, val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal deserializes binary data into a struct pointer.
func Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	targetVal := reflect.ValueOf(v)
	if targetVal.Kind() != reflect.Ptr || targetVal.IsNil() {
		return errors.New("unmarshal target must be a non-nil pointer")
	}
	baseVal := targetVal.Elem()
	if baseVal.Kind() != reflect.Struct {
		return errors.New("unmarshal target must be a pointer to a struct")
	}

	r := bytes.NewReader(data)
	var count int32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return err
	}

	fields := make(map[string]any)
	for i := 0; i < int(count); i++ {
		name, err := readString(r)
		if err != nil {
			return err
		}
		val, err := readValue(r)
		if err != nil {
			return err
		}
		fields[name] = val
	}

	// Assign fields
	meta := getStructMetadata(baseVal.Type())
	if meta == nil {
		return errors.New("failed to get metadata")
	}

	for k, val := range fields {
		info, ok := meta.fields[strings.ToLower(k)]
		if ok {
			fieldVal, err := castValue(val, baseVal.Field(info.index).Type())
			if err != nil {
				return fmt.Errorf("failed to cast field %s: %w", info.name, err)
			}
			baseVal.Field(info.index).Set(fieldVal)
		}
	}
	return nil
}

func writeString(w io.Writer, s string) error {
	b := []byte(s)
	if err := binary.Write(w, binary.BigEndian, int32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readString(r io.Reader) (string, error) {
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func writeStructFields(w io.Writer, val reflect.Value) error {
	meta := getStructMetadata(val.Type())
	if meta == nil {
		return errors.New("failed to get metadata")
	}

	if err := binary.Write(w, binary.BigEndian, int32(len(meta.marshalFields))); err != nil {
		return err
	}

	for _, fieldInfo := range meta.marshalFields {
		if err := writeString(w, fieldInfo.name); err != nil {
			return err
		}
		if err := writeValue(w, val.Field(fieldInfo.index)); err != nil {
			return err
		}
	}
	return nil
}

func writeValue(w io.Writer, val reflect.Value) error {
	if !val.IsValid() {
		return binary.Write(w, binary.BigEndian, byte(0)) // TAG_NULL
	}

	for val.Kind() == reflect.Ptr || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return binary.Write(w, binary.BigEndian, byte(0)) // TAG_NULL
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.String:
		if err := binary.Write(w, binary.BigEndian, byte(1)); err != nil { // TAG_STRING
			return err
		}
		return writeString(w, val.String())

	case reflect.Slice:
		if val.Type().Elem().Kind() == reflect.Uint8 {
			if err := binary.Write(w, binary.BigEndian, byte(2)); err != nil { // TAG_BYTES
				return err
			}
			b := val.Bytes()
			if err := binary.Write(w, binary.BigEndian, int32(len(b))); err != nil {
				return err
			}
			_, err := w.Write(b)
			return err
		}
		return errors.New("unsupported slice type in codec")

	case reflect.Struct:
		if val.Type().String() == "time.Time" {
			t := val.Interface().(time.Time)
			if err := binary.Write(w, binary.BigEndian, byte(3)); err != nil { // TAG_TIME
				return err
			}
			if err := binary.Write(w, binary.BigEndian, t.Unix()); err != nil {
				return err
			}
			return binary.Write(w, binary.BigEndian, int32(t.Nanosecond()))
		}

		if err := binary.Write(w, binary.BigEndian, byte(9)); err != nil { // TAG_STRUCT
			return err
		}
		structName := val.Type().String()
		if err := writeString(w, structName); err != nil {
			return err
		}
		return writeStructFields(w, val)

	case reflect.Int:
		if err := binary.Write(w, binary.BigEndian, byte(5)); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, val.Int())

	case reflect.Int8, reflect.Int16, reflect.Int32:
		if err := binary.Write(w, binary.BigEndian, byte(4)); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, int32(val.Int()))

	case reflect.Int64:
		if err := binary.Write(w, binary.BigEndian, byte(5)); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, val.Int())

	case reflect.Float32, reflect.Float64:
		if err := binary.Write(w, binary.BigEndian, byte(6)); err != nil { // TAG_FLOAT64
			return err
		}
		return binary.Write(w, binary.BigEndian, val.Float())

	case reflect.Bool:
		if err := binary.Write(w, binary.BigEndian, byte(7)); err != nil { // TAG_BOOL
			return err
		}
		var b byte = 0
		if val.Bool() {
			b = 1
		}
		return binary.Write(w, binary.BigEndian, b)

	default:
		return fmt.Errorf("unsupported type %s in codec", val.Type().String())
	}
}

func readValue(r io.Reader) (any, error) {
	var tag byte
	if err := binary.Read(r, binary.BigEndian, &tag); err != nil {
		return nil, err
	}

	switch tag {
	case 0: // TAG_NULL
		return nil, nil
	case 1: // TAG_STRING
		return readString(r)
	case 2: // TAG_BYTES
		var length int32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			return nil, err
		}
		b := make([]byte, length)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}
		return b, nil
	case 3: // TAG_TIME
		var sec int64
		var nsec int32
		if err := binary.Read(r, binary.BigEndian, &sec); err != nil {
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &nsec); err != nil {
			return nil, err
		}
		return time.Unix(sec, int64(nsec)), nil
	case 4: // TAG_INT32
		var val int32
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil
	case 5: // TAG_INT64
		var val int64
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil
	case 6: // TAG_FLOAT64
		var val float64
		if err := binary.Read(r, binary.BigEndian, &val); err != nil {
			return nil, err
		}
		return val, nil
	case 7: // TAG_BOOL
		var b byte
		if err := binary.Read(r, binary.BigEndian, &b); err != nil {
			return nil, err
		}
		return b != 0, nil
	case 9: // TAG_STRUCT
		structName, err := readString(r)
		if err != nil {
			return nil, err
		}
		var count int32
		if err := binary.Read(r, binary.BigEndian, &count); err != nil {
			return nil, err
		}
		fields := make(map[string]any)
		for i := 0; i < int(count); i++ {
			name, err := readString(r)
			if err != nil {
				return nil, err
			}
			val, err := readValue(r)
			if err != nil {
				return nil, err
			}
			fields[name] = val
		}
		return &StructPlaceholder{StructName: structName, Fields: fields}, nil
	default:
		return nil, fmt.Errorf("unknown tag %d in codec", tag)
	}
}

func castSignedInteger(value any, targetType reflect.Type) (reflect.Value, error) {
	var number int64

	switch v := value.(type) {
	case int32:
		number = int64(v)
	case int64:
		number = v
	case int:
		number = int64(v)
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

func castValue(val any, targetType reflect.Type) (reflect.Value, error) {
	if val == nil {
		return reflect.Zero(targetType), nil
	}

	isPtr := targetType.Kind() == reflect.Ptr
	var baseType reflect.Type = targetType
	if isPtr {
		baseType = targetType.Elem()
	}

	valVal := reflect.ValueOf(val)

	if ph, ok := val.(*StructPlaceholder); ok {
		if baseType.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("cannot cast struct placeholder to %s", baseType.String())
		}
		newStruct := reflect.New(baseType).Elem()
		meta := getStructMetadata(baseType)
		if meta == nil {
			return reflect.Value{}, fmt.Errorf("failed to get metadata for %s", baseType.String())
		}

		for k, v := range ph.Fields {
			info, ok := meta.fields[strings.ToLower(k)]
			if ok {
				fieldVal, err := castValue(v, baseType.Field(info.index).Type)
				if err != nil {
					return reflect.Value{}, err
				}
				newStruct.Field(info.index).Set(fieldVal)
			}
		}
		return returnVal(newStruct, isPtr), nil
	}

	switch baseType.Kind() {
	case reflect.String:
		str := ""
		if s, ok := val.(string); ok {
			str = s
		} else {
			str = fmt.Sprintf("%v", val)
		}
		return returnVal(reflect.ValueOf(str), isPtr), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		result, err := castSignedInteger(val, baseType)
		if err != nil {
			return reflect.Value{}, err
		}
		return returnVal(result, isPtr), nil

	case reflect.Float64:
		var f float64
		if v, ok := val.(float64); ok {
			f = v
		} else if v, ok := val.(float32); ok {
			f = float64(v)
		} else if v, ok := val.(int32); ok {
			f = float64(v)
		} else if v, ok := val.(int64); ok {
			f = float64(v)
		} else if v, ok := val.(int); ok {
			f = float64(v)
		}
		return returnVal(reflect.ValueOf(f), isPtr), nil

	case reflect.Bool:
		var b bool
		if v, ok := val.(bool); ok {
			b = v
		}
		return returnVal(reflect.ValueOf(b), isPtr), nil

	case reflect.Slice:
		if baseType.Elem().Kind() == reflect.Uint8 {
			if b, ok := val.([]byte); ok {
				return returnVal(reflect.ValueOf(b), isPtr), nil
			}
		}

	case reflect.Struct:
		if baseType.String() == "time.Time" {
			if t, ok := val.(time.Time); ok {
				return returnVal(reflect.ValueOf(t), isPtr), nil
			}
		}
	}

	if valVal.Type().AssignableTo(baseType) {
		return returnVal(valVal, isPtr), nil
	}

	if valVal.Type().ConvertibleTo(baseType) {
		return returnVal(valVal.Convert(baseType), isPtr), nil
	}

	return reflect.Value{}, fmt.Errorf("cannot cast %T to %s", val, targetType.String())
}

func returnVal(v reflect.Value, isPtr bool) reflect.Value {
	if isPtr {
		ptr := reflect.New(v.Type())
		ptr.Elem().Set(v)
		return ptr
	}
	return v
}

type FieldMeta struct {
	FieldName string
	IsID      bool
	IsIndex   bool
	IsUnique  bool
	IsOrdered bool
}

func parseFieldTag(f reflect.StructField) FieldMeta {
	meta := FieldMeta{
		FieldName: f.Name,
	}
	tag := f.Tag.Get("keeper")
	if tag == "" {
		if strings.ToUpper(f.Name) == "ID" {
			meta.IsID = true
		}
		return meta
	}

	parts := strings.Split(tag, ",")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if i == 0 && part != "id" && part != "index" && part != "unique" && part != "ordered" {
			meta.FieldName = part
			continue
		}
		switch part {
		case "id":
			meta.IsID = true
		case "index":
			meta.IsIndex = true
		case "unique":
			meta.IsUnique = true
			meta.IsIndex = true
		case "ordered":
			meta.IsOrdered = true
		}
	}
	return meta
}

func getFieldName(f reflect.StructField) string {
	meta := parseFieldTag(f)
	return meta.FieldName
}
