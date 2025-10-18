package custom

import (
	"encoding/binary"
	"fmt"
	"math"
)

// TypeID представляет тип данных
type TypeID uint8

const (
	TypeInt32 TypeID = iota + 1
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeBool
	TypeString
	TypeMessage
	TypeList
)

// Value представляет значение любого поддерживаемого типа
type Value struct {
	Type    TypeID
	Int32   int32
	Int64   int64
	Float32 float32
	Float64 float64
	Bool    bool
	String  string
	Message []Field
	List    []Value
}

// Field представляет поле с номером и значением
type Field struct {
	Number uint32
	Value  Value
}

type EncodeError struct {
	Message string
}

func (e *EncodeError) Error() string {
	return e.Message
}

type DecodeError struct {
	Message string
}

func (e *DecodeError) Error() string {
	return e.Message
}

// Encode кодирует значение в бинарный формат
func Encode(value Value) ([]byte, error) {
	// Записываем тип
	buf := []byte{byte(value.Type)}

	// В зависимости от типа записываем значение
	switch value.Type {
	case TypeInt32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(value.Int32))
		buf = append(buf, b...)

	case TypeInt64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(value.Int64))
		buf = append(buf, b...)

	case TypeFloat32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, math.Float32bits(value.Float32))
		buf = append(buf, b...)

	case TypeFloat64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(value.Float64))
		buf = append(buf, b...)

	case TypeBool:
		if value.Bool {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}

	case TypeString:
		// Записываем длину строки
		strLen := len(value.String)
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(strLen))
		buf = append(buf, lenBuf...)
		// Записываем строку
		buf = append(buf, value.String...)

	case TypeMessage:
		// Записываем количество полей
		countBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(countBuf, uint32(len(value.Message)))
		buf = append(buf, countBuf...)

		// Записываем поля
		for _, field := range value.Message {
			// Номер поля
			numBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(numBuf, field.Number)
			buf = append(buf, numBuf...)

			// Значение поля
			fieldData, err := Encode(field.Value)
			if err != nil {
				return nil, err
			}
			buf = append(buf, fieldData...)
		}

	case TypeList:
		if len(value.List) == 0 {
			return nil, &EncodeError{Message: "empty list"}
		}

		// Записываем количество элементов
		countBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(countBuf, uint32(len(value.List)))
		buf = append(buf, countBuf...)

		// Записываем элементы
		for _, item := range value.List {
			itemData, err := Encode(item)
			if err != nil {
				return nil, err
			}
			buf = append(buf, itemData...)
		}
	}

	return buf, nil
}

// Decode декодирует значение из бинарного формата
func Decode(data []byte) (Value, int, error) {
	if len(data) < 1 {
		return Value{}, 0, &DecodeError{Message: "insufficient data"}
	}

	valueType := TypeID(data[0])
	offset := 1

	switch valueType {
	case TypeInt32:
		if len(data[offset:]) < 4 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for int32"}
		}
		value := int32(binary.LittleEndian.Uint32(data[offset:]))
		return Value{Type: TypeInt32, Int32: value}, offset + 4, nil

	case TypeInt64:
		if len(data[offset:]) < 8 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for int64"}
		}
		value := int64(binary.LittleEndian.Uint64(data[offset:]))
		return Value{Type: TypeInt64, Int64: value}, offset + 8, nil

	case TypeFloat32:
		if len(data[offset:]) < 4 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for float32"}
		}
		bits := binary.LittleEndian.Uint32(data[offset:])
		value := math.Float32frombits(bits)
		return Value{Type: TypeFloat32, Float32: value}, offset + 4, nil

	case TypeFloat64:
		if len(data[offset:]) < 8 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for float64"}
		}
		bits := binary.LittleEndian.Uint64(data[offset:])
		value := math.Float64frombits(bits)
		return Value{Type: TypeFloat64, Float64: value}, offset + 8, nil

	case TypeBool:
		if len(data[offset:]) < 1 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for bool"}
		}
		value := data[offset] != 0
		return Value{Type: TypeBool, Bool: value}, offset + 1, nil

	case TypeString:
		if len(data[offset:]) < 4 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for string length"}
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if len(data[offset:]) < length {
			return Value{}, 0, &DecodeError{Message: "insufficient data for string content"}
		}
		value := string(data[offset : offset+length])
		return Value{Type: TypeString, String: value}, offset + length, nil

	case TypeMessage:
		if len(data[offset:]) < 4 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for message field count"}
		}
		fieldCount := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		fields := make([]Field, 0, fieldCount)

		for i := 0; i < fieldCount; i++ {
			if len(data[offset:]) < 4 {
				return Value{}, 0, &DecodeError{Message: "insufficient data for field number"}
			}
			number := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			value, n, err := Decode(data[offset:])
			if err != nil {
				return Value{}, 0, err
			}
			fields = append(fields, Field{Number: number, Value: value})
			offset += n
		}
		return Value{Type: TypeMessage, Message: fields}, offset, nil

	case TypeList:
		if len(data[offset:]) < 4 {
			return Value{}, 0, &DecodeError{Message: "insufficient data for list length"}
		}
		length := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		items := make([]Value, 0, length)

		for i := 0; i < length; i++ {
			value, n, err := Decode(data[offset:])
			if err != nil {
				return Value{}, 0, err
			}
			items = append(items, value)
			offset += n
		}
		return Value{Type: TypeList, List: items}, offset, nil

	default:
		return Value{}, 0, &DecodeError{Message: fmt.Sprintf("unknown type: %d", valueType)}
	}
}
