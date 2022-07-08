package internal

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"time"

	"github.com/goccy/go-zetasql/types"
)

type Rows struct {
	rows    *sql.Rows
	columns []*ColumnSpec
}

func (r *Rows) Columns() []string {
	colNames := make([]string, 0, len(r.columns))
	for _, col := range r.columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

func (r *Rows) ColumnTypeDatabaseTypeName(i int) string {
	return r.columns[i].Type.Name
}

func (r *Rows) Close() error {
	if r.rows == nil {
		return nil
	}
	return r.rows.Close()
}

func (r *Rows) columnTypes() ([]*Type, error) {
	types := make([]*Type, 0, len(r.columns))
	for _, col := range r.columns {
		types = append(types, col.Type)
	}
	return types, nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.rows == nil {
		return io.EOF
	}
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	if err := r.rows.Err(); err != nil {
		return err
	}
	colTypes, err := r.columnTypes()
	if err != nil {
		return err
	}
	values := make([]interface{}, 0, len(dest))
	for i := 0; i < len(dest); i++ {
		var v interface{}
		values = append(values, &v)
	}
	retErr := r.rows.Scan(values...)
	for idx, colType := range colTypes {
		v := reflect.ValueOf(values[idx]).Elem().Interface()
		value, err := r.convertValue(v, colType)
		if err != nil {
			return err
		}
		dest[idx] = value
	}
	return retErr
}

func (r *Rows) convertValue(value interface{}, typ *Type) (driver.Value, error) {
	if value == "NULL" || value == nil {
		return nil, nil
	}
	switch types.TypeKind(typ.Kind) {
	case types.BOOL:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		return val.ToBool()
	case types.ARRAY:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		array, err := val.ToArray()
		if err != nil {
			return nil, err
		}
		elementType, err := typ.ElementType.ToZetaSQLType()
		if err != nil {
			return nil, err
		}
		switch elementType.Kind() {
		case types.INT64:
			v := []int64{}
			for _, value := range array.values {
				if value == nil {
					// TODO: must be add nil to result values
					continue
				}
				iv, err := value.ToInt64()
				if err != nil {
					return nil, err
				}
				v = append(v, iv)
			}
			return v, nil
		case types.DOUBLE:
			v := []float64{}
			for _, value := range array.values {
				fv, err := value.ToFloat64()
				if err != nil {
					return nil, err
				}
				v = append(v, fv)
			}
			return v, nil
		case types.BOOL:
			v := []bool{}
			for _, value := range array.values {
				bv, err := value.ToBool()
				if err != nil {
					return nil, err
				}
				v = append(v, bv)
			}
			return v, nil
		case types.STRING:
			v := []string{}
			for _, value := range array.values {
				sv, err := value.ToString()
				if err != nil {
					return nil, err
				}
				v = append(v, sv)
			}
			return v, nil
		case types.DATE:
			v := []string{}
			for _, value := range array.values {
				date, err := value.ToJSON()
				if err != nil {
					return nil, err
				}
				v = append(v, date)
			}
			return v, nil
		case types.TIMESTAMP:
			v := []time.Time{}
			for _, value := range array.values {
				t, err := value.ToTime()
				if err != nil {
					return nil, err
				}
				v = append(v, t)
			}
			return v, nil
		case types.STRUCT:
			return array.Interface(), nil
		}
	case types.STRUCT:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		s, err := val.ToStruct()
		if err != nil {
			return nil, err
		}
		return s.m, nil
	case types.DATE:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		return val.ToJSON()
	case types.DATETIME:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		return val.ToJSON()
	case types.TIME:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		return val.ToJSON()
	case types.TIMESTAMP:
		val, err := ValueOf(value)
		if err != nil {
			return nil, err
		}
		return val.ToJSON()
	}
	return value, nil
}
