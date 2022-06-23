package zetasqlite

import (
	"database/sql/driver"

	"github.com/goccy/go-zetasql/types"
)

type Rows struct {
	rows    driver.Rows
	columns []*ColumnSpec
}

func (r *Rows) Columns() []string {
	colNames := make([]string, 0, len(r.columns))
	for _, col := range r.columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

func (r *Rows) Close() error {
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
	colTypes, err := r.columnTypes()
	if err != nil {
		return err
	}
	values := make([]driver.Value, len(colTypes))
	retErr := r.rows.Next(values)
	for idx, colType := range colTypes {
		value, err := r.convertValue(values[idx], colType)
		if err != nil {
			return err
		}
		dest[idx] = value
	}
	return retErr
}

func (r *Rows) convertValue(value driver.Value, typ *Type) (driver.Value, error) {
	if typ.IsArray() {
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
		case types.STRING:
			var v []string
			for _, value := range array.values {
				sv, err := value.ToString()
				if err != nil {
					return nil, err
				}
				v = append(v, sv)
			}
			return v, nil
		}
	}
	return value, nil
}
