package zetasqlite

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	ast "github.com/goccy/go-zetasql/resolved_ast"
)

type WindowFuncOptionType int

const (
	WindowFuncOptionUnknown   WindowFuncOptionType = 0
	WindowFuncOptionUnitFrame WindowFuncOptionType = 1
	WindowFuncOptionStart     WindowFuncOptionType = 2
	WindowFuncOptionEnd       WindowFuncOptionType = 3
	WindowFuncOptionPartition WindowFuncOptionType = 4
	WindowFuncOptionRowID     WindowFuncOptionType = 5
	WindowFuncOptionOrderBy   WindowFuncOptionType = 6
)

type WindowFuncOption struct {
	Type  WindowFuncOptionType `json:"type"`
	Value interface{}          `json:"value"`
}

type WindowUnitFrameType int

const (
	WindowUnitFrameUnknown WindowUnitFrameType = 0
	WindowUnitFrameRows    WindowUnitFrameType = 1
	WindowUnitFrameRange   WindowUnitFrameType = 2
)

type WindowBoundaryType int

const (
	WindowBoundaryTypeUnknown    WindowBoundaryType = 0
	WindowUnboundedPrecedingType WindowBoundaryType = 1
	WindowOffsetPrecedingType    WindowBoundaryType = 2
	WindowCurrentRowType         WindowBoundaryType = 3
	WindowOffsetFollowingType    WindowBoundaryType = 4
	WindowUnboundedFollowingType WindowBoundaryType = 5
)

func getWindowFrameUnitOptionFuncSQL(frameUnit ast.FrameUnit) string {
	var typ WindowUnitFrameType
	switch frameUnit {
	case ast.FrameUnitRows:
		typ = WindowUnitFrameRows
	case ast.FrameUnitRange:
		typ = WindowUnitFrameRange
	}
	return fmt.Sprintf("zetasqlite_window_frame_unit(%d)", typ)
}

func getWindowBoundaryStartOptionFuncSQL(boundaryType ast.BoundaryType) string {
	var typ WindowBoundaryType
	switch boundaryType {
	case ast.UnboundedPrecedingType:
		typ = WindowUnboundedPrecedingType
	case ast.OffsetPrecedingType:
		typ = WindowOffsetPrecedingType
	case ast.CurrentRowType:
		typ = WindowCurrentRowType
	case ast.OffsetFollowingType:
		typ = WindowOffsetFollowingType
	case ast.UnboundedFollowingType:
		typ = WindowUnboundedFollowingType
	}
	return fmt.Sprintf("zetasqlite_window_boundary_start(%d)", typ)
}

func getWindowBoundaryEndOptionFuncSQL(boundaryType ast.BoundaryType) string {
	var typ WindowBoundaryType
	switch boundaryType {
	case ast.UnboundedPrecedingType:
		typ = WindowUnboundedPrecedingType
	case ast.OffsetPrecedingType:
		typ = WindowOffsetPrecedingType
	case ast.CurrentRowType:
		typ = WindowCurrentRowType
	case ast.OffsetFollowingType:
		typ = WindowOffsetFollowingType
	case ast.UnboundedFollowingType:
		typ = WindowUnboundedFollowingType
	}
	return fmt.Sprintf("zetasqlite_window_boundary_end(%d)", typ)
}

func getWindowPartitionOptionFuncSQL(column string) string {
	return fmt.Sprintf("zetasqlite_window_partition(%s)", column)
}

func getWindowRowIDOptionFuncSQL() string {
	return "zetasqlite_window_rowid(`rowid`)"
}

func getWindowOrderByOptionFuncSQL(column string) string {
	return fmt.Sprintf("zetasqlite_window_order_by(%s)", column)
}

func windowFrameUnitOptionFunc(frameUnit int64) string {
	b, _ := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionUnitFrame,
		Value: frameUnit,
	})
	return string(b)
}

func windowBoundaryStartOptionFunc(boundaryType int64) string {
	b, _ := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionStart,
		Value: boundaryType,
	})
	return string(b)
}

func windowBoundaryEndOptionFunc(boundaryType int64) string {
	b, _ := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionEnd,
		Value: boundaryType,
	})
	return string(b)
}

func windowPartitionOptionFunc(partition interface{}) string {
	b, _ := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionPartition,
		Value: partition,
	})
	return string(b)
}

func windowRowIDOptionFunc(id int64) string {
	b, _ := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionRowID,
		Value: id,
	})
	return string(b)
}

func windowOrderByOptionFunc(value interface{}) string {
	b, _ := json.Marshal(&WindowFuncOption{
		Type:  WindowFuncOptionOrderBy,
		Value: value,
	})
	return string(b)
}

type WindowFuncStatus struct {
	UnitFrame WindowUnitFrameType
	Start     WindowBoundaryType
	End       WindowBoundaryType
	Partition Value
	RowID     int64
	OrderBy   []Value
}

func parseWindowOptions(opts ...string) (*WindowFuncStatus, error) {
	var status WindowFuncStatus
	for _, opt := range opts {
		var v WindowFuncOption
		if err := json.Unmarshal([]byte(opt), &v); err != nil {
			continue
		}
		switch v.Type {
		case WindowFuncOptionUnitFrame:
			status.UnitFrame = WindowUnitFrameType(int64(v.Value.(float64)))
		case WindowFuncOptionStart:
			status.Start = WindowBoundaryType(int64(v.Value.(float64)))
		case WindowFuncOptionEnd:
			status.End = WindowBoundaryType(int64(v.Value.(float64)))
		case WindowFuncOptionPartition:
			value, err := ValueOf(v.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to Value: %w", v.Value, err)
			}
			status.Partition = value
		case WindowFuncOptionRowID:
			status.RowID = int64(v.Value.(float64))
		case WindowFuncOptionOrderBy:
			value, err := ValueOf(v.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v to Value: %w", v.Value, err)
			}
			status.OrderBy = append(status.OrderBy, value)
		default:
			return nil, fmt.Errorf("unknown window function type %d", v.Type)
		}
	}
	return &status, nil
}

type OrderedValue struct {
	OrderBy []Value
	Value   Value
}

type PartitionedValue struct {
	Partition string
	Value     *OrderedValue
}

type WindowFuncAggregatedStatus struct {
	UnitFrame            WindowUnitFrameType
	Start                WindowBoundaryType
	End                  WindowBoundaryType
	RowID                int64
	once                 sync.Once
	PartitionToValuesMap map[string][]*OrderedValue
	PartitionedValues    []*PartitionedValue
	Values               []*OrderedValue
}

func newWindowFuncAggregatedStatus() *WindowFuncAggregatedStatus {
	return &WindowFuncAggregatedStatus{
		PartitionToValuesMap: map[string][]*OrderedValue{},
	}
}

func (s *WindowFuncAggregatedStatus) Step(value Value, status *WindowFuncStatus) error {
	s.once.Do(func() {
		s.UnitFrame = status.UnitFrame
		s.Start = status.Start
		s.End = status.End
		s.RowID = status.RowID
	})
	if s.UnitFrame != status.UnitFrame {
		return fmt.Errorf("mismatch unit frame type %d != %d", s.UnitFrame, status.UnitFrame)
	}
	if s.Start != status.Start {
		return fmt.Errorf("mismatch boundary type %d != %d", s.Start, status.Start)
	}
	if s.End != status.End {
		return fmt.Errorf("mismatch boundary type %d != %d", s.End, status.End)
	}
	if s.RowID != status.RowID {
		return fmt.Errorf("mismatch rowid %d != %d", s.RowID, status.RowID)
	}
	v := &OrderedValue{
		OrderBy: status.OrderBy,
		Value:   value,
	}
	if status.Partition != nil {
		partition, err := status.Partition.ToString()
		if err != nil {
			return fmt.Errorf("failed to convert partition: %w", err)
		}
		s.PartitionToValuesMap[partition] = append(s.PartitionToValuesMap[partition], v)
		s.PartitionedValues = append(s.PartitionedValues, &PartitionedValue{
			Partition: partition,
			Value:     v,
		})
	}
	s.Values = append(s.Values, v)
	return nil
}

func (s *WindowFuncAggregatedStatus) Done(cb func([]Value, int, int) error) error {
	if s.RowID <= 0 {
		return fmt.Errorf("invalid rowid. rowid must be greater than zero")
	}
	values := s.FilteredValues()
	if len(values) != 0 {
		for orderBy := 0; orderBy < len(values[0].OrderBy); orderBy++ {
			sort.Slice(values, func(i, j int) bool {
				cond, _ := values[i].OrderBy[orderBy].LT(values[j].OrderBy[orderBy])
				return cond
			})
		}
	}
	s.Values = values
	start, err := s.StartIdx()
	if err != nil {
		return fmt.Errorf("failed to get start index: %w", err)
	}
	end, err := s.EndIdx()
	if err != nil {
		return fmt.Errorf("failed to get end index: %w", err)
	}
	resultValues := make([]Value, 0, len(values))
	for _, value := range values {
		resultValues = append(resultValues, value.Value)
	}
	return cb(resultValues, start, end)
}

func (s *WindowFuncAggregatedStatus) FilteredValues() []*OrderedValue {
	if len(s.PartitionedValues) != 0 {
		return s.PartitionToValuesMap[s.Partition()]
	}
	return s.Values
}

func (s *WindowFuncAggregatedStatus) Partition() string {
	return s.PartitionedValues[s.RowID-1].Partition
}

func (s *WindowFuncAggregatedStatus) PartitionedRowIndex() (int, error) {
	curRowID := int(s.RowID - 1)
	partitionedValue := s.PartitionedValues[curRowID]
	for idx, value := range s.Values {
		if value == partitionedValue.Value {
			return idx, nil
		}
	}
	return 0, fmt.Errorf("failed to find partitioned row index")
}

func (s *WindowFuncAggregatedStatus) StartIdx() (int, error) {
	switch s.Start {
	case WindowUnboundedPrecedingType:
		return 0, nil
	case WindowCurrentRowType:
		return s.PartitionedRowIndex()
	}
	return 0, fmt.Errorf("unsupported boundary type %d", s.Start)
}

func (s *WindowFuncAggregatedStatus) EndIdx() (int, error) {
	switch s.End {
	case WindowUnboundedFollowingType:
		return len(s.FilteredValues()) - 1, nil
	case WindowCurrentRowType:
		return s.PartitionedRowIndex()
	}
	return 0, fmt.Errorf("unsupported boundary type %d", s.End)
}
