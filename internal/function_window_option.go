package internal

import (
	"sync"
)

type WindowFuncAggregatedStatus struct {
	once   sync.Once
	Values []Value
	opt    *AggregatorOption
}

func newWindowFuncAggregatedStatus() *WindowFuncAggregatedStatus {
	return &WindowFuncAggregatedStatus{
		opt: &AggregatorOption{
			Distinct:    false,
			IgnoreNulls: false,
		},
	}
}

// RelevantValues retrieves the list of values in the window, respecting both IgnoreNulls and Distinct options
func (s *WindowFuncAggregatedStatus) RelevantValues() ([]Value, error) {
	var filteredValues []Value
	var valueMap = map[string]struct{}{}

	for i := range s.Values {
		value := s.Values[i]
		if s.IgnoreNulls() && value == nil {
			continue
		}
		if s.Distinct() {
			key, err := value.ToString()
			if err != nil {
				return nil, err
			}
			if _, exists := valueMap[key]; exists {
				continue
			}
			valueMap[key] = struct{}{}
		}
		filteredValues = append(filteredValues, value)
	}
	return filteredValues, nil
}

// Step adds a value to the window
func (s *WindowFuncAggregatedStatus) Step(value Value) error {
	s.Values = append(s.Values, value)
	return nil
}

// Inverse removes the oldest entry of a value from the window
func (s *WindowFuncAggregatedStatus) Inverse(value Value) error {
	for i, v := range s.Values {
		if v == value {
			var j int
			if len(s.Values) == i-1 {
				j = i
			} else {
				j = i + 1
			}
			s.Values = append(s.Values[:i], s.Values[j:]...)
			break
		}
	}
	return nil
}

func (s *WindowFuncAggregatedStatus) IgnoreNulls() bool {
	return s.opt.IgnoreNulls
}

func (s *WindowFuncAggregatedStatus) Distinct() bool {
	return s.opt.Distinct
}
