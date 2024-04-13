package interfaces

type Mapper interface {
	Map(input MapInput, emit func(string, string))
}

type Reducer interface {
	Reduce(input ReduceInput, emit func(int)) []KeyValue
}

type MapInput interface {
	Value() string
}

type ReduceInput interface {
	Key() string
	NextValue() string
	Done() bool
}

type KeyValue struct {
	Key   string
	Value string
}
