package interfaces

type Mapper interface {
	Map(input MapInput, emit func(string, string))
}

type Reducer interface {
	Reduce(input ReducerInput, emit func(string))
}

type MapInput interface {
	Value() string
}

type ReducerInput interface {
	Key() string
	Value() string
	NextValue()
	Done() bool
}
