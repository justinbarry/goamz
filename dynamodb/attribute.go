package dynamodb

const (
	TYPE_STRING = "S"
	TYPE_NUMBER = "N"
	TYPE_BIN    = "B"
)

type PrimaryKey struct {
	KeyAttribute   *Attribute
	RangeAttribute *Attribute
}

type Attribute struct {
	Type  string
	Name  string
	Value string
}

func NewStringAttribute(name string, value string) *Attribute {
	return &Attribute{TYPE_STRING,
		name,
		value,
	}
}

func NewNumericAttribute(name string, value string) *Attribute {
	return &Attribute{TYPE_NUMBER,
		name,
		value,
	}
}

func NewBinaryAttribute(name string, value string) *Attribute {
	return &Attribute{TYPE_BIN,
		name,
		value,
	}
}
