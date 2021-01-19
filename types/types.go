package types

// Options is a set of FDW options provided by user during table creation.
type Options map[string]string

type Relation struct {
	ID        Oid
	IsValid   bool
	Attr      *TupleDesc
	Namespace string
}

type TupleDesc struct {
	TypeID  Oid
	TypeMod int
	//HasOid  bool
	Attrs []Attr // columns
}

type Attr struct {
	Name       string
	Type       Oid
	Dimensions int
	NotNull    bool
	Dropped    bool
}

// Cost is a approximate cost of an operation. See Postgres docs for details.
type Cost float64

// Oid is a Postgres internal object ID.
type Oid uint

type RelSize struct {
	Rows   int
	Width  int
	Tuples int
}

type PathKey struct {
	ColumnNames []string
	Rows        Cost
}
