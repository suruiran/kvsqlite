package kvsqlite

//go:generate go tool stringer -type KeyKind -trimprefix "KeyKind"
type KeyKind int

const (
	KeyKindUndefined KeyKind = iota
	KeyKindString
	KeyKindHash
)
