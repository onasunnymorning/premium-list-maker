package models

// Label represents a domain label in the database
type Label struct {
	ID     int64
	Label  string
	Length int
	Tags   []string
}

