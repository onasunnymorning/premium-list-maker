package tagger

import "fmt"

// GenerateLengthTag generates a length-based tag for a label
// Returns a tag in the format "len:N" where N is the label length
func GenerateLengthTag(length int) string {
	return fmt.Sprintf("len:%d", length)
}

