package importer

import (
	"errors"
	"regexp"
	"strings"

	"golang.org/x/net/idna"
)

const (
	LabelMinLen = 1
	LabelMaxLen = 63
)

var (
	ErrInvalidLabelLength            = errors.New("label is too short or too long")
	ErrInvalidLabelDash              = errors.New("label starts or ends with a hyphen")
	ErrInvalidLabelDoubleDash        = errors.New("label contains consecutive hyphens (and is not a valid A-label)")
	ErrInvalidLabelIDN               = errors.New("label is an invalid IDN")
	ErrLabelContainsInvalidCharacter = errors.New("label contains invalid characters")
)

// regex for valid label characters (letters, digits, hyphens)
var validLabelChars = regexp.MustCompile(`^[a-z0-9-]+$`)

// ValidateLabel checks if the label is valid according to the defined rules.
// It returns an error if the label is too short or too long, starts or ends with a hyphen,
// contains two consecutive hyphens (unless it is an IDN label), is an invalid IDN label,
// or contains invalid characters.
func ValidateLabel(label string) error {
	// 1. Check length
	if len(label) > LabelMaxLen || len(label) < LabelMinLen {
		return ErrInvalidLabelLength
	}

	// 2. Check characters (must be lowercase alphanumeric + hyphen)
	// We assume label is already lowercased by the caller, but strictly speaking
	// validLabelChars expects lowercase.
	if !validLabelChars.MatchString(label) {
		return ErrLabelContainsInvalidCharacter
	}

	// 3. Start or end with hyphen
	if strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
		return ErrInvalidLabelDash
	}

	// 4. Consecutive hyphens (3rd and 4th position)
	// If it has "xn--" prefix, it's an IDN, check validity using idna package
	// If it has "--" in 3rd/4th position but NOT "xn--", it's invalid.
	// Actually, the rule is: if it has "--" in 3rd and 4th position, it MUST be an IDN (xn--).
	if len(label) >= 4 && label[2] == '-' && label[3] == '-' {
		if !strings.HasPrefix(label, "xn--") {
			return ErrInvalidLabelDoubleDash
		}
	}

	// 5. IDN Validation
	if strings.HasPrefix(label, "xn--") {
		// Use ToUnicode to validate the A-label
		if _, err := idna.Registration.ToUnicode(label); err != nil {
			return ErrInvalidLabelIDN
		}
	}

	return nil
}
