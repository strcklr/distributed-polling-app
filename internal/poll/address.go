package poll

import (
	"fmt"
	"net/mail"
	"strings"
)

// NormalizeAddress returns addr as a trimmed, lowercase bare email address, or
// an error if it isn't one. Participants are stored and compared in this form.
func NormalizeAddress(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	parsed, err := mail.ParseAddress(addr)
	if err != nil {
		return "", fmt.Errorf("%q is not a valid email address: %w", addr, err)
	}
	if parsed.Address != addr {
		return "", fmt.Errorf("%q is not a bare email address", addr)
	}

	return strings.ToLower(parsed.Address), nil
}

func normalizeParticipants(participants []string) ([]string, error) {
	var normalized []string
	for _, p := range participants {
		participant, err := NormalizeAddress(p)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, participant)
	}

	return normalized, nil
}
