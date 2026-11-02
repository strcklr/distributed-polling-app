package poll

// NormalizeAddress returns addr as a trimmed, lowercase bare email address, or
// an error if it isn't one. Participants are stored and compared in this form.
//
// TODO(#45): parse with net/mail.ParseAddress and reject anything that isn't
// a bare address, such as "Alice <alice@example.com>": the parsed Address
// must equal the trimmed input. Then use it in New (an invalid participant is
// ErrInvalidPoll, and duplicates are checked after normalizing), in Vote (an
// invalid address is ErrNotParticipant), and in dynamo.Store.CastVote before
// the participant goes into the request.
func NormalizeAddress(addr string) (string, error) {
	return addr, nil
}
