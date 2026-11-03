// Package email delivers outbox messages over SMTP.
package email

import (
	"context"
	"errors"

	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

var errNotImplemented = errors.New("not implemented")

var _ outbox.Sender = SMTPSender{}

// SMTPSender is an outbox.Sender that delivers each message in its own SMTP
// session, such as to Mailpit in local development.
type SMTPSender struct {
	// Addr is the SMTP server's host:port.
	Addr string
	// From is the sender's email address.
	From string
}

// Send delivers m in one attempt. Its Message-ID depends only on m.EventID
// and m.To, so a resent message keeps the same ID. An error for a permanent
// rejection (a 5xx reply) wraps outbox.ErrPermanent.
func (s SMTPSender) Send(ctx context.Context, m outbox.Message) error {
	// TODO(#15):
	//   - Dial s.Addr with a net.Dialer's DialContext. Use context.AfterFunc
	//     to set a past deadline on the conn when ctx is done, so a hung
	//     server can't hold the send past cancellation.
	//   - smtp.NewClient, then Mail(s.From), Rcpt(m.To), Data, write the
	//     message, close the writer, and Quit.
	//   - Headers: From, To, Subject, Date, Message-ID, MIME-Version and
	//     Content-Type "text/plain; charset=utf-8". Encode Subject with
	//     mime.QEncoding, so a newline in a poll question can't add headers.
	//   - Message-ID: hash EventID and To (crypto/sha256), at From's domain.
	//   - Pass errors through classify.
	return errNotImplemented
}

// classify wraps err with outbox.ErrPermanent if it's a 5xx SMTP reply, and
// returns any other error unchanged.
func classify(err error) error {
	// TODO(#15): net/smtp returns replies as *textproto.Error, with the
	// status in Code.
	return err
}
