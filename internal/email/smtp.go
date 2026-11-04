// Package email delivers outbox messages over SMTP.
package email

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/smtp"
	"net/textproto"
	"strings"
	"time"

	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

var _ outbox.Sender = SMTPSender{}

// SMTPSender is an outbox.Sender that delivers each message in its own SMTP
// session, such as to Mailpit in local development.
type SMTPSender struct {
	// Addr is the SMTP server's host:port.
	Addr string
	// From is the sender's email address.
	From string
}

func splitEmailAddr(emailAddr string) (string, string, error) {
	user, domain, ok := strings.CutLast(emailAddr, "@")
	if !ok {
		return "", "", fmt.Errorf("invalid address %q", emailAddr)
	}

	return user, domain, nil
}

func (s SMTPSender) formatEmail(m outbox.Message) ([]byte, error) {
	_, domain, err := splitEmailAddr(s.From)
	if err != nil {
		return nil, err
	}

	// Separator here prevents concatenation from creating a duplicate hash subset
	sum := sha256.Sum256([]byte(m.EventID + "\x00" + m.To))
	hash := "<" + hex.EncodeToString(sum[:16]) + "@" + domain + ">"

	headers := [][2]string{
		{"From", s.From},
		{"To", m.To},
		{"Subject", mime.QEncoding.Encode("utf-8", m.Subject)},
		{"Date", time.Now().Format(time.RFC1123Z)},
		{"Message-ID", hash},
		{"MIME-Version", "1.0"},
		{"Content-Type", `text/plain; charset="utf-8"`},
	}

	var message strings.Builder
	for _, h := range headers {
		fmt.Fprintf(&message, "%s: %s\r\n", h[0], h[1])
	}

	// A blank line separates headers from the actual body
	message.WriteString("\r\n")
	message.WriteString(m.Body)

	return []byte(message.String()), nil
}

// Send delivers m in one attempt. Its Message-ID depends only on m.EventID
// and m.To, so a resent message keeps the same ID. An error for a permanent
// rejection (a 5xx reply) wraps outbox.ErrPermanent.
func (s SMTPSender) Send(ctx context.Context, m outbox.Message) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.Addr)
	if err != nil {
		return classify(err)
	}

	defer conn.Close()

	stop := context.AfterFunc(ctx, func() {
		conn.Close()
	})
	defer stop()

	host, _, err := net.SplitHostPort(s.Addr)
	if err != nil {
		return classify(err)
	}

	b, err := s.formatEmail(m)
	if err != nil {
		return classify(err)
	}

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return classify(err)
	}

	err = client.Mail(s.From)
	if err != nil {
		return classify(err)
	}

	err = client.Rcpt(m.To)
	if err != nil {
		return classify(err)
	}

	w, err := client.Data()
	if err != nil {
		return classify(err)
	}

	_, err = w.Write(b)
	if err != nil {
		return classify(err)
	}

	err = w.Close()
	if err != nil {
		return classify(err)
	}

	// Drop the error as the message is already accepted
	_ = client.Quit()

	return nil
}

// classify wraps err with outbox.ErrPermanent if it's a 5xx SMTP reply, and
// returns any other error unchanged.
func classify(err error) error {
	if reply, ok := errors.AsType[*textproto.Error](err); ok && reply.Code >= 500 && reply.Code < 600 {
		return fmt.Errorf("%w: %w", outbox.ErrPermanent, err)
	}
	return err
}
