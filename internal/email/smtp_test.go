package email

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/strcklr/distributed-polling-app/internal/outbox"
)

const from = "polls@example.test"

func TestSMTPSenderDelivers(t *testing.T) {
	s, api := newMailpit(t)
	m := testMessage(t)

	if err := s.Send(t.Context(), m); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	got := api.only(t, m.To)
	if got.From.Address != from {
		t.Errorf("From = %q, want %q", got.From.Address, from)
	}
	if got.Subject != m.Subject {
		t.Errorf("Subject = %q, want %q", got.Subject, m.Subject)
	}
	if body := strings.TrimSpace(api.text(t, got.ID)); body != m.Body {
		t.Errorf("body = %q, want %q", body, m.Body)
	}
}

func TestSMTPSenderEncodesSubject(t *testing.T) {
	s, api := newMailpit(t)
	m := testMessage(t)
	m.Subject = "Où déjeuner ? 🍜"

	if err := s.Send(t.Context(), m); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if got := api.only(t, m.To).Subject; got != m.Subject {
		t.Errorf("Subject = %q, want %q", got, m.Subject)
	}
}

func TestSMTPSenderSubjectCantAddHeaders(t *testing.T) {
	s, api := newMailpit(t)
	m := testMessage(t)
	m.Subject = "Lunch?\r\nBcc: " + m.To + ".evil"

	if err := s.Send(t.Context(), m); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	got := api.only(t, m.To)
	if len(got.Bcc) > 0 {
		t.Errorf("Bcc = %v, want none: the subject added a header", got.Bcc)
	}
	if !strings.HasPrefix(got.Subject, "Lunch?") {
		t.Errorf("Subject = %q, want it to start with %q", got.Subject, "Lunch?")
	}
}

func TestSMTPSenderMessageIDIsStable(t *testing.T) {
	s, api := newMailpit(t)
	m := testMessage(t)
	other := m
	other.To = "other-" + m.To

	for _, msg := range []outbox.Message{m, m, other} {
		if err := s.Send(t.Context(), msg); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
	}

	sent := api.search(t, m.To)
	if len(sent) != 2 {
		t.Fatalf("got %d messages to %s, want 2", len(sent), m.To)
	}
	if sent[0].MessageID != sent[1].MessageID {
		t.Errorf("resend has Message-ID %q, want %q", sent[1].MessageID, sent[0].MessageID)
	}
	if !strings.HasSuffix(sent[0].MessageID, "@example.test") {
		t.Errorf("Message-ID = %q, want it at From's domain", sent[0].MessageID)
	}
	if id := api.only(t, other.To).MessageID; id == sent[0].MessageID {
		t.Errorf("another recipient got the same Message-ID %q", id)
	}
}

func TestSMTPSenderStopsWhenServerHangs(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		ln.Close()
	})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		<-done
		conn.Close()
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = SMTPSender{Addr: ln.Addr().String(), From: from}.Send(ctx, testMessage(t))
	if err == nil {
		t.Fatal("Send() to a server that never greets succeeded, want an error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Send returned after %v, want it to stop soon after ctx is done", elapsed)
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		permanent bool
	}{
		{"mailbox unavailable", &textproto.Error{Code: 550, Msg: "no such user"}, true},
		{"wrapped rejection", fmt.Errorf("rcpt: %w", &textproto.Error{Code: 553, Msg: "bad address"}), true},
		{"service unavailable", &textproto.Error{Code: 421, Msg: "try again later"}, false},
		{"mailbox busy", &textproto.Error{Code: 450, Msg: "busy"}, false},
		{"connection dropped", io.EOF, false},
		{"canceled", context.Canceled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.err)
			if !errors.Is(got, tt.err) {
				t.Errorf("classify(%v) = %v, want it to wrap the original", tt.err, got)
			}
			if p := errors.Is(got, outbox.ErrPermanent); p != tt.permanent {
				t.Errorf("classify(%v) permanent = %v, want %v", tt.err, p, tt.permanent)
			}
		})
	}
}

func TestClassifyNil(t *testing.T) {
	if err := classify(nil); err != nil {
		t.Errorf("classify(nil) = %v, want nil", err)
	}
}

// testMessage returns a message to a recipient no other test uses, so tests
// can share one Mailpit.
func testMessage(t *testing.T) outbox.Message {
	t.Helper()
	return outbox.Message{
		EventID: "p1#created",
		To:      strings.ToLower(rand.Text()) + "@example.test",
		Subject: "Vote: Lunch?",
		Body:    "Where should we eat?\n\n- Tacos\n- Ramen",
	}
}

// newMailpit returns a sender to Mailpit's SMTP server at $SMTP_ADDR and a
// client for its API at $MAILPIT_URL, skipping the test when either isn't set.
func newMailpit(t *testing.T) (SMTPSender, mailpit) {
	t.Helper()
	addr, base := os.Getenv("SMTP_ADDR"), os.Getenv("MAILPIT_URL")
	if addr == "" || base == "" {
		t.Skip("SMTP_ADDR or MAILPIT_URL not set; run `docker compose up -d` and set them to localhost:1025 and http://localhost:8025")
	}
	return SMTPSender{Addr: addr, From: from}, mailpit{base: base}
}

type mailpit struct {
	base string
}

type mailpitMessage struct {
	ID        string
	MessageID string
	From      mailpitAddress
	To        []mailpitAddress
	Bcc       []mailpitAddress
	Subject   string
}

type mailpitAddress struct {
	Address string
}

func (m mailpit) search(t *testing.T, to string) []mailpitMessage {
	t.Helper()
	var res struct{ Messages []mailpitMessage }
	m.get(t, "/api/v1/search?query="+url.QueryEscape(fmt.Sprintf("to:%q", to)), &res)
	return res.Messages
}

func (m mailpit) only(t *testing.T, to string) mailpitMessage {
	t.Helper()
	msgs := m.search(t, to)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages to %s, want 1", len(msgs), to)
	}
	return msgs[0]
}

func (m mailpit) text(t *testing.T, id string) string {
	t.Helper()
	var res struct{ Text string }
	m.get(t, "/api/v1/message/"+id, &res)
	return res.Text
}

func (m mailpit) get(t *testing.T, path string, v any) {
	t.Helper()
	resp, err := http.Get(m.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: %s", path, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}
