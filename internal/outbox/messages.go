package outbox

import (
	"fmt"
	"slices"
	"strings"
	"text/template"
)

const (
	createdSubject = "Vote: {{.Question}}"
	closedSubject  = "Results: {{.Question}}"

	createdBody = `{{.Question}}
{{range $i, $o := .Options}}	{{$i}}: {{$o}}
{{end}}
Vote with: POST /polls/{{.PollID}}/votes`
	closedBody = `{{.Question}}
{{range $i, $o := .Options}}	{{$o}}: {{index $.Counts $i}}
{{end}}
{{winner .}}`
)

func winner(e Event) string {
	top := slices.Max(e.Counts)

	options := []string{}
	for i, c := range e.Counts {
		if c == top {
			options = append(options, e.Options[i])
		}
	}

	status := "Winner"
	if len(options) > 1 {
		status = "Tie"
	}

	return fmt.Sprintf("%s: %s", status, strings.Join(options, ", "))
}

var (
	createdSubjectTemplate = createTemplate("createdSubject", createdSubject)
	createdBodyTemplate    = createTemplate("createdBody", createdBody)
	closedSubjectTemplate  = createTemplate("closedSubject", closedSubject)
	closedBodyTemplate     = createTemplate("closedBody", closedBody)
)

func createTemplate(name string, text string) *template.Template {
	return template.Must(template.New(name).Funcs(template.FuncMap{"winner": winner}).Parse(text))
}

func (e Event) checkCounts() error {
	if len(e.Counts) != len(e.Options) {
		return fmt.Errorf("poll %s has %d counts for %d options", e.PollID, len(e.Counts), len(e.Options))
	}

	return nil
}

// Messages returns one message per recipient of e, in the order of
// e.Recipients: an invitation to vote for EventPollCreated, and the results
// for EventPollClosed.
//
// An invitation's subject is "Vote: <question>". Its body has the question,
// each option with the index to vote with, and how to vote on poll e.PollID.
//
// A results subject is "Results: <question>". Its body has a
// "<option>: <count>" line for each option, then "Winner: <option>", or
// "Tie: <option>, <option>" when options share the highest count.
//
// It returns an error for an unknown event type, or for a closed event whose
// counts don't match its options.
func Messages(e Event) ([]Message, error) {
	var sTemplate, bTemplate *template.Template
	switch e.Type {
	case EventPollCreated:
		sTemplate = createdSubjectTemplate
		bTemplate = createdBodyTemplate
	case EventPollClosed:
		if err := e.checkCounts(); err != nil {
			return nil, err
		}
		sTemplate = closedSubjectTemplate
		bTemplate = closedBodyTemplate
	default:
		return nil, fmt.Errorf("unknown event type %q", e.Type)
	}

	var subject, body string
	var sb strings.Builder
	if err := sTemplate.Execute(&sb, e); err != nil {
		return nil, err
	}
	subject = sb.String()

	sb.Reset()
	if err := bTemplate.Execute(&sb, e); err != nil {
		return nil, err
	}
	body = sb.String()

	var messages []Message
	for _, r := range e.Recipients {
		messages = append(messages, Message{
			EventID: e.ID,
			To:      r,
			Subject: subject,
			Body:    body,
		})
	}

	return messages, nil
}
