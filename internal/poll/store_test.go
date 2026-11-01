package poll_test

import (
	"testing"

	"github.com/strcklr/distributed-polling-app/internal/poll"
	"github.com/strcklr/distributed-polling-app/internal/poll/storetest"
)

func newMemoryStore(t *testing.T) poll.Store {
	return poll.NewMemoryStore()
}

func TestMemoryStoreCreateGet(t *testing.T) {
	storetest.RunCreateGet(t, newMemoryStore)
}

func TestMemoryStoreCastVote(t *testing.T) {
	storetest.RunCastVote(t, newMemoryStore)
}
