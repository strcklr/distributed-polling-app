package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	NewHandler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want = %d", recorder.Code, http.StatusOK)
	}
	var got map[string]string
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	if status := got["status"]; status != "ok" {
		t.Errorf(`status = %q, want "ok"`, status)
	}
}
