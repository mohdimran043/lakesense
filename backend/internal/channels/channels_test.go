package channels

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/smtp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/rules"
)

type mapResolver map[int64]Channel

func (m mapResolver) Channel(_ context.Context, id int64) (Channel, error) {
	return m[id], nil
}

func note(sev rules.Severity, ch int64) rules.Notification {
	return rules.Notification{
		Incident:  &rules.Incident{ID: 42},
		ChannelID: ch, Severity: sev,
		Title: "sync_failed on public.orders", Body: "connection refused",
	}
}

func TestSlackDeliversBlocks(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := mapResolver{1: {ID: 1, Type: "slack", Config: map[string]any{"webhook_url": srv.URL}}}
	n := New(res, srv.Client(), nil)
	require.NoError(t, n.Send(context.Background(), note(rules.SeverityCritical, 1)))

	blocks, ok := got["blocks"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, blocks, "slack payload carries blocks")
}

func TestWebhookDeliversGenericJSON(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	res := mapResolver{2: {ID: 2, Name: "ops", Type: "webhook", Config: map[string]any{"url": srv.URL}}}
	n := New(res, srv.Client(), nil)
	require.NoError(t, n.Send(context.Background(), note(rules.SeverityWarning, 2)))

	assert.Equal(t, float64(42), got["incident_id"])
	assert.Equal(t, "warning", got["severity"])
	assert.Equal(t, "ops", got["channel"])
}

func TestTelegramHitsBotAPI(t *testing.T) {
	var path string
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := mapResolver{3: {ID: 3, Type: "telegram", Config: map[string]any{
		"bot_token": "TOK", "chat_id": "123", "api_base": srv.URL,
	}}}
	n := New(res, srv.Client(), nil)
	require.NoError(t, n.Send(context.Background(), note(rules.SeverityInfo, 3)))

	assert.Equal(t, "/botTOK/sendMessage", path)
	assert.Equal(t, "123", got["chat_id"])
	// underscores are markdown-escaped, so match the unescaped table name.
	assert.Contains(t, got["text"], "public.orders")
	assert.Equal(t, "Markdown", got["parse_mode"])
}

func TestEmailUsesSMTPSender(t *testing.T) {
	var toGot []string
	var msgGot string
	fake := func(_ string, _ smtp.Auth, _ string, to []string, msg []byte) error {
		toGot = to
		msgGot = string(msg)
		return nil
	}
	res := mapResolver{4: {ID: 4, Type: "email", Config: map[string]any{
		"from": "alerts@lakesense.dev", "to": []any{"oncall@x.com"}, "smtp_host": "localhost",
	}}}
	n := New(res, nil, fake)
	require.NoError(t, n.Send(context.Background(), note(rules.SeverityCritical, 4)))

	assert.Equal(t, []string{"oncall@x.com"}, toGot)
	assert.True(t, strings.Contains(msgGot, "Subject: [LakeSense CRITICAL]"), "subject carries severity")
}

func TestNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	res := mapResolver{5: {ID: 5, Type: "webhook", Config: map[string]any{"url": srv.URL}}}
	n := New(res, srv.Client(), nil)
	assert.Error(t, n.Send(context.Background(), note(rules.SeverityWarning, 5)))
}

func TestUnknownTypeErrors(t *testing.T) {
	res := mapResolver{6: {ID: 6, Type: "carrierpigeon"}}
	n := New(res, nil, nil)
	assert.Error(t, n.Send(context.Background(), note(rules.SeverityInfo, 6)))
}
