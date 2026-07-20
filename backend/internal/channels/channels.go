// Package channels implements delivery adapters behind the rules.Notifier
// interface: Slack, Telegram, SMTP email, and a generic webhook. The rule
// engine hands it a Notification with a channel id; the multiplexer resolves
// the channel, formats per its type, and delivers. Network and SMTP are behind
// injectable clients so delivery is unit-testable without external services.
package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	"github.com/lakesense/lakesense/backend/internal/rules"
)

// Channel is a resolved delivery target.
type Channel struct {
	ID     int64
	Name   string
	Type   string // slack | telegram | email | webhook
	Config map[string]any
}

// Resolver loads a channel by id (from the DB in production, a map in tests).
type Resolver interface {
	Channel(ctx context.Context, id int64) (Channel, error)
}

// SMTPSender abstracts smtp.SendMail for testing.
type SMTPSender func(addr string, a smtp.Auth, from string, to []string, msg []byte) error

// Notifier is the multiplexing rules.Notifier implementation.
type Notifier struct {
	resolver Resolver
	http     *http.Client
	sendMail SMTPSender
}

// New builds a Notifier. A nil http client defaults to a 10s-timeout client;
// a nil sender defaults to net/smtp.SendMail.
func New(resolver Resolver, httpClient *http.Client, sender SMTPSender) *Notifier {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	if sender == nil {
		sender = smtp.SendMail
	}
	return &Notifier{resolver: resolver, http: httpClient, sendMail: sender}
}

// Send implements rules.Notifier.
func (n *Notifier) Send(ctx context.Context, note rules.Notification) error {
	ch, err := n.resolver.Channel(ctx, note.ChannelID)
	if err != nil {
		return fmt.Errorf("resolve channel %d: %w", note.ChannelID, err)
	}
	switch ch.Type {
	case "slack":
		return n.sendSlack(ctx, ch, note)
	case "telegram":
		return n.sendTelegram(ctx, ch, note)
	case "webhook":
		return n.sendWebhook(ctx, ch, note)
	case "email":
		return n.sendEmail(ch, note)
	default:
		return fmt.Errorf("unknown channel type %q", ch.Type)
	}
}

// --- Slack: rich blocks to an incoming webhook ---

func (n *Notifier) sendSlack(ctx context.Context, ch Channel, note rules.Notification) error {
	url := cfgString(ch, "webhook_url")
	if url == "" {
		return fmt.Errorf("slack channel %d missing webhook_url", ch.ID)
	}
	body := map[string]any{
		"blocks": []map[string]any{
			{"type": "header", "text": map[string]any{"type": "plain_text", "text": fmt.Sprintf("%s %s", severityEmoji(note.Severity), note.Title)}},
			{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": note.Body}},
			{"type": "context", "elements": []map[string]any{
				{"type": "mrkdwn", "text": fmt.Sprintf("*Severity:* %s  •  *Incident:* #%d", note.Severity, incidentID(note))},
			}},
		},
	}
	return n.postJSON(ctx, url, body)
}

// --- Telegram: bot sendMessage ---

func (n *Notifier) sendTelegram(ctx context.Context, ch Channel, note rules.Notification) error {
	token := cfgString(ch, "bot_token")
	chatID := cfgString(ch, "chat_id")
	if token == "" || chatID == "" {
		return fmt.Errorf("telegram channel %d missing bot_token or chat_id", ch.ID)
	}
	text := fmt.Sprintf("%s *%s*\n%s\n_severity: %s • incident #%d_",
		severityEmoji(note.Severity), escapeMarkdown(note.Title), escapeMarkdown(note.Body), note.Severity, incidentID(note))
	base := cfgString(ch, "api_base")
	if base == "" {
		base = "https://api.telegram.org"
	}
	url := fmt.Sprintf("%s/bot%s/sendMessage", base, token)
	return n.postJSON(ctx, url, map[string]any{"chat_id": chatID, "text": text, "parse_mode": "Markdown"})
}

// --- Generic webhook ---

func (n *Notifier) sendWebhook(ctx context.Context, ch Channel, note rules.Notification) error {
	url := cfgString(ch, "url")
	if url == "" {
		return fmt.Errorf("webhook channel %d missing url", ch.ID)
	}
	return n.postJSON(ctx, url, map[string]any{
		"incident_id": incidentID(note),
		"severity":    note.Severity,
		"title":       note.Title,
		"body":        note.Body,
		"channel":     ch.Name,
	})
}

// --- Email via SMTP ---

func (n *Notifier) sendEmail(ch Channel, note rules.Notification) error {
	from := cfgString(ch, "from")
	to := cfgStringSlice(ch, "to")
	host := cfgString(ch, "smtp_host")
	port := cfgString(ch, "smtp_port")
	if from == "" || len(to) == 0 || host == "" {
		return fmt.Errorf("email channel %d missing from/to/smtp_host", ch.ID)
	}
	if port == "" {
		port = "587"
	}
	var auth smtp.Auth
	if u := cfgString(ch, "username"); u != "" {
		auth = smtp.PlainAuth("", u, cfgString(ch, "password"), host)
	}
	subject := fmt.Sprintf("[LakeSense %s] %s", strings.ToUpper(string(note.Severity)), note.Title)
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n\r\nSeverity: %s\r\nIncident: #%d\r\n",
		from, strings.Join(to, ", "), subject, note.Body, note.Severity, incidentID(note))
	return n.sendMail(host+":"+port, auth, from, to, []byte(msg))
}

// postJSON POSTs a JSON body and treats any non-2xx as an error.
func (n *Notifier) postJSON(ctx context.Context, url string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.http.Do(req)
	if err != nil {
		return fmt.Errorf("deliver: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("delivery returned status %d", resp.StatusCode)
	}
	return nil
}

// --- helpers ---

func severityEmoji(s rules.Severity) string {
	switch s {
	case rules.SeverityCritical:
		return "🔴"
	case rules.SeverityWarning:
		return "🟡"
	default:
		return "🔵"
	}
}

func incidentID(note rules.Notification) int64 {
	if note.Incident == nil {
		return 0
	}
	return note.Incident.ID
}

func escapeMarkdown(s string) string {
	r := strings.NewReplacer("_", "\\_", "*", "\\*", "`", "\\`", "[", "\\[")
	return r.Replace(s)
}

func cfgString(ch Channel, key string) string {
	if v, ok := ch.Config[key].(string); ok {
		return v
	}
	return ""
}

func cfgStringSlice(ch Channel, key string) []string {
	switch v := ch.Config[key].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		return []string{v}
	default:
		return nil
	}
}
