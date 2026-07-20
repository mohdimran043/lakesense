// Package enrich turns a raw pipeline failure into a plain-English incident:
// root cause, affected tables, a suggested fix, and a severity recommendation.
// It calls the Anthropic Messages API over plain net/http (no SDK), and — per
// the standing rule that every LLM feature degrades gracefully — always returns
// a usable Enrichment even when no API key is configured or the API errors. The
// non-LLM fallback is deterministic and derived from the failure's own fields.
package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultEndpoint = "https://api.anthropic.com/v1/messages"
	apiVersion      = "2023-06-01"
	maxTokens       = 1024
	maxAttempts     = 3
)

// Failure is the raw context handed to enrichment.
type Failure struct {
	Pipeline     string
	Stream       string
	Connector    string
	ErrorCode    string
	ErrorMessage string
	Retryable    bool
}

// Enrichment is the structured result. Source is "llm" or "fallback" so the UI
// can label AI-generated text honestly.
type Enrichment struct {
	RootCause      string   `json:"root_cause"`
	AffectedTables []string `json:"affected_tables"`
	SuggestedFix   string   `json:"suggested_fix"`
	Severity       string   `json:"severity"` // info | warning | critical
	Source         string   `json:"source"`   // llm | fallback
}

// Client calls the Anthropic API. A zero API key disables the LLM path (every
// call returns the deterministic fallback).
type Client struct {
	httpClient *http.Client
	apiKey     string
	model      string
	endpoint   string
}

// New builds a Client. endpoint defaults to the public API when empty
// (overridden in tests). A nil httpClient gets a 30s-timeout default.
func New(apiKey, model, endpoint string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	return &Client{httpClient: hc, apiKey: apiKey, model: model, endpoint: endpoint}
}

// Enabled reports whether the LLM path is configured.
func (c *Client) Enabled() bool { return strings.TrimSpace(c.apiKey) != "" }

// Enrich returns a structured enrichment for a failure. It never fails from the
// caller's perspective: the returned Enrichment is always usable. A non-nil
// error means the LLM path was attempted and fell back — surface it in logs,
// but still use the Enrichment (its Source will be "fallback").
func (c *Client) Enrich(ctx context.Context, f Failure) (Enrichment, error) {
	if !c.Enabled() {
		return fallback(f), nil
	}
	e, err := c.callAPI(ctx, f)
	if err != nil {
		return fallback(f), err
	}
	e.Source = "llm"
	if e.Severity == "" {
		e.Severity = fallbackSeverity(f)
	}
	if len(e.AffectedTables) == 0 && f.Stream != "" {
		e.AffectedTables = []string{f.Stream}
	}
	return e, nil
}

// callAPI performs the Anthropic request with retry/backoff and strict JSON
// parsing of the model's reply.
func (c *Client) callAPI(ctx context.Context, f Failure) (Enrichment, error) {
	reqBody, err := json.Marshal(anthropicRequest{
		Model:     c.model,
		MaxTokens: maxTokens,
		System:    systemPrompt,
		Messages:  []message{{Role: "user", Content: userPrompt(f)}},
	})
	if err != nil {
		return Enrichment{}, fmt.Errorf("marshal request: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		e, retryable, err := c.attempt(ctx, reqBody)
		if err == nil {
			return e, nil
		}
		lastErr = err
		if !retryable {
			break
		}
		select {
		case <-ctx.Done():
			return Enrichment{}, ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
	return Enrichment{}, lastErr
}

func (c *Client) attempt(ctx context.Context, body []byte) (Enrichment, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return Enrichment{}, false, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", apiVersion)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Enrichment{}, true, fmt.Errorf("call anthropic: %w", err) // network → retryable
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return Enrichment{}, true, fmt.Errorf("anthropic status %d: %s", resp.StatusCode, raw)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Enrichment{}, false, fmt.Errorf("anthropic status %d: %s", resp.StatusCode, raw)
	}

	var ar anthropicResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return Enrichment{}, false, fmt.Errorf("decode response envelope: %w", err)
	}
	text := ar.firstText()
	if text == "" {
		return Enrichment{}, false, fmt.Errorf("empty model response")
	}
	var e Enrichment
	if err := json.Unmarshal([]byte(stripFences(text)), &e); err != nil {
		return Enrichment{}, false, fmt.Errorf("parse model JSON: %w", err)
	}
	return e, false, nil
}

// DraftPostmortem asks the model for a short postmortem when an incident
// resolves. Falls back to a templated summary when the LLM is unavailable.
func (c *Client) DraftPostmortem(ctx context.Context, f Failure, resolution string) (string, error) {
	if !c.Enabled() {
		return fallbackPostmortem(f, resolution), nil
	}
	reqBody, err := json.Marshal(anthropicRequest{
		Model:     c.model,
		MaxTokens: maxTokens,
		System:    postmortemSystem,
		Messages:  []message{{Role: "user", Content: postmortemPrompt(f, resolution)}},
	})
	if err != nil {
		return fallbackPostmortem(f, resolution), err
	}
	text, err := c.rawText(ctx, reqBody)
	if err != nil {
		return fallbackPostmortem(f, resolution), err
	}
	return text, nil
}

// rawText performs one request and returns the model's plain text reply.
func (c *Client) rawText(ctx context.Context, body []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("anthropic-version", apiVersion)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("anthropic status %d", resp.StatusCode)
	}
	var ar anthropicResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return "", err
	}
	if t := ar.firstText(); t != "" {
		return t, nil
	}
	return "", fmt.Errorf("empty response")
}

func backoff(attempt int) time.Duration {
	return time.Duration(attempt*attempt) * 200 * time.Millisecond
}

// stripFences removes a leading/trailing ```json fence if the model wrapped its
// JSON, so parsing stays strict on the payload itself.
func stripFences(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}

// --- Anthropic wire types (minimal subset) ---

type anthropicRequest struct {
	Model     string    `json:"model"`
	MaxTokens int       `json:"max_tokens"`
	System    string    `json:"system,omitempty"`
	Messages  []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

func (r anthropicResponse) firstText() string {
	for _, b := range r.Content {
		if b.Type == "text" && b.Text != "" {
			return b.Text
		}
	}
	return ""
}
