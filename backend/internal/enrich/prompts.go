package enrich

import "fmt"

// systemPrompt constrains the model to strict JSON matching the Enrichment
// shape. Keeping the schema in the prompt (rather than relying on a specific
// model's structured-output feature) keeps enrichment portable across models.
const systemPrompt = `You are a senior data-reliability engineer triaging a data-pipeline failure.
Reply with ONLY a JSON object, no prose and no markdown fences, matching exactly:
{
  "root_cause": "one or two sentences, plain English",
  "affected_tables": ["schema.table", "..."],
  "suggested_fix": "one concrete, actionable step",
  "severity": "info | warning | critical"
}
Base every field strictly on the failure details provided. Do not invent table
names that are not implied by the failure. If unsure of severity, choose the
higher one.`

func userPrompt(f Failure) string {
	return fmt.Sprintf(`Pipeline: %s
Source connector: %s
Stream: %s
Error code: %s
Retryable: %t
Error message: %s`,
		orNone(f.Pipeline), orNone(f.Connector), orNone(f.Stream),
		orNone(f.ErrorCode), f.Retryable, orNone(f.ErrorMessage))
}

const postmortemSystem = `You are a data-reliability engineer writing a brief, factual incident
postmortem. Use plain English and Markdown. Keep it under 150 words with three
short sections: What happened, Impact, Follow-up. Base it only on the details
provided.`

func postmortemPrompt(f Failure, resolution string) string {
	return fmt.Sprintf(`%s

Resolution: %s`, userPrompt(f), orNone(resolution))
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
