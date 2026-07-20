package escalation

import "time"

// Responder is a person/team on call, identified by the channels that reach
// them.
type Responder struct {
	Name       string  `json:"name"`
	ChannelIDs []int64 `json:"channel_ids"`
}

// Override pins a responder for a fixed window, taking precedence over the
// rotation (holiday cover, incident swaps).
type Override struct {
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Responder Responder `json:"responder"`
}

// OnCall is a weekly rotation with optional overrides.
type OnCall struct {
	ID         int64
	Name       string
	Responders []Responder `json:"responders"`
	Overrides  []Override  `json:"overrides"`
}

// Current returns who is on call at now: an active override wins, otherwise the
// rotation advances by ISO week so the assignment is stable within a week and
// rotates predictably across weeks.
func (o OnCall) Current(now time.Time) Responder {
	for _, ov := range o.Overrides {
		if !now.Before(ov.Start) && now.Before(ov.End) {
			return ov.Responder
		}
	}
	if len(o.Responders) == 0 {
		return Responder{}
	}
	_, week := now.ISOWeek()
	return o.Responders[week%len(o.Responders)]
}
