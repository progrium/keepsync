package keeper

import (
	"errors"
	"fmt"
	"time"

	"github.com/progrium/keepsync/internal/api"
)

// Policy defaults from docs/design.md. Expire defaults to 0 (never expire).
const (
	DefaultGrace    = 48 * time.Hour
	DefaultMaxDefer = 7 * 24 * time.Hour
)

// ValidatePolicy checks Grace < MaxDefer < Expire, with Expire == 0 meaning
// nothing expires. It also requires a permanode.
func ValidatePolicy(p api.KeepPolicy) error {
	if p.Permanode == "" {
		return errors.New("keeper: policy has no permanode")
	}
	if p.Grace < 0 || p.MaxDefer < 0 || p.Expire < 0 {
		return errors.New("keeper: negative duration in policy")
	}
	if !(p.Grace < p.MaxDefer) {
		return fmt.Errorf("keeper: grace (%s) must be less than max-defer (%s)", p.Grace, p.MaxDefer)
	}
	if p.Expire != 0 && !(p.MaxDefer < p.Expire) {
		return fmt.Errorf("keeper: max-defer (%s) must be less than expire (%s)", p.MaxDefer, p.Expire)
	}
	return nil
}

// WithDefaults fills a zero Grace and MaxDefer with the defaults. Expire is
// left alone, since 0 already means never expire.
func WithDefaults(p api.KeepPolicy) api.KeepPolicy {
	if p.Grace == 0 {
		p.Grace = DefaultGrace
	}
	if p.MaxDefer == 0 {
		p.MaxDefer = DefaultMaxDefer
	}
	return p
}
