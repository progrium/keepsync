//go:build !unix

package zerofs

import "time"

func (s *Supervisor) stopStale(time.Duration) error { return nil }
