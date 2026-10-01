package termotion

import (
	"fmt"
	"time"
)

// Canonical Duration Display: mirrored by fmtDuration in web/termotion.js — keep the two in step.
func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Truncate(100 * time.Millisecond)

	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}

	d = d.Truncate(time.Second)
	h := int64(d / time.Hour)
	m := int64(d % time.Hour / time.Minute)
	s := int64(d % time.Minute / time.Second)

	if h == 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
}
