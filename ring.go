package termotion

import (
	"iter"
	"strings"
)

type ringBuffer struct {
	buf     []string
	pos     int
	midLine bool
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		buf: make([]string, 0, size),
	}
}

func (r *ringBuffer) addLine(s string) {
	if len(r.buf) < cap(r.buf) {
		r.buf = append(r.buf, s)
	} else {
		r.buf[r.pos] = s
		r.pos = (r.pos + 1) % cap(r.buf)
	}
}

func (r *ringBuffer) add(s string) {
	parts := strings.Split(s, "\n")

	for i, part := range parts {
		if i == len(parts)-1 && part == "" {
			r.midLine = false
			return
		}

		if i == 0 && r.midLine && len(r.buf) > 0 {
			lastIdx := (r.pos - 1 + len(r.buf)) % len(r.buf)
			r.buf[lastIdx] += part
		} else {
			r.addLine(part)
		}
	}

	r.midLine = true
}

func (r *ringBuffer) lines() iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := 0; i < len(r.buf); i++ {
			idx := (r.pos + i) % len(r.buf)
			if !yield(r.buf[idx]) {
				return
			}
		}
	}
}
