package termotion

import (
	"fmt"
	"io"
	"time"
)

type textRendererState struct {
	tasks map[int64]time.Time
	w     io.Writer
}

func (s *textRendererState) handleEvent(e any) {
	switch e := e.(type) {
	case TitleEvent:
		fmt.Fprintf(s.w, "Title: %s\n", e.Title)
	case TaskStartEvent:
		if e.ID == 0 {
			s.tasks[0] = e.StartedAt
			fmt.Fprintf(s.w, "Session started at %s\n", e.StartedAt.Format(time.RFC3339))
			return
		}
		if _, exists := s.tasks[e.ID]; exists {
			fmt.Fprintf(s.w, "Task %d started but already exists\n", e.ID)
			return
		}
		fmt.Fprintf(s.w, "Task %d started (parent: %d) at %s\n", e.ID, e.Parent, e.StartedAt.Format(time.RFC3339))
		s.tasks[e.ID] = e.StartedAt
	case TaskStopEvent:
		startedAt, exists := s.tasks[e.ID]
		if e.ID == 0 {
			if !exists {
				fmt.Fprintf(s.w, "Session stopped but never started\n")
				return
			}
			fmt.Fprintf(s.w, "Session stopped (%s)\n", e.StoppedAt.Sub(startedAt).Truncate(time.Millisecond))
			return
		}
		if !exists {
			fmt.Fprintf(s.w, "Task %d stopped but does not exist\n", e.ID)
			return
		}
		dur := e.StoppedAt.Sub(startedAt).Truncate(time.Millisecond)
		if e.Err != nil {
			fmt.Fprintf(s.w, "Task %d stopped with error: %v (%s)\n", e.ID, e.Err, dur)
		} else {
			fmt.Fprintf(s.w, "Task %d stopped successfully (%s)\n", e.ID, dur)
		}
	case TaskProgressEvent:
		if _, exists := s.tasks[e.ID]; !exists {
			fmt.Fprintf(s.w, "Task %d progress update but does not exist\n", e.ID)
			return
		}
		fmt.Fprintf(s.w, "Task %d progress: %.2f%%\n", e.ID, e.Percent*100)
	case TaskETAEvent:
		if _, exists := s.tasks[e.ID]; !exists {
			fmt.Fprintf(s.w, "Task %d ETA update but does not exist\n", e.ID)
			return
		}
		if e.FinishAt.IsZero() {
			fmt.Fprintf(s.w, "Task %d ETA cleared\n", e.ID)
		} else {
			fmt.Fprintf(s.w, "Task %d ETA %s\n", e.ID, e.FinishAt.Format(time.RFC3339))
		}
	case TaskRateEvent:
		if _, exists := s.tasks[e.ID]; !exists {
			fmt.Fprintf(s.w, "Task %d rate update but does not exist\n", e.ID)
			return
		}
		if e.Unit == "" {
			fmt.Fprintf(s.w, "Task %d rate cleared\n", e.ID)
		} else {
			fmt.Fprintf(s.w, "Task %d rate %s\n", e.ID, formatRate(e.Rate, e.Unit))
		}
	case TaskDisplayEvent:
		if _, exists := s.tasks[e.ID]; !exists {
			fmt.Fprintf(s.w, "Task %d display update but does not exist\n", e.ID)
			return
		}
		if e.Title != nil {
			fmt.Fprintf(s.w, "Task %d title updated: %s\n", e.ID, *e.Title)
		}
		if e.DisplayProgress != nil {
			fmt.Fprintf(s.w, "Task %d display progress: %t\n", e.ID, *e.DisplayProgress)
		}
		if e.DisplaySpinner != nil {
			fmt.Fprintf(s.w, "Task %d display spinner: %t\n", e.ID, *e.DisplaySpinner)
		}
		if e.DisplayElapsed != nil {
			fmt.Fprintf(s.w, "Task %d display elapsed: %t\n", e.ID, *e.DisplayElapsed)
		}
		if e.HideWhenDone != nil {
			fmt.Fprintf(s.w, "Task %d hide when done: %t\n", e.ID, *e.HideWhenDone)
		}
	case TaskMessageEvent:
		if _, exists := s.tasks[e.ID]; !exists {
			fmt.Fprintf(s.w, "Task %d message but does not exist\n", e.ID)
			return
		}
		fmt.Fprintf(s.w, "Task %d message: %s\n", e.ID, string(e.Msg))
	default:
		fmt.Fprintf(s.w, "Unknown event type: %T\n", e)
	}
}

// TextRenderer returns a Renderer that writes human-readable text descriptions
// of events to the provided io.Writer.
func TextRenderer(w io.Writer) Renderer {
	return func(events <-chan any) {
		state := textRendererState{w: w, tasks: make(map[int64]time.Time)}
		for e := range events {
			state.handleEvent(e)
		}
	}
}
