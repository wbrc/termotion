package termotion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TitleEvent represents an update to the display title of the task list.
type TitleEvent struct {
	Title string `json:"title"`
}

// TaskStartEvent indicates that a new task has started, with an optional parent
// task ID. ID 0 denotes the session (the root run) rather than a task node.
// StartedAt is captured by the producer at the moment the event is emitted.
type TaskStartEvent struct {
	ID        int64     `json:"id"`
	Parent    int64     `json:"parent,omitempty"`
	StartedAt time.Time `json:"startedAt"`
}

// TaskStopEvent indicates that a task has completed, along with any error that
// occurred. ID 0 denotes the session. StoppedAt is captured by the producer at
// the moment the event is emitted.
type TaskStopEvent struct {
	ID        int64     `json:"id"`
	Err       error     `json:"-"`
	StoppedAt time.Time `json:"stoppedAt"`
}

func (e TaskStopEvent) MarshalJSON() ([]byte, error) {
	type alias struct {
		ID        int64     `json:"id"`
		Canceled  bool      `json:"canceled,omitempty"`
		Err       string    `json:"err,omitempty"`
		StoppedAt time.Time `json:"stoppedAt"`
	}

	var a alias
	a.ID = e.ID
	a.StoppedAt = e.StoppedAt
	if e.Err != nil {
		a.Err = e.Err.Error()
		if errors.Is(e.Err, context.Canceled) {
			a.Canceled = true
		}
	}

	return json.Marshal(a)
}

func (e *TaskStopEvent) UnmarshalJSON(data []byte) error {
	type alias struct {
		ID        int64     `json:"id"`
		Cancelled bool      `json:"canceled,omitempty"`
		Err       string    `json:"err,omitempty"`
		StoppedAt time.Time `json:"stoppedAt"`
	}

	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}

	e.ID = a.ID
	e.StoppedAt = a.StoppedAt
	if a.Cancelled {
		e.Err = context.Canceled
	} else if a.Err != "" {
		e.Err = errors.New(a.Err)
	}

	return nil
}

// TaskProgressEvent indicates an update to the progress percentage of a task.
type TaskProgressEvent struct {
	ID      int64   `json:"id"`
	Percent float64 `json:"percent"`
}

// TaskDisplayEvent represents an update to the display properties of a task,
// such as title, spinner visibility, or progress bar visibility.
type TaskDisplayEvent struct {
	ID              int64   `json:"id"`
	Title           *string `json:"title,omitempty"`
	DisplaySpinner  *bool   `json:"displaySpinner,omitempty"`
	DisplayProgress *bool   `json:"displayProgress,omitempty"`
	DisplayElapsed  *bool   `json:"displayElapsed,omitempty"`
	HideWhenDone    *bool   `json:"hideWhenDone,omitempty"`
}

// TaskMessageEvent represents a message output from a task, which can be used
// for logging.
type TaskMessageEvent struct {
	ID  int64  `json:"id"`
	Msg []byte `json:"msg"`
}

// TaskETAEvent declares a task's estimated finish time. FinishAt is the absolute
// instant at which the producer expects the task to complete; the zero time
// clears the ETA. Renderers derive the displayed countdown from FinishAt.
type TaskETAEvent struct {
	ID       int64     `json:"id"`
	FinishAt time.Time `json:"finishAt"`
}

// TaskRateEvent declares a task's speed of work as a magnitude plus an opaque
// unit label that carries its own time basis ("MB/s", "items/min"). An empty
// Unit clears the rate; a Rate of 0 is a real value meaning a stalled task.
// Unlike the timing events this is a sample, not something renderers derive:
// the declared value stands until the producer declares another.
type TaskRateEvent struct {
	ID   int64   `json:"id"`
	Rate float64 `json:"rate"`
	Unit string  `json:"unit"`
}

// GenericEvent is a wrapper that allows opaquely encoding and decoding any of
// the defined event types.
type GenericEvent struct {
	Event any // The actual event, which can be one of TitleEvent, TaskStartEvent, TaskStopEvent, TaskProgressEvent, TaskDisplayEvent, TaskMessageEvent, TaskETAEvent, or TaskRateEvent.
}

func (g GenericEvent) MarshalJSON() ([]byte, error) {
	type wrapper struct {
		Type string `json:"type"`
		Raw  any    `json:"raw"`
	}

	var w wrapper

	switch ev := g.Event.(type) {
	case TitleEvent:
		w.Type = "title"
		w.Raw = ev
	case TaskStartEvent:
		w.Type = "taskStart"
		w.Raw = ev
	case TaskStopEvent:
		w.Type = "taskStop"
		w.Raw = ev
	case TaskProgressEvent:
		w.Type = "taskProgress"
		w.Raw = ev
	case TaskDisplayEvent:
		w.Type = "taskDisplay"
		w.Raw = ev
	case TaskMessageEvent:
		w.Type = "taskMessage"
		w.Raw = ev
	case TaskETAEvent:
		w.Type = "taskEta"
		w.Raw = ev
	case TaskRateEvent:
		w.Type = "taskRate"
		w.Raw = ev
	default:
		return nil, fmt.Errorf("unknown event type: %T", g.Event)
	}

	return json.Marshal(w)
}

func (g *GenericEvent) UnmarshalJSON(data []byte) error {
	type wrapper struct {
		Type string          `json:"type"`
		Raw  json.RawMessage `json:"raw"`
	}

	var w wrapper
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}

	switch w.Type {
	case "title":
		var ev TitleEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskStart":
		var ev TaskStartEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskStop":
		var ev TaskStopEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskProgress":
		var ev TaskProgressEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskDisplay":
		var ev TaskDisplayEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskMessage":
		var ev TaskMessageEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskEta":
		var ev TaskETAEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	case "taskRate":
		var ev TaskRateEvent
		if err := json.Unmarshal(w.Raw, &ev); err != nil {
			return err
		}
		g.Event = ev
	default:
		return fmt.Errorf("unknown event type: %s", w.Type)
	}

	return nil
}
