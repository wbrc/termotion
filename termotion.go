package termotion

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// Task represents a unit of work that can report progress, spawn subtasks, and emit log messages.
type Task struct {
	id        int64
	termotion *Termotion
	g         *errgroup.Group
	ctx       context.Context
}

// Write implements io.Writer, sending p as a log message for this task.
func (t *Task) Write(p []byte) (n int, err error) {
	t.termotion.events <- TaskMessageEvent{
		ID:  t.id,
		Msg: p,
	}
	return len(p), nil
}

func (t *Task) sub() *Task {
	g, ctx := errgroup.WithContext(t.ctx)
	return &Task{
		id:        t.termotion.nexID.Add(1),
		termotion: t.termotion,
		g:         g,
		ctx:       ctx,
	}
}

var (
	enabledTrue  = new(true)
	enabledFalse = new(false)
	isEnabled    = func(b bool) *bool {
		if b {
			return enabledTrue
		}
		return enabledFalse
	}
)

// Spinner enables or disables the spinner animation for this task.
func (t *Task) Spinner(enabled bool) {
	t.termotion.events <- TaskDisplayEvent{
		ID:             t.id,
		DisplaySpinner: isEnabled(enabled),
	}
}

// Title updates the display name of this task.
func (t *Task) Title(title string) {
	t.termotion.events <- TaskDisplayEvent{
		ID:    t.id,
		Title: new(title),
	}
}

// Progress enables or disables the progress bar for this task.
func (t *Task) Progress(enabled bool) {
	t.termotion.events <- TaskDisplayEvent{
		ID:              t.id,
		DisplayProgress: isEnabled(enabled),
	}
}

// Elapsed shows or hides this task's elapsed-time readout. Unlike the spinner
// and progress bar it is shown by default. Hiding it does not affect the ETA
// countdown. Elapsed on the session (id 0) is a no-op in the bundled renderers.
func (t *Task) Elapsed(enabled bool) {
	t.termotion.events <- TaskDisplayEvent{
		ID:             t.id,
		DisplayElapsed: isEnabled(enabled),
	}
}

// HideWhenDone removes this task, with its subtasks, from the rendered tree once
// it stops successfully. Failed and canceled tasks stay visible.
func (t *Task) HideWhenDone(enabled bool) {
	t.termotion.events <- TaskDisplayEvent{
		ID:           t.id,
		HideWhenDone: isEnabled(enabled),
	}
}

// SetPercent sets the progress bar completion percentage (0.0 to 1.0).
func (t *Task) SetPercent(percent float64) {
	t.termotion.events <- TaskProgressEvent{
		ID:      t.id,
		Percent: percent,
	}
}

// ETA declares this task's estimated finish time. finishAt is the absolute
// instant at which the task is expected to complete; renderers show a live
// countdown to it. Passing the zero time clears the ETA. ETA on the session
// (id 0) is a no-op in the bundled renderers.
func (t *Task) ETA(finishAt time.Time) {
	t.termotion.events <- TaskETAEvent{
		ID:       t.id,
		FinishAt: finishAt,
	}
}

// Rate declares this task's speed of work as a magnitude plus a unit label that
// carries its own time basis, e.g. Rate(12.4, "MB/s") or Rate(120, "items/min").
// The unit is opaque to termotion: it is reproduced verbatim and the magnitude is
// never rescaled, so picking a readable unit is the producer's job. An empty unit
// clears the rate; 0 is a real value meaning the task is stalled.
//
// Unlike the elapsed and ETA readouts, a rate cannot be animated from a single
// event — it depends on work only the producer can see. Call Rate as often as you
// want the displayed value to look live. Rate on the session (id 0) is a no-op in
// the bundled renderers.
func (t *Task) Rate(rate float64, unit string) {
	t.termotion.events <- TaskRateEvent{
		ID:   t.id,
		Rate: rate,
		Unit: unit,
	}
}

// Subtask spawns a subtask that runs in the task's error group and waits
// for its completion, returning any error.
func (t *Task) Subtask(f func(t *Task) error) error {
	errChan := make(chan error)
	t.Go(func(sub *Task) error {
		err := f(sub)
		errChan <- err
		return err
	})
	return <-errChan
}

// Go spawns a subtask that runs concurrently in the task's error group.
func (t *Task) Go(f func(t *Task) error) {
	subtask := t.sub()

	t.termotion.events <- TaskStartEvent{
		ID:        subtask.id,
		Parent:    t.id,
		StartedAt: time.Now(),
	}

	t.g.Go(func() error {
		err := f(subtask)
		waitErr := subtask.Wait()
		if err == nil {
			err = waitErr
		}
		t.termotion.events <- TaskStopEvent{
			ID:        subtask.id,
			Err:       err,
			StoppedAt: time.Now(),
		}
		return err
	})
}

// Context returns the context associated with this task.
func (t *Task) Context() context.Context {
	return t.ctx
}

// Wait blocks until all subtasks in the error group have completed and returns the first error.
func (t *Task) Wait() error {
	return t.g.Wait()
}

// Termotion is the top-level orchestrator that manages tasks and dispatches events to a renderer.
type Termotion struct {
	events       chan any
	rendererDone chan struct{}
	nexID        atomic.Int64
	Task
}

// Title sets the top-level title displayed by the renderer.
func (t *Termotion) Title(title string) {
	t.events <- TitleEvent{Title: title}
}

// Close waits for all tasks in the error group to finish, closes the event
// channel to notify the renderer, and returns the first error. After calling
// Close, the Termotion instance must not be used anymore.
func (t *Termotion) Close() error {
	err := t.Wait()
	t.events <- TaskStopEvent{ID: 0, StoppedAt: time.Now()}
	close(t.events)
	<-t.rendererDone
	return err
}

// Renderer is a function that consumes events and renders them. It blocks until
// the events channel is closed and the renderer has finished processing all
// events, at which point it returns.
type Renderer func(events <-chan any)

// MultiRenderer returns a Renderer that dispatches events to multiple renderers.
func MultiRenderer(renderers ...Renderer) Renderer {
	return func(events <-chan any) {
		chans := make([]chan any, len(renderers))
		wg := sync.WaitGroup{}
		for i := range renderers {
			chans[i] = make(chan any)
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				renderers[i](chans[i])
			}(i)
		}

		for e := range events {
			for _, ch := range chans {
				ch <- e
			}
		}
		for _, ch := range chans {
			close(ch)
		}

		wg.Wait()
	}
}

// New creates a Termotion instance with the given renderer
func New(ctx context.Context, renderer Renderer) (t *Termotion) {
	t = &Termotion{
		events:       make(chan any),
		rendererDone: make(chan struct{}),
	}
	t.Task.termotion = t
	t.Task.g, t.Task.ctx = errgroup.WithContext(ctx)

	go func() {
		renderer(t.events)
		close(t.rendererDone)
	}()

	t.events <- TaskStartEvent{ID: 0, StartedAt: time.Now()}

	return t
}
