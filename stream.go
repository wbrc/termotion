package termotion

import (
	"encoding/json"
	"io"
)

// StreamRenderer returns a Renderer that serializes events as JSON and writes
// them to the provided io.WriteCloser. w will be closed when the events channel is closed.
func StreamRenderer(w io.WriteCloser) Renderer {
	return func(events <-chan any) {
		enc := json.NewEncoder(w)
		for e := range events {
			enc.Encode(GenericEvent{e})
		}
		w.Close()
	}
}

// RenderStream reads JSON-encoded events from r and renders them with the given
// Renderer. It will read until it encounters an error or EOF, at which point it
// will close the events channel and wait for the Renderer to finish processing
// before returning any non EOF error.
func RenderStream(w io.Reader, r Renderer) (err error) {
	events := make(chan any)

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		r(events)
	}()

	dec := json.NewDecoder(w)
	for {
		var e GenericEvent
		if err = dec.Decode(&e); err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			break
		}
		events <- e.Event
	}
	close(events)
	<-drained
	return err
}
