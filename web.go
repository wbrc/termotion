package termotion

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	webWriteWait  = 10 * time.Second
	webPongWait   = 60 * time.Second
	webPingPeriod = (webPongWait * 9) / 10
	webClientBuf  = 256
)

// wireTask is the JSON shape of a task in the snapshot sent to clients.
type wireTask struct {
	ID              int64    `json:"id"`
	Parent          int64    `json:"parent"`
	Title           string   `json:"title"`
	StartedAt       string   `json:"startedAt,omitempty"`
	StoppedAt       string   `json:"stoppedAt,omitempty"`
	FinishAt        string   `json:"finishAt,omitempty"`
	Done            bool     `json:"done"`
	Canceled        bool     `json:"canceled"`
	Err             string   `json:"err,omitempty"`
	Percent         float64  `json:"percent"`
	Rate            float64  `json:"rate"`
	RateUnit        string   `json:"rateUnit,omitempty"`
	DisplaySpinner  bool     `json:"displaySpinner"`
	DisplayProgress bool     `json:"displayProgress"`
	DisplayElapsed  bool     `json:"displayElapsed"`
	HideWhenDone    bool     `json:"hideWhenDone"`
	Messages        []string `json:"messages"`
}

// wireSnapshot is the full-state message sent to a client on connect.
type wireSnapshot struct {
	Kind      string     `json:"kind"`
	Title     string     `json:"title"`
	StartedAt string     `json:"startedAt,omitempty"`
	StoppedAt string     `json:"stoppedAt,omitempty"`
	Tasks     []wireTask `json:"tasks"`
}

// wireEvent is a single live delta broadcast to connected clients. The id field
// is always present so id 0 can denote the session.
type wireEvent struct {
	Kind            string   `json:"kind"`
	ID              int64    `json:"id"`
	Parent          int64    `json:"parent,omitempty"`
	Title           *string  `json:"title,omitempty"`
	StartedAt       string   `json:"startedAt,omitempty"`
	StoppedAt       string   `json:"stoppedAt,omitempty"`
	FinishAt        string   `json:"finishAt,omitempty"`
	Canceled        bool     `json:"canceled,omitempty"`
	Err             string   `json:"err,omitempty"`
	Percent         *float64 `json:"percent,omitempty"`
	Rate            *float64 `json:"rate,omitempty"`
	RateUnit        *string  `json:"rateUnit,omitempty"`
	DisplaySpinner  *bool    `json:"displaySpinner,omitempty"`
	DisplayProgress *bool    `json:"displayProgress,omitempty"`
	DisplayElapsed  *bool    `json:"displayElapsed,omitempty"`
	HideWhenDone    *bool    `json:"hideWhenDone,omitempty"`
	Messages        []string `json:"messages,omitempty"`
}

// webTaskState is the reduced server-side state for a single task.
type webTaskState struct {
	id              int64
	parent          int64
	title           string
	startedAt       time.Time
	stoppedAt       time.Time
	done            bool
	canceled        bool
	errMsg          string
	percent         float64
	finishAt        time.Time
	rate            float64
	rateUnit        string
	displaySpinner  bool
	displayProgress bool
	displayElapsed  bool
	hideWhenDone    bool
	messages        *ringBuffer
}

func (t *webTaskState) wire() wireTask {
	return wireTask{
		ID:              t.id,
		Parent:          t.parent,
		Title:           t.title,
		StartedAt:       rfc(t.startedAt),
		StoppedAt:       rfc(t.stoppedAt),
		FinishAt:        rfc(t.finishAt),
		Done:            t.done,
		Canceled:        t.canceled,
		Err:             t.errMsg,
		Percent:         t.percent,
		Rate:            t.rate,
		RateUnit:        t.rateUnit,
		DisplaySpinner:  t.displaySpinner,
		DisplayProgress: t.displayProgress,
		DisplayElapsed:  t.displayElapsed,
		HideWhenDone:    t.hideWhenDone,
		Messages:        collectLines(t.messages),
	}
}

// webState is the reduced snapshot of a whole session, kept so that any client
// connecting mid-run immediately receives an up-to-date view.
type webState struct {
	title     string
	startedAt time.Time
	stoppedAt time.Time
	tasks     map[int64]*webTaskState
	order     []int64
	msgLimit  int
}

// webClient is a single connected browser, fed serialized messages over send.
type webClient struct {
	send chan []byte
}

// webHub reduces the event stream into webState and fans messages out to all
// connected clients. All state access is guarded by mu so that a client's
// snapshot is always consistent with the deltas it subsequently receives.
type webHub struct {
	mu      sync.Mutex
	state   *webState
	clients map[*webClient]struct{}
}

func newWebHub(msgLimit int) *webHub {
	return &webHub{
		state: &webState{
			tasks:    make(map[int64]*webTaskState),
			msgLimit: msgLimit,
		},
		clients: make(map[*webClient]struct{}),
	}
}

// broadcast applies an event to the cached state and forwards the resulting
// delta to every connected client.
func (h *webHub) broadcast(e any) {
	h.mu.Lock()
	defer h.mu.Unlock()

	msg := h.reduce(e)
	if msg == nil {
		return
	}

	for c := range h.clients {
		select {
		case c.send <- msg:
		default:
			// Slow client: drop it rather than stalling the whole hub.
			delete(h.clients, c)
			close(c.send)
		}
	}
}

// reduce mutates the cached state for e and returns the serialized delta to
// broadcast, or nil if the event is a no-op.
func (h *webHub) reduce(e any) []byte {
	s := h.state
	switch ev := e.(type) {
	case TitleEvent:
		s.title = ev.Title
		title := ev.Title
		return mustJSON(wireEvent{Kind: "title", Title: &title})

	case TaskStartEvent:
		if ev.ID == 0 {
			s.startedAt = ev.StartedAt
			return mustJSON(wireEvent{Kind: "taskStart", ID: 0, StartedAt: rfc(ev.StartedAt)})
		}
		if _, ok := s.tasks[ev.ID]; ok {
			return nil
		}
		t := &webTaskState{
			id:        ev.ID,
			parent:    ev.Parent,
			startedAt: ev.StartedAt,
			messages:  newRingBuffer(s.msgLimit),
			// Elapsed is shown unless the producer opts out, so it cannot rely on the zero value.
			displayElapsed: true,
		}
		s.tasks[ev.ID] = t
		s.order = append(s.order, ev.ID)
		return mustJSON(wireEvent{Kind: "taskStart", ID: ev.ID, Parent: ev.Parent, StartedAt: rfc(ev.StartedAt)})

	case TaskStopEvent:
		if ev.ID == 0 {
			s.stoppedAt = ev.StoppedAt
			return mustJSON(wireEvent{Kind: "taskStop", ID: 0, StoppedAt: rfc(ev.StoppedAt)})
		}
		t, ok := s.tasks[ev.ID]
		if !ok || t.done {
			return nil
		}
		t.done = true
		t.stoppedAt = ev.StoppedAt
		w := wireEvent{Kind: "taskStop", ID: ev.ID, StoppedAt: rfc(ev.StoppedAt)}
		if ev.Err != nil {
			if errors.Is(ev.Err, context.Canceled) {
				t.canceled = true
				w.Canceled = true
			} else {
				t.errMsg = ev.Err.Error()
				w.Err = t.errMsg
			}
		}
		return mustJSON(w)

	case TaskProgressEvent:
		t, ok := s.tasks[ev.ID]
		if !ok {
			return nil
		}
		t.percent = ev.Percent
		p := ev.Percent
		return mustJSON(wireEvent{Kind: "taskProgress", ID: ev.ID, Percent: &p})

	case TaskETAEvent:
		if ev.ID == 0 {
			return nil
		}
		t, ok := s.tasks[ev.ID]
		if !ok {
			return nil
		}
		t.finishAt = ev.FinishAt
		return mustJSON(wireEvent{Kind: "taskEta", ID: ev.ID, FinishAt: rfc(ev.FinishAt)})

	case TaskRateEvent:
		if ev.ID == 0 {
			return nil
		}
		t, ok := s.tasks[ev.ID]
		if !ok {
			return nil
		}
		t.rate = ev.Rate
		t.rateUnit = ev.Unit
		// Pointers so a stalled task (rate 0) and a cleared rate (empty unit) both
		// survive omitempty.
		rate, unit := ev.Rate, ev.Unit
		return mustJSON(wireEvent{Kind: "taskRate", ID: ev.ID, Rate: &rate, RateUnit: &unit})

	case TaskDisplayEvent:
		t, ok := s.tasks[ev.ID]
		if !ok {
			return nil
		}
		w := wireEvent{Kind: "taskDisplay", ID: ev.ID}
		if ev.Title != nil {
			t.title = *ev.Title
			w.Title = ev.Title
		}
		if ev.DisplaySpinner != nil {
			t.displaySpinner = *ev.DisplaySpinner
			w.DisplaySpinner = ev.DisplaySpinner
		}
		if ev.DisplayProgress != nil {
			t.displayProgress = *ev.DisplayProgress
			w.DisplayProgress = ev.DisplayProgress
		}
		if ev.DisplayElapsed != nil {
			t.displayElapsed = *ev.DisplayElapsed
			w.DisplayElapsed = ev.DisplayElapsed
		}
		if ev.HideWhenDone != nil {
			t.hideWhenDone = *ev.HideWhenDone
			w.HideWhenDone = ev.HideWhenDone
		}
		return mustJSON(w)

	case TaskMessageEvent:
		t, ok := s.tasks[ev.ID]
		if !ok {
			return nil
		}
		t.messages.add(string(ev.Msg))
		// Send the task's current bounded line set so clients stay consistent
		// with the snapshot without re-implementing the ring buffer.
		return mustJSON(wireEvent{Kind: "taskMessage", ID: ev.ID, Messages: collectLines(t.messages)})
	}
	return nil
}

// register snapshots the current state and adds c under the same lock so that
// no delta is lost or duplicated relative to the snapshot.
func (h *webHub) register(c *webClient) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := h.snapshotLocked()
	h.clients[c] = struct{}{}
	return snap
}

func (h *webHub) unregister(c *webClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
}

func (h *webHub) snapshotLocked() []byte {
	s := h.state
	snap := wireSnapshot{
		Kind:      "snapshot",
		Title:     s.title,
		StartedAt: rfc(s.startedAt),
		StoppedAt: rfc(s.stoppedAt),
	}
	for _, id := range s.order {
		snap.Tasks = append(snap.Tasks, s.tasks[id].wire())
	}
	return mustJSON(snap)
}

type webConfig struct {
	msgLimit    int
	checkOrigin func(*http.Request) bool
}

// WebOpt is a functional option for configuring a WebRenderer.
type WebOpt func(*webConfig)

// WithWebMessageLimit sets how many log lines are retained per task in the
// server-side cache (default 5, matching the Bubble Tea renderer).
func WithWebMessageLimit(n int) WebOpt {
	return func(c *webConfig) {
		if n > 0 {
			c.msgLimit = n
		}
	}
}

// WithWebCheckOrigin sets the WebSocket origin check. By default all origins are
// allowed; pass a stricter function to restrict which pages may connect.
func WithWebCheckOrigin(fn func(*http.Request) bool) WebOpt {
	return func(c *webConfig) {
		if fn != nil {
			c.checkOrigin = fn
		}
	}
}

// WebRenderer is both a Renderer (via Render) that consumes the event stream and
// an http.Handler exposing a single WebSocket endpoint. It caches session state
// so any client connecting mid-run immediately receives an up-to-date view, then
// streams live deltas.
//
// The WebRenderer does not serve the browser client. Delivering the JS/CSS is the
// library user's responsibility; the assets live in the repository's web/
// directory. Serve them however suits your app — for example copy termotion.js
// and termotion.css into your own static assets, or pin them from a CDN against a
// matching release tag. The JS/CSS and the Go wire protocol are released together,
// so keep them on the same version.
//
// Mount it at the WebSocket path and feed its Render method to New:
//
//	web := termotion.NewWebRenderer()
//	http.Handle("/termotion/ws", web)
//	t := termotion.New(ctx, web.Render)
//
// Point the client's data-termotion-url attribute at that same path.
type WebRenderer struct {
	hub      *webHub
	upgrader websocket.Upgrader
}

// NewWebRenderer creates a WebRenderer.
func NewWebRenderer(opts ...WebOpt) *WebRenderer {
	cfg := webConfig{
		msgLimit:    5,
		checkOrigin: func(*http.Request) bool { return true },
	}
	for _, o := range opts {
		o(&cfg)
	}

	return &WebRenderer{
		hub:      newWebHub(cfg.msgLimit),
		upgrader: websocket.Upgrader{CheckOrigin: cfg.checkOrigin},
	}
}

// Render implements the Renderer signature. It blocks until the events channel
// is closed, reducing every event into the cached state and broadcasting deltas
// to connected clients. Connections stay open after the session ends so late
// viewers still see the final state.
func (wr *WebRenderer) Render(events <-chan any) {
	for e := range events {
		wr.hub.broadcast(e)
	}
}

// ServeHTTP upgrades every request to the WebSocket event stream. Mount it
// directly at the path you want the socket on; it owns no sub-paths.
func (wr *WebRenderer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := wr.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := &webClient{send: make(chan []byte, webClientBuf)}
	snapshot := wr.hub.register(client)

	go wr.writePump(conn, client, snapshot)
	wr.readPump(conn, client)
}

// readPump drains incoming frames (only control frames are expected) so that
// pongs and close notifications are processed, and unregisters on exit.
func (wr *WebRenderer) readPump(conn *websocket.Conn, c *webClient) {
	defer func() {
		wr.hub.unregister(c)
		conn.Close()
	}()

	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(webPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(webPongWait))
		return nil
	})

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// writePump sends the initial snapshot, then streams deltas and periodic pings.
func (wr *WebRenderer) writePump(conn *websocket.Conn, c *webClient, snapshot []byte) {
	ticker := time.NewTicker(webPingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	conn.SetWriteDeadline(time.Now().Add(webWriteWait))
	if err := conn.WriteMessage(websocket.TextMessage, snapshot); err != nil {
		return
	}

	for {
		select {
		case msg, ok := <-c.send:
			conn.SetWriteDeadline(time.Now().Add(webWriteWait))
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(webWriteWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func rfc(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func collectLines(r *ringBuffer) []string {
	out := make([]string, 0)
	for l := range r.lines() {
		out = append(out, l)
	}
	return out
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
