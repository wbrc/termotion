package termotion

import (
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultProgressInterval  = 250 * time.Millisecond
	defaultProgressSmoothing = 3 * time.Second
)

type progressConfig struct {
	total     int64
	percent   bool
	rate      bool
	eta       bool
	interval  time.Duration
	smoothing time.Duration
}

// ProgressOption configures one of the io progress wrappers returned by
// Task.ProgressReader, Task.ProgressWriter and Task.Copy.
type ProgressOption func(*progressConfig)

// Total declares the expected byte count of the stream being wrapped. It is the
// denominator for Percent and the input to a derived ETA; without it both are
// silently inert. Streams of unknown length (chunked HTTP, a decompressor, a
// pipe) simply omit it.
func Total(n int64) ProgressOption {
	return func(c *progressConfig) { c.total = n }
}

// Percent reports the completion fraction of the stream via Task.SetPercent.
// Requires Total. It does not enable the progress bar — call Task.Progress for
// that.
func Percent() ProgressOption {
	return func(c *progressConfig) { c.percent = true }
}

// Rate reports the throughput of the stream via Task.Rate, auto-scaled to an IEC
// unit (B/s, KiB/s, MiB/s, GiB/s, TiB/s) and smoothed with an exponential moving
// average. A stalled stream decays to 0 B/s rather than holding its last value;
// the readout is cleared once the stream ends.
func Rate() ProgressOption {
	return func(c *progressConfig) { c.rate = true }
}

// ETA reports an estimated finish time via Task.ETA, derived from the remaining
// bytes and the smoothed rate. Requires Total. While the stream is stalled no
// estimate is emitted, so the existing countdown runs out and disappears instead
// of receding.
func ETA() ProgressOption {
	return func(c *progressConfig) { c.eta = true }
}

// Interval sets how often progress is sampled and reported. It defaults to
// 250ms, which is about as fast as a changing readout stays legible.
func Interval(d time.Duration) ProgressOption {
	return func(c *progressConfig) { c.interval = d }
}

// Smoothing sets the time constant of the rate's moving average: larger values
// ride out bursty streams, smaller values react faster. Defaults to 3s.
func Smoothing(d time.Duration) ProgressOption {
	return func(c *progressConfig) { c.smoothing = d }
}

type progressTracker struct {
	task *Task
	cfg  progressConfig

	done atomic.Int64

	started    bool
	startOnce  sync.Once
	stopOnce   sync.Once
	finishOnce sync.Once
	stopped    chan struct{}
	exited     chan struct{}

	// owned by the sampling goroutine
	lastTick time.Time
	lastDone int64
	lastPct  float64
	ema      float64
	haveEMA  bool
}

func newProgressTracker(t *Task, opts []ProgressOption) *progressTracker {
	cfg := progressConfig{
		interval:  defaultProgressInterval,
		smoothing: defaultProgressSmoothing,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.interval <= 0 {
		cfg.interval = defaultProgressInterval
	}
	if cfg.smoothing <= 0 {
		cfg.smoothing = defaultProgressSmoothing
	}

	return &progressTracker{
		task:    t,
		cfg:     cfg,
		stopped: make(chan struct{}),
		exited:  make(chan struct{}),
	}
}

func (p *progressTracker) reports() bool {
	return p.cfg.rate || (p.cfg.total > 0 && (p.cfg.percent || p.cfg.eta))
}

// begin starts sampling before the first byte moves, so a stream that stalls on
// its very first read still shows a decaying rate.
func (p *progressTracker) begin() {
	if !p.reports() {
		return
	}
	p.startOnce.Do(func() {
		p.started = true
		p.lastTick = time.Now()
		go p.sample()
	})
}

func (p *progressTracker) add(n int64) {
	p.done.Add(n)
}

func (p *progressTracker) sample() {
	defer close(p.exited)

	ticker := time.NewTicker(p.cfg.interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopped:
			return
		case <-p.task.ctx.Done():
			return
		case now := <-ticker.C:
			p.tick(now)
		}
	}
}

func (p *progressTracker) tick(now time.Time) {
	elapsed := now.Sub(p.lastTick)
	p.lastTick = now
	if elapsed <= 0 {
		return
	}

	done := p.done.Load()
	delta := done - p.lastDone
	p.lastDone = done

	if p.cfg.percent && delta > 0 {
		p.reportPercent(done)
	}

	if !p.cfg.rate && !p.cfg.eta {
		return
	}

	instant := float64(delta) / elapsed.Seconds()
	if p.haveEMA {
		alpha := 1 - math.Exp(-elapsed.Seconds()/p.cfg.smoothing.Seconds())
		p.ema += alpha * (instant - p.ema)
	} else {
		p.ema = instant
		p.haveEMA = true
	}

	if p.cfg.rate {
		p.task.Rate(scaleByteRate(p.ema))
	}
	// A zero-byte tick gives no basis for an estimate: leave the last one to
	// expire rather than let a decaying rate push it outwards.
	if p.cfg.eta && delta > 0 {
		p.reportETA(now, done)
	}
}

func (p *progressTracker) reportPercent(done int64) {
	if p.cfg.total <= 0 {
		return
	}
	pct := min(max(float64(done)/float64(p.cfg.total), 0), 1)
	if pct == p.lastPct {
		return
	}
	p.lastPct = pct
	p.task.SetPercent(pct)
}

func (p *progressTracker) reportETA(now time.Time, done int64) {
	if p.cfg.total <= 0 || done >= p.cfg.total || p.ema <= 0 {
		return
	}
	seconds := float64(p.cfg.total-done) / p.ema
	if seconds > math.MaxInt64/float64(time.Second) {
		return
	}
	p.task.ETA(now.Add(time.Duration(seconds * float64(time.Second))))
}

// finish stops sampling and quiets the readouts. complete reports whether the
// stream ended of its own accord rather than by error. Only the first call counts.
func (p *progressTracker) finish(complete bool) {
	p.finishOnce.Do(func() {
		p.stop()

		if complete && p.cfg.percent && p.cfg.total > 0 {
			p.task.SetPercent(1)
		}
		if p.cfg.rate {
			p.task.Rate(0, "")
		}
		if p.cfg.eta && p.cfg.total > 0 {
			p.task.ETA(time.Time{})
		}
	})
}

func (p *progressTracker) stop() {
	p.stopOnce.Do(func() { close(p.stopped) })
	if p.started {
		<-p.exited
	}
}

// byteRateUnits are IEC (binary) scales; see docs/adr/0006.
var byteRateUnits = [...]string{"B/s", "KiB/s", "MiB/s", "GiB/s", "TiB/s"}

func scaleByteRate(bytesPerSecond float64) (float64, string) {
	if !(bytesPerSecond > 0) || math.IsInf(bytesPerSecond, 0) {
		return 0, byteRateUnits[0]
	}

	i := 0
	for bytesPerSecond >= 1024 && i < len(byteRateUnits)-1 {
		bytesPerSecond /= 1024
		i++
	}
	return bytesPerSecond, byteRateUnits[i]
}

type progressReader struct {
	r io.Reader
	p *progressTracker
}

func (r *progressReader) Read(b []byte) (int, error) {
	r.p.begin()
	n, err := r.r.Read(b)
	if n > 0 {
		r.p.add(int64(n))
	}
	if err != nil {
		r.p.finish(err == io.EOF)
	}
	return n, err
}

type progressWriter struct {
	w io.Writer
	p *progressTracker
}

func (w *progressWriter) Write(b []byte) (int, error) {
	w.p.begin()
	n, err := w.w.Write(b)
	if n > 0 {
		w.p.add(int64(n))
	}
	if err != nil {
		w.p.finish(false)
	}
	return n, err
}

func (w *progressWriter) Close() error {
	w.p.finish(true)
	return nil
}

// ProgressReader wraps r so that bytes read through it are reported on this task.
// Nothing is reported unless you opt in with Percent, Rate or ETA, and it never
// changes what the task displays — pair it with Task.Progress or Task.Spinner.
//
// Reporting stops when r returns io.EOF or an error, and in any case when the
// task ends; there is nothing to close. Wrapping r hides any io.WriterTo it
// implements, costing io.Copy its fast path — bytes the kernel moves on its own
// cannot be counted.
//
// Rate and ETA are no-ops on the session (id 0) in the bundled renderers.
func (t *Task) ProgressReader(r io.Reader, opts ...ProgressOption) io.Reader {
	return &progressReader{r: r, p: newProgressTracker(t, opts)}
}

// ProgressWriter wraps w so that bytes written through it are reported on this
// task, under the same rules as ProgressReader.
//
// A writer has no end-of-stream of its own, so Close is it: closing the wrapper
// stops reporting and settles the progress bar at 100%. It does not close w —
// the wrapper did not open it. Until Close, or the end of the task, the wrapper
// keeps reporting.
func (t *Task) ProgressWriter(w io.Writer, opts ...ProgressOption) io.WriteCloser {
	return &progressWriter{w: w, p: newProgressTracker(t, opts)}
}

// Copy is io.Copy with progress reported on this task, on the same terms as
// ProgressReader. Bytes are counted as they are read from src.
func (t *Task) Copy(dst io.Writer, src io.Reader, opts ...ProgressOption) (int64, error) {
	p := newProgressTracker(t, opts)
	defer p.finish(false)
	return io.Copy(dst, &progressReader{r: src, p: p})
}
