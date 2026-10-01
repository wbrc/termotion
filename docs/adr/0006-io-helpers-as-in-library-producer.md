# io helpers as an in-library producer

`Task.ProgressReader`, `Task.ProgressWriter` and `Task.Copy` wrap an `io.Reader`
or `io.Writer`, count the bytes that pass through, and declare Percent, Rate and
ETA on the Task's behalf. Each signal is opt-in (`Percent()`, `Rate()`, `ETA()`),
`Total(n)` supplies the byte count, and the helpers never touch a display flag.

On its face this contradicts ADR-0005, which says Termotion "never parses,
rescales, or reasons about the unit", and ADR-0003, which makes the ETA something
the producer declares rather than something Termotion computes. Both stand. The
line those ADRs draw is between **producer** and **renderer**, not between "user
code" and "library code", and these helpers sit squarely on the producer side:
they run in the producer's process, wrap the producer's stream, and observe work
no renderer can see. A renderer receiving their output cannot tell it apart from
a hand-written `t.SetPercent` call, which is the point — the wire contract is
unchanged.

What is new is that the library now ships a *rate estimator*, and an estimator
has policy. That policy is fixed here rather than left to each caller, so that
every Termotion progress readout behaves the same way.

## Considered Options

- **Report on every `Read`/`Write` call instead of on a ticker.** Rejected: a 1 GiB
  copy at 32 KiB per read is ~32k events down a JSON stream, and it has the wrong
  failure mode. A stalled stream stops calling `Read`, so it would emit nothing at
  all — and ADR-0005 forbids renderers from ageing out a Rate, so the screen would
  hold a healthy `12.4 MiB/s` forever beside an ETA marching into the past. A
  clock, not the data, has to drive the readout. The ticker runs from the first
  `Read`/`Write` until end-of-stream or the end of the Task.
- **Cumulative average (`total bytes / total elapsed`) for the rate.** Rejected: it
  barely reacts to a slowdown, which defeats the reason the ticker exists. A
  sliding window over the last N seconds was the runner-up and is exactly as
  defensible; the EMA won on being allocation-free and stateless beyond one float.
  The time constant (3s) and the sample interval (250ms) are exposed as
  `Smoothing()` and `Interval()` rather than buried, because "how twitchy should
  this number be" is genuinely situational.
- **A fixed `B/s` unit.** Rejected: `formatRateValue` never uses exponent notation,
  so a fast transfer renders as `12400000 B/s`. Auto-scaling per sample means the
  unit label can flicker between `999 KiB/s` and `1 MiB/s` at a boundary; fixing
  the scale once from `Total` avoids the flicker but renders a collapsed transfer
  as `0.001 MiB/s`. Flicker at a boundary was judged the cheaper defect.
- **SI (`MB/s`, ÷1000), or the popular ÷1024-labelled-`MB/s`.** Rejected in favour
  of IEC (`KiB/s`, `MiB/s`, ÷1024): unambiguous, and a doc comment can state it
  without embarrassment. This is the producer making the editorial call ADR-0005
  reserves for producers — it is not Termotion rescaling someone else's unit.
- **Sniffing `Total` from the wrapped value** (`*os.File` via `Stat`, `io.Seeker`,
  `*io.LimitedReader`). Rejected: convenient, but seeking a live stream has real
  side effects, and the cases that most need a Total (an HTTP body) cannot be
  sniffed anyway. `Total` is explicit; percent and ETA without one are documented
  no-ops rather than a panic or a silent fallback to a spinner.
- **Enabling the progress bar when `Percent()` is requested.** Rejected: the
  helpers report values, the caller decides what is drawn. Flipping
  `DisplayProgress` would clobber a flag the caller may have set deliberately, and
  turning it off again at end-of-stream fights a caller who wanted the finished bar
  to stay.
- **Emitting an ETA on a stalled stream anyway.** Rejected: with an EMA the rate
  decays towards zero, so `remaining / rate` runs away and the countdown *grows* —
  `5s → 12s → 40s → 3m`. Arithmetically honest, and it looks broken. A rate
  threshold below which the ETA is cleared was considered and rejected for
  inventing a magic constant that the EMA makes fuzzy.

## Consequences

- **A zero-byte tick emits no ETA.** The last estimate is left to count down and
  expire on its own, which the Remaining rules already handle: it vanishes once
  `finishAt` is in the past. So a stall degrades into "no estimate" with no extra
  machinery, while the Rate still decays visibly to `0 B/s` — the one readout ADR-0005
  says must mean "stalled".
- **Percent is clamped to `[0,1]` and snaps to `1` on a clean end, never on error.**
  A lying `Content-Length` cannot hand a renderer a fraction above 1, and a stream
  that ends short still completes its bar — but a failed transfer does not.
- **`ProgressReader` needs no `Close`; `ProgressWriter` does.** A reader announces
  its own end with `io.EOF`. A writer never does, so `Close` is its end-of-stream.
  That `Close` does **not** close the wrapped writer: the wrapper did not open it,
  and `defer pw.Close()` on a wrapper around `os.Stdout` must not close stdout.
  This is a deliberate violation of the usual `io.WriteCloser` expectation and is
  called out in the doc comment.
- **The Task's context is the backstop.** `Go` waits on the subtask's group once
  the task function returns, so a task's own context is always cancelled when the
  task ends. The sampling goroutine cannot outlive its Task even if the caller
  abandons a reader half-read.
- **Wrapping costs `io.Copy` its fast path.** A wrapped reader hides any `io.WriterTo`
  it implements, so `sendfile`/`copy_file_range` are off the table. This is
  inherent: bytes the kernel moves without us cannot be counted.
- **No byte counts on the wire.** `Total` and the running count stay producer-side;
  the protocol carries only the resulting Percent, Rate and ETA. Renderers gain
  nothing to reason about and the wire format is unchanged.
