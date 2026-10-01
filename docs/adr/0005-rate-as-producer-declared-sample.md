# Rate as a producer-declared sample

A Task's Rate is a **magnitude plus an opaque unit label**, declared by the
producer via `Task.Rate(value, unit)` and carried on a dedicated
`TaskRateEvent{ID, Rate, Unit}` (JSON `rate` / `unit`). Renderers print it and do
nothing else: they never compute it, never rescale its magnitude, and never
interpolate or age it out. An empty unit is the "no rate" sentinel; visibility is
derived from the unit and the run state, so a Rate vanishes when the Task stops.

This looks like it contradicts ADR-0003, which rejected carrying a `Duration` for
the ETA precisely because a value frozen at emit time "drifts and cannot animate a
live countdown". A Rate is exactly such a frozen value, and we ship it anyway,
because Rate differs from Elapsed and Remaining in kind:

- **Elapsed and Remaining are derivations.** They are pure functions of the clock.
  A single `startedAt` or `finishAt` lets a renderer animate them forever, so
  sending a pre-computed duration would throw away information for nothing.
- **A Rate is a sample.** It depends on work completed, which the renderer cannot
  observe. No amount of clock-watching produces a new rate, so there is nothing
  for the renderer to derive and nothing it can honestly animate.

## Considered Options

- **Producer reports cumulative counts, renderer derives `Δcount/Δt`.** Rejected:
  it hands renderers a smoothing policy — instantaneous, windowed, or average
  since start? — that each would implement differently, so the Bubble Tea and web
  views would disagree about the same stream. That is the failure mode ADR-0003's
  second rejected option warns about, and it makes the renderer originate
  information rather than display it.
- **Renderer rescales the magnitude** (`1234 B/s` → `1.2 kB/s`). Rejected: once
  the unit is opaque the renderer cannot know the base (1000 vs 1024), cannot know
  the label already carries a prefix (`MB/s` → `kMB/s`), and cannot know whether
  scaling is meaningful at all (`1200 items/min` → `1.2 kitems/min`). Picking the
  right unit is the producer's job.
- **Structured unit** (`Rate(120, "items", time.Minute)`). Rejected: the time basis
  is an editorial choice — "items/minute" rather than "items/second" says something
  about the Task's tempo that the renderer cannot infer — and making it structured
  only invites renderers to rebase it.
- **A preformatted string** (`Rate("12.4 MB/s")`). Rejected: the web and JSON
  renderers are machine-facing, and a `float64` on the wire keeps charting and
  thresholding possible at no cost to the producer's control over the label.
- **Folding `Rate` and `Unit` into `TaskProgressEvent`.** Rejected on a concrete
  detail: `Percent` is a plain `float64`, not a pointer, so a rate-only update
  would silently reset progress to `0`. Fixing that means a breaking wire change to
  an unrelated event.
- **Renderers aging out stale rates.** Rejected: it reintroduces a
  renderer-invented policy (what threshold? dim or hide?) and punishes correct
  producers — a deliberately rate-limited job running at a constant `10 items/s`
  has nothing new to say, and re-emitting identical events to defeat a timeout is
  chatter.

## Consequences

- **Keeping a Rate looking live is the producer's obligation.** A producer that
  declares a Rate once every five seconds will show a frozen number beside a
  ticking Elapsed. The fix is to call `Rate()` more often. Renderers must not
  "fix" it by interpolating between samples, which would misrepresent every
  producer whose rate is genuinely constant.
- **`0` is a real value, not a sentinel.** `0 B/s` beside a spinner is a stalled
  Task, which is exactly when the readout matters most. That rules out the obvious
  zero-value sentinel and leaves the empty unit as the only "no rate" signal.
- **No new display flag.** A Rate is shown iff its unit is non-empty and the Task
  is running, so visibility stays a derived, render-time condition, as ADR-0003
  established for the ETA. `Rate` on **id 0** (the Session) is a documented no-op.
- The wire/JSON format gains `TaskRateEvent` and `rate` / `rateUnit` fields on the
  web snapshot and delta — a schema addition to keep backward-aware for stored
  streams.
- A **Canonical Rate Display** joins the canonical duration display of ADR-0004:
  three significant figures with trailing zeros trimmed, a space, then the unit
  verbatim. `formatRate` in `rate.go` and `fmtRate` in `web/termotion.js` must stay
  in step. Fixed decimals were rejected because `0.004 MB/s` would render as
  `0.0 MB/s`, making a slow Task indistinguishable from a stalled one.
