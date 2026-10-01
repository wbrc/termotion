# ETA as a producer-declared finish time

A Task's ETA is expressed as an **absolute finish time** (`time.Time`), declared
by the producer via `Task.ETA(finishAt)` and carried on a dedicated
`TaskETAEvent{ID, FinishAt}` (JSON `finishAt`, RFC3339). Renderers derive the
displayed **Remaining** countdown (`finishAt - now`) at draw time, exactly as they
already derive **Elapsed** from `StartedAt`. This extends ADR-0001: the producer
owns timing, renderers only display it.

## Considered Options

- **Carry a `Duration` instead of an absolute time.** Rejected: a duration frozen
  at emit time drifts and cannot animate a live countdown, and it would sit next
  to a ticking Elapsed looking broken. The same reasoning ADR-0001 used to reject
  renderer-measured timing applies here.
- **A visibility flag plus a value (like progress: `TaskProgressEvent` +
  `TaskDisplayEvent.DisplayProgress`).** Rejected: ETA visibility is fully
  determined by the value and the clock, so a stored flag would be redundant state
  that renderers could disagree with.

## Consequences

- **Hide is a derived, render-time condition, not stored state:** an ETA is shown
  only while `finishAt` is non-zero, still in the future, and the Task is running.
  It disappears on its own when the countdown reaches zero, when `finishAt` is
  already past, or when the Task stops. The zero `time.Time` is the producer's
  explicit "no ETA" sentinel.
- The wire/JSON format gains `TaskETAEvent` and a `finishAt` field on the web
  snapshot/delta — a schema addition to keep backward-aware for stored streams.
- `ETA` on **id 0** (the Session) is a documented no-op; ETA is a Task concept.
- Renderers gain one derived value, **Remaining**, the countdown mirror of Elapsed.
