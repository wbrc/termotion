# Producer-sourced task timing carried on events

Task and Session start/stop times are captured by the **producer** (`time.Now()`
when it emits `TaskStartEvent` / `TaskStopEvent`) and carried on those events as
absolute `time.Time` values (`StartedAt` / `StoppedAt`, RFC3339 in JSON), rather
than being measured by each renderer's `stopwatch`. Renderers derive **Elapsed**
(`now - startedAt`, frozen at `stoppedAt - startedAt`) at draw time; the per-task
stopwatch is kept only as a re-render ticker.

## Considered Options

- **Renderer measures time (status quo).** Rejected: every renderer re-derives
  timing independently, and the remote/JSON renderer (events cross a
  `StreamRenderer` → `RenderStream` hop) would report times skewed by transport
  delay. Timing belongs at the source.
- **Send a computed `Duration` on stop only.** Rejected: cannot drive a live
  counter, cannot express "still running", and loses the monotonic-clock benefit
  of an in-process `time.Time`.
- **Dedicated `SessionEvent` for the root timer.** Rejected in favour of reusing
  `TaskStart/StopEvent` with **id 0** as the session. Real task ids start at 1, so
  id 0 is an unambiguous sentinel and needs no new event type or JSON case.

## Consequences

- The event/JSON wire format gains `startedAt` / `stoppedAt` fields — a schema
  change to be kept backward-aware for any stored streams.
- Renderers must special-case **id 0** as the Session: set session start/stop
  times, draw no task node.
- In-process renderers benefit from the monotonic clock in `time.Now()`; across
  the JSON boundary only wall-clock (RFC3339) survives, which is acceptable.
