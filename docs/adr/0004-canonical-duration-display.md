# Canonical duration display

Every renderer formats Elapsed and Remaining with one shared rule: `S.ds` below a
minute (`2.0s`), `Mm SSs` below an hour (`1m05s`), `Hh MMm SSs` above (`1h02m05s`),
always truncated toward zero. The terminal renderer previously leaned on Go's
`Duration.String()`, which drops trailing zeros and so re-flowed the line ten times
a second as a task ticked `2s`, `2.1s`, `2.2s`. The web renderer had independently
invented its own near-identical format — the drift that made a shared rule worth
writing down rather than patching one renderer.

## Considered Options

- **Show tenths at every magnitude** (`1m05.0s`). Rejected: the tenth exists to show
  liveness, and past a minute the whole-second digit already supplies that. Below a
  minute it is the only moving digit; above one it is noise on a line already carrying
  tree prefix, spinner, title, progress bar, Elapsed and ETA.
- **Zero-pad the sub-minute value** (`02.0s`). Rejected: it reads like a clock, and a
  single once-per-task widening at ten seconds was never the problem.
- **Let minutes grow unbounded** (`600m00s`). Rejected: a reader should not have to
  divide by sixty.
- **Ceiling the Remaining countdown while truncating Elapsed.** Rejected: that needs a
  mode flag, and a formatter that knows what it is formatting is exactly the coupling
  that let the two renderers drift apart in the first place.

## Consequences

- The rule has two implementations — `formatDuration` in `duration.go` and
  `fmtDuration` in `web/termotion.js` — that must be changed together. Each carries a
  comment naming the other.
- Truncating the countdown means `ETA 0.0s` can show for a frame before Remaining
  vanishes.
- Line width still changes twice over a long task's life, at ten seconds and at one
  minute. That is the accepted floor, not an oversight.
