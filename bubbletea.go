package termotion

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/stopwatch"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type task struct {
	id              int64
	title           string
	messages        *ringBuffer
	startedAt       time.Time
	stoppedAt       time.Time
	stopWatch       stopwatch.Model
	spinner         spinner.Model
	displaySpinner  bool
	progress        progress.Model
	percent         float64
	displayProgress bool
	displayElapsed  bool
	hideWhenDone    bool
	finishAt        time.Time
	rate            float64
	rateUnit        string
	done            bool
	err             error
	subtasks        []*task
}

// elapsed is the task's display duration, derived from the producer-captured
// start/stop times: live while running, frozen once stopped.
func (t *task) elapsed() time.Duration {
	if t.startedAt.IsZero() {
		return 0
	}
	if t.done {
		return t.stoppedAt.Sub(t.startedAt)
	}
	return time.Since(t.startedAt)
}

// remaining is the task's ETA countdown, derived from the producer-declared
// finishAt. It is only meaningful (positive) while the task is running and
// finishAt is in the future; ok is false when no ETA should be shown.
func (t *task) remaining() (d time.Duration, ok bool) {
	if t.done || t.finishAt.IsZero() {
		return 0, false
	}
	d = time.Until(t.finishAt)
	if d <= 0 {
		return 0, false
	}
	return d, true
}

// showRate reports whether the producer-declared rate should be drawn. Unlike
// elapsed and remaining there is nothing to derive: the last declared sample
// stands until replaced, and an empty unit is the producer's "no rate" sentinel.
func (t *task) showRate() bool {
	return !t.done && t.rateUnit != ""
}

// hidden reports whether the task opted out of the tree and stopped successfully.
func (t *task) hidden() bool {
	return t.hideWhenDone && t.done && t.err == nil
}

func visibleTasks(tasks []*task) (visible []*task, hidden int) {
	for _, t := range tasks {
		if t.hidden() {
			hidden++
		} else {
			visible = append(visible, t)
		}
	}
	return visible, hidden
}

func renderHiddenSummary(n int, prefix string, last bool) string {
	connector := defaultStyles.childConnector + " "
	if last {
		connector = defaultStyles.lastChildConnector + " "
	}
	noun := "tasks"
	if n == 1 {
		noun = "task"
	}
	return prefix + defaultStyles.treeStyle.Render(connector) +
		defaultStyles.doneStyle.Render(defaultStyles.doneSymbol) + " " +
		defaultStyles.doneStyle.Render(fmt.Sprintf("%d %s done", n, noun)) + "\n"
}

func (t *task) update(msg tea.Msg) []tea.Cmd {
	var cmds []tea.Cmd
	if !t.done {
		var cmd tea.Cmd
		t.stopWatch, cmd = t.stopWatch.Update(msg)
		cmds = append(cmds, cmd)

		t.spinner, cmd = t.spinner.Update(msg)
		cmds = append(cmds, cmd)

		t.progress, cmd = t.progress.Update(msg)
		cmds = append(cmds, cmd)
	}
	return cmds
}

type (
	closeEvent     struct{}
	quitTimeoutMsg struct{}
)

type styleConfig struct {
	titleStyle    lipgloss.Style
	treeStyle     lipgloss.Style
	msgStyle      lipgloss.Style
	runningStyle  lipgloss.Style
	doneStyle     lipgloss.Style
	errorStyle    lipgloss.Style
	canceledStyle lipgloss.Style
	nameStyle     lipgloss.Style
	timeStyle     lipgloss.Style
	rateStyle     lipgloss.Style

	spinnerOptions  []spinner.Option
	progressOptions []progress.Option

	verticalConnector  string
	lastChildConnector string
	childConnector     string
	errorSymbol        string
	canceledSymbol     string
	doneSymbol         string
	runningSymbol      string
}

type bubbleteaRendererState struct {
	title       string
	width       int
	startedAt   time.Time
	stoppedAt   time.Time
	stopWatch   stopwatch.Model
	subtasks    []*task
	allTasks    map[int64]*task
	wantQuit    bool
	modeReports int
	styles      *styleConfig

	hiddenSummary bool
}

// elapsed is the session's total display duration, derived from the
// producer-captured session start/stop times (session is task id 0).
func (s *bubbleteaRendererState) elapsed() time.Duration {
	if s.startedAt.IsZero() {
		return 0
	}
	if !s.stoppedAt.IsZero() {
		return s.stoppedAt.Sub(s.startedAt)
	}
	return time.Since(s.startedAt)
}

func (s *bubbleteaRendererState) handleTaskStartEvent(e TaskStartEvent) []tea.Cmd {
	var cmds []tea.Cmd

	// id 0 is the session, not a task node
	if e.ID == 0 {
		s.startedAt = e.StartedAt
		return cmds
	}

	// id already exists, ignore
	if _, ok := s.allTasks[e.ID]; ok {
		return cmds
	}

	sw := stopwatch.New(stopwatch.WithInterval(100 * time.Millisecond))
	cmds = append(cmds, sw.Init())
	spinner := spinner.New(s.styles.spinnerOptions...)
	cmds = append(cmds, spinner.Tick)
	t := &task{
		id:        e.ID,
		startedAt: e.StartedAt,
		stopWatch: sw,
		spinner:   spinner,
		progress:  progress.New(s.styles.progressOptions...),
		messages:  newRingBuffer(5),
		// Elapsed is shown unless the producer opts out, so it cannot rely on the zero value.
		displayElapsed: true,
	}
	s.allTasks[e.ID] = t
	if e.Parent != 0 {
		parent, ok := s.allTasks[e.Parent]
		if ok {
			parent.subtasks = append(parent.subtasks, t)
		}
	} else {
		s.subtasks = append(s.subtasks, t)
	}

	return cmds
}

func (s *bubbleteaRendererState) handleTaskStopEvent(e TaskStopEvent) []tea.Cmd {
	var cmds []tea.Cmd

	// id 0 is the session, not a task node
	if e.ID == 0 {
		s.stoppedAt = e.StoppedAt
		return cmds
	}

	t, ok := s.allTasks[e.ID]

	// id doesn't exist, ignore
	if !ok {
		return cmds
	}

	// already marked done, ignore
	if t.done {
		return cmds
	}

	t.done = true
	t.err = e.Err
	t.stoppedAt = e.StoppedAt
	cmds = append(cmds, t.stopWatch.Stop())
	return cmds
}

func (s *bubbleteaRendererState) handleTaskProgressEvent(e TaskProgressEvent) []tea.Cmd {
	var cmds []tea.Cmd

	t, ok := s.allTasks[e.ID]

	// id doesn't exist, ignore
	if !ok {
		return cmds
	}

	t.percent = e.Percent

	return cmds
}

func (s *bubbleteaRendererState) handleTaskETAEvent(e TaskETAEvent) []tea.Cmd {
	var cmds []tea.Cmd

	// id 0 is the session, which has no ETA
	if e.ID == 0 {
		return cmds
	}

	t, ok := s.allTasks[e.ID]
	if !ok {
		return cmds
	}

	t.finishAt = e.FinishAt

	return cmds
}

func (s *bubbleteaRendererState) handleTaskRateEvent(e TaskRateEvent) []tea.Cmd {
	var cmds []tea.Cmd

	// id 0 is the session, which has no rate
	if e.ID == 0 {
		return cmds
	}

	t, ok := s.allTasks[e.ID]
	if !ok {
		return cmds
	}

	t.rate = e.Rate
	t.rateUnit = e.Unit

	return cmds
}

func (s *bubbleteaRendererState) handleTaskDisplayEvent(e TaskDisplayEvent) []tea.Cmd {
	var cmds []tea.Cmd

	t, ok := s.allTasks[e.ID]

	// id doesn't exist, ignore
	if !ok {
		return cmds
	}

	if e.Title != nil {
		t.title = *e.Title
	}

	if e.DisplaySpinner != nil {
		t.displaySpinner = *e.DisplaySpinner
	}

	if e.DisplayProgress != nil {
		t.displayProgress = *e.DisplayProgress
	}

	if e.DisplayElapsed != nil {
		t.displayElapsed = *e.DisplayElapsed
	}

	if e.HideWhenDone != nil {
		t.hideWhenDone = *e.HideWhenDone
	}

	return cmds
}

func (s *bubbleteaRendererState) Init() tea.Cmd {
	s.stopWatch = stopwatch.New(stopwatch.WithInterval(100 * time.Millisecond))
	return s.stopWatch.Init()
}

func (s *bubbleteaRendererState) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		// app is in raw mode, so we manually send INT on ctrl+c
		if msg.String() == "ctrl+c" {
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
		}
	case tea.WindowSizeMsg:
		s.width = msg.Width
	case TitleEvent:
		s.title = msg.Title
	case TaskStartEvent:
		cmds = append(cmds, s.handleTaskStartEvent(msg)...)
	case TaskStopEvent:
		cmds = append(cmds, s.handleTaskStopEvent(msg)...)
	case TaskProgressEvent:
		cmds = append(cmds, s.handleTaskProgressEvent(msg)...)
	case TaskETAEvent:
		cmds = append(cmds, s.handleTaskETAEvent(msg)...)
	case TaskRateEvent:
		cmds = append(cmds, s.handleTaskRateEvent(msg)...)
	case TaskDisplayEvent:
		cmds = append(cmds, s.handleTaskDisplayEvent(msg)...)
	case TaskMessageEvent:
		if t, ok := s.allTasks[msg.ID]; ok {
			t.messages.add(string(msg.Msg))
		}
	case closeEvent:
		s.wantQuit = true
		if s.modeReports >= 2 {
			return s, tea.Quit
		}
		// Fallback timeout in case mode reports don't arrive (e.g., not a TTY).
		cmds = append(cmds, tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg {
			return quitTimeoutMsg{}
		}))
	case tea.ModeReportMsg:
		s.modeReports++
		if s.wantQuit && s.modeReports >= 2 {
			return s, tea.Quit
		}
	case quitTimeoutMsg:
		if s.wantQuit {
			return s, tea.Quit
		}
	}

	var nmsg tea.Cmd
	s.stopWatch, nmsg = s.stopWatch.Update(msg)
	cmds = append(cmds, nmsg)

	for _, t := range s.allTasks {
		cmds = append(cmds, t.update(msg)...)
	}

	return s, tea.Batch(cmds...)
}

func wrapLine(s string, width int) []string {
	if width <= 0 || len(s) <= width {
		return []string{s}
	}
	var lines []string
	for len(s) > width {
		lines = append(lines, s[:width])
		s = s[width:]
	}
	if len(s) > 0 {
		lines = append(lines, s)
	}
	return lines
}

func renderTask(t *task, prefix string, last bool, width int, hiddenSummary bool) string {
	var connector string
	if last {
		connector = defaultStyles.lastChildConnector + " "
	} else {
		connector = defaultStyles.childConnector + " "
	}

	var status string
	var styledName string
	switch {
	case t.err != nil && !errors.Is(t.err, context.Canceled):
		status = defaultStyles.errorStyle.Render(defaultStyles.errorSymbol)
		styledName = defaultStyles.errorStyle.Render(t.title)
	case t.err != nil && errors.Is(t.err, context.Canceled):
		status = defaultStyles.canceledStyle.Render(defaultStyles.canceledSymbol)
		styledName = defaultStyles.canceledStyle.Render(t.title)
	case t.done:
		status = defaultStyles.doneStyle.Render(defaultStyles.doneSymbol)
		styledName = defaultStyles.doneStyle.Render(t.title)
	default:
		if t.displaySpinner {
			status = defaultStyles.runningStyle.Render(t.spinner.View())
		} else {
			status = defaultStyles.runningStyle.Render(defaultStyles.runningSymbol)
		}
		styledName = defaultStyles.nameStyle.Render(t.title)
	}

	var bar string
	if t.displayProgress && !t.done {
		bar = " " + t.progress.ViewAs(t.percent)
	}
	var rate string
	if t.showRate() {
		rate = " " + defaultStyles.rateStyle.Render(formatRate(t.rate, t.rateUnit))
	}
	var eta string
	if rem, ok := t.remaining(); ok {
		eta = " " + defaultStyles.timeStyle.Render(fmt.Sprintf("ETA %s", formatDuration(rem)))
	}
	var elapsed string
	if t.displayElapsed {
		elapsed = " " + defaultStyles.timeStyle.Render(fmt.Sprintf("(%s)", formatDuration(t.elapsed())))
	}
	// Elapsed trails the other metrics; only the failure trailer follows it.
	line := prefix + defaultStyles.treeStyle.Render(connector) + status + " " + styledName + bar + rate + eta + elapsed

	if t.err != nil && !errors.Is(t.err, context.Canceled) {
		line += " " + defaultStyles.errorStyle.Render(t.err.Error())
	} else if t.err != nil && errors.Is(t.err, context.Canceled) {
		line += " " + defaultStyles.canceledStyle.Render("canceled")
	}

	var childPrefix string
	if last {
		childPrefix = prefix + "    "
	} else {
		childPrefix = prefix + defaultStyles.treeStyle.Render(defaultStyles.verticalConnector) + "   "
	}

	subtasks, hidden := visibleTasks(t.subtasks)
	if !hiddenSummary {
		hidden = 0
	}

	if !t.done || (t.err != nil && !errors.Is(t.err, context.Canceled)) {
		addPrefix := " "
		if len(subtasks) != 0 || hidden != 0 {
			addPrefix = defaultStyles.treeStyle.Render(defaultStyles.verticalConnector)
		}
		msgPrefix := childPrefix + addPrefix + " "
		msgWidth := width - lipgloss.Width(msgPrefix)
		renderedLines := 0
		const maxLines = 5
		for msg := range t.messages.lines() {
			wrapped := wrapLine(msg, msgWidth)
			for _, wl := range wrapped {
				if renderedLines >= maxLines {
					break
				}
				line += "\n" + msgPrefix + defaultStyles.msgStyle.Render(wl)
				renderedLines++
			}
			if renderedLines >= maxLines {
				break
			}
		}
	}

	line += "\n"

	if hidden != 0 {
		line += renderHiddenSummary(hidden, childPrefix, len(subtasks) == 0)
	}
	for i, sub := range subtasks {
		line += renderTask(sub, childPrefix, i == len(subtasks)-1, width, hiddenSummary)
	}

	return line
}

func (s *bubbleteaRendererState) View() tea.View {
	var out string
	if s.title != "" {
		out = defaultStyles.titleStyle.Render(s.title) + defaultStyles.timeStyle.Render(fmt.Sprintf(" (%s)", formatDuration(s.elapsed()))) + "\n"
	}
	tasks, hidden := visibleTasks(s.subtasks)
	if s.hiddenSummary && hidden != 0 {
		out += renderHiddenSummary(hidden, "", len(tasks) == 0)
	}
	for i, t := range tasks {
		out += renderTask(t, "", i == len(tasks)-1, s.width, s.hiddenSummary)
	}
	return tea.NewView(out)
}

var (
	// DefaultTitleStyle is the default lipgloss style for the top-level title.
	DefaultTitleStyle = lipgloss.NewStyle().Bold(true)
	// DefaultTreeStyle is the default lipgloss style for tree connectors.
	DefaultTreeStyle = lipgloss.NewStyle().Faint(true)
	// DefaultMsgStyle is the default lipgloss style for task log messages.
	DefaultMsgStyle = lipgloss.NewStyle().Faint(true)
	// DefaultNameStyle is the default lipgloss style for task names.
	DefaultNameStyle = lipgloss.NewStyle().Bold(true)
	// DefaultTimeStyle is the default lipgloss style for elapsed time.
	DefaultTimeStyle = lipgloss.NewStyle().Faint(true)
	// DefaultRateStyle is the default lipgloss style for the rate readout.
	DefaultRateStyle = lipgloss.NewStyle().Faint(true)
	// DefaultRunningStyle is the default lipgloss style for running task indicators.
	DefaultRunningStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#3f949e"))
	// DefaultDoneStyle is the default lipgloss style for completed task indicators.
	DefaultDoneStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#55b655"))
	// DefaultErrorStyle is the default lipgloss style for failed task indicators.
	DefaultErrorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#b62d2d"))
	// DefaultCanceledStyle is the default lipgloss style for canceled task indicators.
	DefaultCanceledStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#b96f3b"))

	// DefaultSpinnerOptions are the default spinner configuration options.
	DefaultSpinnerOptions = []spinner.Option{spinner.WithSpinner(spinner.MiniDot)}
	// DefaultProgressOptions are the default progress bar configuration options.
	DefaultProgressOptions = []progress.Option{
		progress.WithFillCharacters('#', '.'),
		progress.WithColors(lipgloss.Color("#2f70a5")),
	}

	// DefaultVerticalConnector is the default character for vertical tree lines.
	DefaultVerticalConnector = "│"
	// DefaultLastChildConnector is the default connector for the last child in a tree.
	DefaultLastChildConnector = "└──"
	// DefaultChildConnector is the default connector for non-last children in a tree.
	DefaultChildConnector = "├──"
	// DefaultErrorSymbol is the default symbol displayed for failed tasks.
	DefaultErrorSymbol = "⛌"
	// DefaultCanceledSymbol is the default symbol displayed for canceled tasks.
	DefaultCanceledSymbol = "⏹"
	// DefaultDoneSymbol is the default symbol displayed for completed tasks.
	DefaultDoneSymbol = "✓"
	// DefaultRunningSymbol is the default symbol displayed for running tasks.
	DefaultRunningSymbol = "●"
)

var defaultStyles = styleConfig{
	titleStyle:    DefaultTitleStyle,
	treeStyle:     DefaultTreeStyle,
	msgStyle:      DefaultMsgStyle,
	runningStyle:  DefaultRunningStyle,
	doneStyle:     DefaultDoneStyle,
	errorStyle:    DefaultErrorStyle,
	canceledStyle: DefaultCanceledStyle,
	nameStyle:     DefaultNameStyle,
	timeStyle:     DefaultTimeStyle,
	rateStyle:     DefaultRateStyle,

	spinnerOptions:  DefaultSpinnerOptions,
	progressOptions: DefaultProgressOptions,

	verticalConnector:  DefaultVerticalConnector,
	lastChildConnector: DefaultLastChildConnector,
	childConnector:     DefaultChildConnector,
	errorSymbol:        DefaultErrorSymbol,
	canceledSymbol:     DefaultCanceledSymbol,
	doneSymbol:         DefaultDoneSymbol,
	runningSymbol:      DefaultRunningSymbol,
}

// BubbleteaOpt is a functional option for configuring BubbleteaRenderer.
type BubbleteaOpt func(*bubbleteaRendererState)

// WithTitleStyle sets the lipgloss style for the top-level title.
func WithTitleStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.titleStyle = s
	}
}

// WithTreeStyle sets the lipgloss style for tree connectors.
func WithTreeStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.treeStyle = s
	}
}

// WithMsgStyle sets the lipgloss style for task log messages.
func WithMsgStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.msgStyle = s
	}
}

// WithRunningStyle sets the lipgloss style for running task indicators.
func WithRunningStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.runningStyle = s
	}
}

// WithDoneStyle sets the lipgloss style for completed task indicators.
func WithDoneStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.doneStyle = s
	}
}

// WithErrorStyle sets the lipgloss style for failed task indicators.
func WithErrorStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.errorStyle = s
	}
}

// WithCanceledStyle sets the lipgloss style for canceled task indicators.
func WithCanceledStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.canceledStyle = s
	}
}

// WithNameStyle sets the lipgloss style for task names.
func WithNameStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.nameStyle = s
	}
}

// WithTimeStyle sets the lipgloss style for elapsed time display.
func WithTimeStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.timeStyle = s
	}
}

// WithRateStyle sets the lipgloss style for the rate readout.
func WithRateStyle(s lipgloss.Style) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.rateStyle = s
	}
}

// WithSpinnerOptions sets the spinner configuration options for tasks.
func WithSpinnerOptions(opts ...spinner.Option) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.spinnerOptions = opts
	}
}

// WithProgressOptions sets the progress bar configuration options for tasks.
func WithProgressOptions(opts ...progress.Option) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.progressOptions = opts
	}
}

// WithTreeCharacters sets the characters used to draw the task tree.
func WithTreeCharacters(vertical, lastChild, child string) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.verticalConnector = vertical
		b.styles.lastChildConnector = lastChild
		b.styles.childConnector = child
	}
}

// WithSymbols sets the status symbols for running, done, error, and canceled tasks.
func WithSymbols(running, done, error, canceled string) BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.styles.runningSymbol = running
		b.styles.doneSymbol = done
		b.styles.errorSymbol = error
		b.styles.canceledSymbol = canceled
	}
}

// WithHiddenSummary draws a parent's hide-when-done children that stopped
// successfully as one "✓ N tasks done" line instead of dropping them silently.
func WithHiddenSummary() BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		b.hiddenSummary = true
	}
}

func WithAscii() BubbleteaOpt {
	return func(b *bubbleteaRendererState) {
		WithProgressOptions(
			progress.WithFillCharacters('#', '.'),
			progress.WithColors(lipgloss.Color("#2f70a5")),
		)(b)
		WithSpinnerOptions(spinner.WithSpinner(spinner.Line))(b)
		WithTreeCharacters("|", "`--", "|--")(b)
		WithSymbols("+", "*", "x", "~")(b)
	}
}

// BubbleteaRenderer returns a Renderer that displays an interactive terminal UI using Bubble Tea.
func BubbleteaRenderer(opts ...BubbleteaOpt) Renderer {
	return func(events <-chan any) {
		s := &bubbleteaRendererState{
			allTasks: make(map[int64]*task),
			styles:   &defaultStyles,
		}
		for _, opt := range opts {
			opt(s)
		}

		p := tea.NewProgram(s, tea.WithoutSignalHandler())

		doneChan := make(chan struct{})

		go func() {
			p.Run()
			close(doneChan)
		}()

		for e := range events {
			p.Send(e)
		}
		p.Send(closeEvent{})

		<-doneChan
	}
}
