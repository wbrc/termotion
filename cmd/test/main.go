package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"time"

	"github.com/wbrc/termotion"
)

type slowReader struct {
	r   io.Reader
	ctx context.Context
}

func (sr *slowReader) Read(p []byte) (n int, err error) {
	select {
	case <-sr.ctx.Done():
		return 0, sr.ctx.Err()
	default:
	}
	time.Sleep(50 * time.Millisecond)
	n, err = sr.r.Read(p)
	return n, err
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	pr, pw := io.Pipe()

	rendered := make(chan struct{})
	go func() {
		defer close(rendered)
		termotion.RenderStream(pr, termotion.BubbleteaRenderer())
		// termotion.RenderStream(pr, termotion.TextRenderer(os.Stdout))
	}()
	defer func() {
		<-rendered
	}()

	t := termotion.New(ctx, termotion.StreamRenderer(pw))

	t.Title("My Tasks")

	t.Subtask(func(t *termotion.Task) error {
		t.Progress(true)
		t.Title("load")
		const size = 1024 * 1024
		src := io.LimitReader(&slowReader{r: rand.NewChaCha8([32]byte{}), ctx: ctx}, size)
		_, err := t.Copy(io.Discard, src,
			termotion.Total(size),
			termotion.Percent(),
			termotion.Rate(),
			termotion.ETA(),
		)
		if err != nil {
			return err
		}

		t.Progress(false)
		t.Spinner(true)
		time.Sleep(1 * time.Second)

		t.Title("task 0")

		return nil
	})

	t.Title("process")

	t.Subtask(func(t *termotion.Task) error {
		t.Title("sub")
		t.Spinner(true)
		t.Go(func(t *termotion.Task) error {
			t.Title("some")
			t.Spinner(true)
			for i := range 20 {
				select {
				case <-t.Context().Done():
					return t.Context().Err()
				default:
				}
				fmt.Fprintf(t, "Hello line %d\n", i)
				time.Sleep(100 * time.Millisecond)
			}
			t.Spinner(false)
			t.Title("new title hehe")
			select {
			case <-t.Context().Done():
				return t.Context().Err()
			case <-time.After(1 * time.Second):
			}
			return nil
		})
		time.Sleep(100 * time.Millisecond)
		t.Go(func(t *termotion.Task) error {
			t.Title("some2")
			t.Spinner(true)
			fmt.Fprint(t, "some message\n")
			fmt.Fprint(t, "some message2\n")
			select {
			case <-t.Context().Done():
				return t.Context().Err()
			case <-time.After(4 * time.Second):
			}
			return errors.New("some error")
		})
		t.Go(func(t *termotion.Task) error {
			t.Title("some three")
			t.Subtask(doer)
			t.Subtask(doer)
			select {
			case <-t.Context().Done():
				return t.Context().Err()
			case <-time.After(1 * time.Second):
			}
			return nil
		})
		return nil
	})

	t.Subtask(func(t *termotion.Task) error {
		t.Title("stuff")
		for i := range 20 {
			select {
			case <-t.Context().Done():
				return t.Context().Err()
			default:
			}
			fmt.Fprintf(t, "World line %d\n", i)
			time.Sleep(150 * time.Millisecond)
		}
		return nil
	})
	t.Subtask(func(t *termotion.Task) error {
		t.Title("other stuff")
		select {
		case <-t.Context().Done():
			return t.Context().Err()
		case <-time.After(1 * time.Second):
		}
		return nil
	})
	t.Go(doer)

	err := t.Close()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func doer(t *termotion.Task) error {
	t.Title("gen")
	wait := time.Duration(rand.IntN(100))*time.Millisecond + 500*time.Millisecond
	for i := range rand.IntN(3) + 4 {
		select {
		case <-t.Context().Done():
			return t.Context().Err()
		default:
		}
		fmt.Fprintf(t, "Hello line %d\n", i)
		time.Sleep(wait)
	}
	return nil
}
