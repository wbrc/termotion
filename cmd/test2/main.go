package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
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
	task := termotion.New(context.Background(), termotion.BubbleteaRenderer(termotion.WithHiddenSummary()))
	task.Title("asdf")
	defer task.Close()

	task.Subtask(func(t *termotion.Task) error {
		t.HideWhenDone(true)
		t.Title("subtask")
		time.Sleep(1 * time.Second)
		fmt.Fprintf(t, "do stuff\n")
		time.Sleep(1 * time.Second)
		return nil
	})

	task.Subtask(func(t *termotion.Task) error {
		t.HideWhenDone(true)
		t.Title("another subtask")
		t.Go(func(t *termotion.Task) error {
			t.HideWhenDone(true)
			t.Title("parallel subtask 1")
			time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
			return nil
		})
		t.Go(func(t *termotion.Task) error {
			t.HideWhenDone(true)
			t.Title("parallel subtask 2")
			time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
			return nil
		})
		t.Go(func(t *termotion.Task) error {
			t.HideWhenDone(true)
			t.Title("parallel subtask 3")
			time.Sleep(time.Duration(rand.IntN(1000)) * time.Millisecond)
			return nil
		})
		t.Go(func(t *termotion.Task) error {
			t.HideWhenDone(true)
			t.Title("parallel subtask 4")
			time.Sleep(time.Duration(rand.IntN(1000))*time.Millisecond + 2*time.Second)
			return nil
		})

		t.Wait()

		fmt.Fprintf(t, "all parallel subtasks completed\n")
		time.Sleep(time.Second)

		return nil
	})

}
