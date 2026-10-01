// Command webtest serves the termotion web renderer and runs a sample workload
// so you can watch it live in a browser at http://localhost:8080.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"time"

	_ "embed"

	"github.com/wbrc/termotion"
)

//go:embed index.html
var indexHTML []byte

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
	time.Sleep(100 * time.Millisecond)
	return sr.r.Read(p)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	web := termotion.NewWebRenderer()

	mux := http.NewServeMux()
	mux.Handle("/termotion/ws", web)
	// The web renderer no longer serves its own JS/CSS — delivering them is the
	// caller's job. Here we serve the repository's web/ directory from disk; a
	// real app would copy the two files into its own static assets or pin them
	// from a CDN against a matching release tag.
	mux.Handle("/termotion/", http.StripPrefix("/termotion/", http.FileServer(http.Dir("web"))))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
		}
	}()
	log.Println("open http://localhost:8080")

	time.Sleep(5 * time.Second)

	t := termotion.New(ctx, web.Render)
	runWorkload(ctx, t)
	if err := t.Close(); err != nil {
		log.Printf("workload finished with error: %v", err)
	}

	log.Println("workload done — server still serving final state, Ctrl+C to quit")
	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
}

func runWorkload(ctx context.Context, t *termotion.Termotion) {
	t.Title("My Tasks")

	for i := range 10 {
		t.Subtask(func(t *termotion.Task) error {
			t.Title(fmt.Sprintf("task %d", i))
			t.HideWhenDone(true)
			time.Sleep(time.Second)
			return nil
		})
	}

	t.Subtask(func(t *termotion.Task) error {
		t.Title("load")
		t.Progress(true)

		const total = 1024 * 1024
		src := io.LimitReader(&slowReader{r: rand.NewChaCha8([32]byte{}), ctx: ctx}, total)
		buf := make([]byte, 32*1024)
		var read int64
		startedAt := time.Now()
		for {
			n, err := src.Read(buf)
			read += int64(n)
			t.SetPercent(float64(read) / total)
			// Re-declared every chunk: renderers never derive a rate, so it only
			// looks live as often as the producer says so.
			t.Rate(float64(read)/1024/time.Since(startedAt).Seconds(), "kB/s")
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
		}

		t.Rate(0, "")
		t.Progress(false)
		t.Spinner(true)
		time.Sleep(500 * time.Millisecond)
		t.Title("loaded")
		return nil
	})

	t.Title("process")

	t.Subtask(func(t *termotion.Task) error {
		t.Title("sub")
		t.Spinner(true)
		t.Go(func(t *termotion.Task) error {
			t.Title("emit")
			t.Spinner(true)
			startedAt := time.Now()
			for i := range 20 {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				fmt.Fprintf(t, "Hello line %d\n", i)
				t.Rate(float64(i+1)/time.Since(startedAt).Minutes(), "lines/min")
				time.Sleep(100 * time.Millisecond)
			}
			t.Spinner(false)
			t.Title("emitted")
			return nil
		})
		t.Go(func(t *termotion.Task) error {
			t.Title("will fail")
			t.Spinner(true)
			select {
			case <-t.Context().Done():
				return t.Context().Err()
			case <-time.After(3 * time.Second):
			}
			return errors.New("something went wrong")
		})
		t.Go(func(t *termotion.Task) error {
			t.Title("nested")
			t.Subtask(doer)
			t.Subtask(doer)
			return nil
		})
		return nil
	})

	t.Subtask(func(t *termotion.Task) error {
		t.Title("stream")
		for i := range 15 {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Fprintf(t, "World line %d\n", i)
			time.Sleep(150 * time.Millisecond)
		}
		return nil
	})
}

func doer(t *termotion.Task) error {
	t.Title("gen")
	t.Spinner(true)
	wait := time.Duration(rand.IntN(100))*time.Millisecond + 400*time.Millisecond
	for i := range rand.IntN(3) + 3 {
		if t.Context().Err() != nil {
			return t.Context().Err()
		}
		fmt.Fprintf(t, "item %d\n", i)
		time.Sleep(wait)
	}
	t.Spinner(false)
	return nil
}
