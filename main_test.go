package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The production config shape: explicit values survive, omitted ones get defaults.
func TestLoadConfig(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, `
watch_path: "/efs/src_cg_v2/uploads"
s3_bucket: "classygroundcovers-static"
s3_region: "us-east-2"
s3_prefix: "uploads"
retry_file: "/var/lib/file-sync-service/retry_queue.json"
retry_interval_seconds: 300
cloudfront_enabled: true
cloudfront_distribution_id: "E30K7UZU9CG8LG"
invalidation_interval_seconds: 120
wildcard_threshold: 5
exclude_dirs:
  - "*_files"
  - "*_data"
`))
	if err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"watch_path", cfg.WatchPath, "/efs/src_cg_v2/uploads"},
		{"s3_bucket", cfg.S3Bucket, "classygroundcovers-static"},
		{"s3_prefix", cfg.S3Prefix, "uploads"},
		{"cloudfront_enabled", cfg.CloudFrontEnabled, true},
		{"cloudfront_distribution_id", cfg.CloudFrontDistribution, "E30K7UZU9CG8LG"},
		{"invalidation_interval_seconds", cfg.InvalidationInterval, 120},
		{"wildcard_threshold", cfg.WildcardThreshold, 5},
		{"exclude_dirs", len(cfg.ExcludeDirs), 2},
		{"max_retry_attempts default", cfg.MaxRetryAttempts, 5},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(writeConfig(t, `watch_path: "/tmp/w"`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RetryFile != "retry_queue.json" || cfg.RetryInterval != 300 || cfg.MaxRetryAttempts != 5 ||
		cfg.InvalidationInterval != 60 || cfg.WildcardThreshold != 10 {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	if _, err := loadConfig(filepath.Join(t.TempDir(), "absent.yml")); err == nil {
		t.Fatal("want an error for a missing config file")
	}
}

func TestIsExcludedDir(t *testing.T) {
	fs := &FileSync{config: &Config{
		WatchPath:   "/efs/uploads",
		ExcludeDirs: []string{"*_files", "articles/_attic"},
	}}

	cases := []struct {
		path string
		want bool
	}{
		{"/efs/uploads/pcf_order_files", true},        // basename pattern, top level
		{"/efs/uploads/a/b/report_files", true},       // basename pattern, any depth
		{"/efs/uploads/articles/_attic", true},        // relative-path pattern
		{"/efs/uploads/other/articles/_attic", false}, // relative pattern is anchored at WatchPath
		{"/efs/uploads/images", false},
		{"/efs/uploads/files", false},
	}
	for _, c := range cases {
		if got := fs.isExcludedDir(c.path); got != c.want {
			t.Errorf("isExcludedDir(%q) = %v, want %v", c.path, got, c.want)
		}
	}

	none := &FileSync{config: &Config{WatchPath: "/efs/uploads"}}
	if none.isExcludedDir("/efs/uploads/pcf_order_files") {
		t.Error("no patterns must exclude nothing")
	}
}

// A standalone CHMOD must not reach the debouncer, or it replaces a pending CREATE/WRITE
// and the file is never uploaded (fixed in 6291d6a).
func TestShouldProcess(t *testing.T) {
	fs := &FileSync{config: &Config{}}

	cases := []struct {
		name  string
		event fsnotify.Event
		want  bool
	}{
		{"standalone chmod", fsnotify.Event{Name: "/u/a.jpg", Op: fsnotify.Chmod}, false},
		{"write with chmod", fsnotify.Event{Name: "/u/a.jpg", Op: fsnotify.Write | fsnotify.Chmod}, true},
		{"create", fsnotify.Event{Name: "/u/a.jpg", Op: fsnotify.Create}, true},
		{"write", fsnotify.Event{Name: "/u/a.jpg", Op: fsnotify.Write}, true},
		{"remove", fsnotify.Event{Name: "/u/a.jpg", Op: fsnotify.Remove}, true},
		{"hidden file", fsnotify.Event{Name: "/u/.a.jpg", Op: fsnotify.Create}, false},
		{"tmp suffix", fsnotify.Event{Name: "/u/a.jpg.tmp", Op: fsnotify.Write}, false},
		{"editor swap", fsnotify.Event{Name: "/u/a.swp", Op: fsnotify.Write}, false},
	}
	for _, c := range cases {
		if got := fs.shouldProcess(c.event); got != c.want {
			t.Errorf("%s: shouldProcess = %v, want %v", c.name, got, c.want)
		}
	}
}

// runEventLoop starts processEvents on a fake watcher and returns its event channel.
func runEventLoop(t *testing.T, delay time.Duration, handle func(fsnotify.Event, bool)) chan<- fsnotify.Event {
	t.Helper()
	events := make(chan fsnotify.Event)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fs := &FileSync{
		config:        &Config{},
		watcher:       &fsnotify.Watcher{Events: events, Errors: make(chan error)},
		ctx:           ctx,
		cancel:        cancel,
		debounceDelay: delay,
		handler:       handle,
	}
	go fs.processEvents()
	return events
}

// Timers fire while the loop keeps arming new ones. Without the lock -race reports the
// debounce maps; in production that is a fatal concurrent map write.
func TestProcessEventsConcurrentTimers(t *testing.T) {
	var mu sync.Mutex
	handled := map[string]int{}
	events := runEventLoop(t, time.Millisecond, func(e fsnotify.Event, _ bool) {
		mu.Lock()
		handled[e.Name]++
		mu.Unlock()
	})

	const files = 40
	for round := 0; round < 25; round++ {
		for i := 0; i < files; i++ {
			events <- fsnotify.Event{Name: fmt.Sprintf("/w/f%d.jpg", i), Op: fsnotify.Write}
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(handled) != files {
		t.Fatalf("handled %d distinct files, want %d", len(handled), files)
	}
}

// A burst on one path inside the debounce window is handled once, as its last event.
func TestProcessEventsDebounceBurst(t *testing.T) {
	got := make(chan fsnotify.Event, 4)
	events := runEventLoop(t, 50*time.Millisecond, func(e fsnotify.Event, _ bool) { got <- e })

	events <- fsnotify.Event{Name: "/w/a.jpg", Op: fsnotify.Create}
	events <- fsnotify.Event{Name: "/w/a.jpg", Op: fsnotify.Write}
	events <- fsnotify.Event{Name: "/w/a.jpg", Op: fsnotify.Write}

	select {
	case e := <-got:
		if e.Op != fsnotify.Write {
			t.Errorf("handled %s, want the last event (WRITE)", e.Op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the burst was never handled")
	}
	select {
	case e := <-got:
		t.Fatalf("the burst was handled twice; second event %s", e.Op)
	case <-time.After(200 * time.Millisecond):
	}
}

// Handlers append while the batcher drains: every path is delivered exactly once.
func TestInvalidationBatchConcurrent(t *testing.T) {
	fs := &FileSync{config: &Config{CloudFrontEnabled: true}, invalidationBatch: &InvalidationBatch{}}

	const writers, perWriter = 8, 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				fs.addToInvalidationBatch(fmt.Sprintf("/uploads/w%d/f%d.jpg", w, i))
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	seen := map[string]int{}
	drain := func() {
		for _, p := range fs.takeInvalidationBatch() {
			seen[p]++
		}
	}
	for running := true; running; {
		select {
		case <-done:
			running = false
		default:
			drain()
		}
	}
	drain()

	if len(seen) != writers*perWriter {
		t.Fatalf("delivered %d distinct paths, want %d", len(seen), writers*perWriter)
	}
	for p, n := range seen {
		if n != 1 {
			t.Fatalf("%s delivered %d times", p, n)
		}
	}
}

func TestRetryQueueConcurrentAppend(t *testing.T) {
	fs := &FileSync{config: &Config{RetryFile: filepath.Join(t.TempDir(), "retry.json")}}

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fs.addToRetryQueue("delete", fmt.Sprintf("/w/%d", i), fmt.Sprintf("uploads/%d", i))
		}()
	}
	wg.Wait()

	if got := len(fs.retryQueue); got != n {
		t.Fatalf("retry queue holds %d entries, want %d", got, n)
	}
}

// Entries a handler appends while a retry pass runs survive the end of that pass.
func TestRetryPassKeepsEntriesAddedDuringIt(t *testing.T) {
	fs := &FileSync{config: &Config{RetryFile: filepath.Join(t.TempDir(), "retry.json")}}
	fs.addToRetryQueue("delete", "/w/a", "uploads/a")
	fs.addToRetryQueue("delete", "/w/b", "uploads/b")

	queue, taken := fs.snapshotRetryQueue()
	fs.addToRetryQueue("delete", "/w/c", "uploads/c") // arrives mid-pass

	still := []RetryOperation{queue[1]} // a succeeded, b failed again
	if n := fs.finishRetryPass(still, taken); n != 2 {
		t.Fatalf("queue length %d after the pass, want 2", n)
	}
	if fs.retryQueue[0].S3Key != "uploads/b" || fs.retryQueue[1].S3Key != "uploads/c" {
		t.Fatalf("queue = %+v, want [b c]", fs.retryQueue)
	}
}
