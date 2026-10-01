package main

import (
	"os"
	"path/filepath"
	"testing"

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
