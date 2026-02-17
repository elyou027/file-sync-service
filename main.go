package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v2"
)

// Config represents the application configuration
type Config struct {
	WatchPath              string `yaml:"watch_path"`
	S3Bucket               string `yaml:"s3_bucket"`
	S3Region               string `yaml:"s3_region"`
	S3Prefix               string `yaml:"s3_prefix"`
	RetryFile              string `yaml:"retry_file"`
	RetryInterval          int    `yaml:"retry_interval_seconds"`
	MaxRetryAttempts       int    `yaml:"max_retry_attempts"`
	CloudFrontDistribution string `yaml:"cloudfront_distribution_id"`
	CloudFrontEnabled      bool   `yaml:"cloudfront_enabled"`
	InvalidationInterval   int    `yaml:"invalidation_interval_seconds"`
	WildcardThreshold      int    `yaml:"wildcard_threshold"`
}

// RetryOperation represents a pending file operation
type RetryOperation struct {
	Type         string    `json:"type"` // "upload", "delete", or "invalidate"
	FilePath     string    `json:"file_path"`
	S3Key        string    `json:"s3_key"`
	Timestamp    time.Time `json:"timestamp"`
	AttemptCount int       `json:"attempt_count"`
}

// InvalidationBatch holds multiple paths for batch invalidation
type InvalidationBatch struct {
	paths     []string
	timestamp time.Time
}

// FileSync manages file synchronization to S3 and CloudFront invalidation
type FileSync struct {
	config            *Config
	s3Client          *s3.Client
	cloudFrontClient  *cloudfront.Client
	watcher           *fsnotify.Watcher
	retryQueue        []RetryOperation
	invalidationBatch *InvalidationBatch
	recentlyCreated   map[string]time.Time // Track recently created files
	ctx               context.Context
	cancel            context.CancelFunc
}

// NewFileSync creates a new FileSync instance
func NewFileSync(configPath string) (*FileSync, error) {
	// Load configuration
	cfg, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize AWS config
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.S3Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Initialize AWS clients
	s3Client := s3.NewFromConfig(awsCfg)
	var cloudFrontClient *cloudfront.Client
	if cfg.CloudFrontEnabled {
		cloudFrontClient = cloudfront.NewFromConfig(awsCfg)
	}

	// Initialize file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	fs := &FileSync{
		config:           cfg,
		s3Client:         s3Client,
		cloudFrontClient: cloudFrontClient,
		watcher:          watcher,
		ctx:              ctx,
		cancel:           cancel,
		invalidationBatch: &InvalidationBatch{
			paths:     make([]string, 0),
			timestamp: time.Now(),
		},
		recentlyCreated: make(map[string]time.Time), // Initialize map
	}

	// Load existing retry queue
	if err := fs.loadRetryQueue(); err != nil {
		log.Printf("Warning: failed to load retry queue: %v", err)
	}

	return fs, nil
}

// loadConfig loads configuration from YAML file
func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Set defaults
	if cfg.RetryFile == "" {
		cfg.RetryFile = "retry_queue.json"
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 300 // 5 minutes
	}
	if cfg.MaxRetryAttempts == 0 {
		cfg.MaxRetryAttempts = 5 // Default 5 attempts
	}
	if cfg.InvalidationInterval == 0 {
		cfg.InvalidationInterval = 60 // 60 seconds
	}
	if cfg.WildcardThreshold == 0 {
		cfg.WildcardThreshold = 10 // 10 files in one directory
	}

	return &cfg, nil
}

// Start begins file watching and synchronization
func (fs *FileSync) Start() error {
	log.Printf("Starting file synchronization service")
	log.Printf("Watching directory: %s", fs.config.WatchPath)
	log.Printf("S3 bucket: %s", fs.config.S3Bucket)
	if fs.config.CloudFrontEnabled {
		log.Printf("CloudFront distribution: %s", fs.config.CloudFrontDistribution)
	}

	// Add initial watch paths recursively
	if err := fs.addWatchRecursive(fs.config.WatchPath); err != nil {
		return fmt.Errorf("failed to add watch paths: %w", err)
	}

	// Start retry goroutine
	go fs.retryLoop()

	// Start invalidation batch processor
	if fs.config.CloudFrontEnabled {
		go fs.invalidationBatchProcessor()
	}

	// Start event processing
	go fs.processEvents()

	log.Printf("File synchronization service started successfully")
	return nil
}

// Stop gracefully stops the service
func (fs *FileSync) Stop() {
	log.Printf("Stopping file synchronization service...")
	fs.cancel()
	fs.watcher.Close()

	// Process any remaining invalidations
	if fs.config.CloudFrontEnabled && len(fs.invalidationBatch.paths) > 0 {
		fs.processInvalidationBatch()
	}

	fs.saveRetryQueue()
	log.Printf("Service stopped")
}

// addWatchRecursive adds watches to directory and all subdirectories
func (fs *FileSync) addWatchRecursive(root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if err := fs.watcher.Add(path); err != nil {
				return err
			}
			log.Printf("Added watch for directory: %s", path)
		}
		return nil
	})
}

// processEvents processes filesystem events
func (fs *FileSync) processEvents() {
	debounce := make(map[string]*time.Timer)
	newFiles := make(map[string]bool) // Track truly new files

	for {
		select {
		case <-fs.ctx.Done():
			return
		case event, ok := <-fs.watcher.Events:
			if !ok {
				return
			}

			// Skip temporary files
			if fs.isTemporaryFile(event.Name) {
				continue
			}

			log.Printf("File event: %s %s", event.Op, event.Name)

			// Track new files on CREATE
			if event.Op&fsnotify.Create == fsnotify.Create {
				if fileInfo, err := os.Stat(event.Name); err == nil && !fileInfo.IsDir() {
					newFiles[event.Name] = true
					log.Printf("Marking as new file: %s", event.Name)
				}
			}

			// Clean up tracking for removed files
			if event.Op&fsnotify.Remove == fsnotify.Remove {
				delete(newFiles, event.Name)
			}

			// Debounce rapid successive events on the same file
			if timer, exists := debounce[event.Name]; exists {
				timer.Stop()
			}

			debounce[event.Name] = time.AfterFunc(1*time.Second, func() { // Increased to 1 second
				fs.handleFileEventWithCheck(event, newFiles[event.Name])
				delete(debounce, event.Name)
				delete(newFiles, event.Name) // Clean up after processing
			})

		case err, ok := <-fs.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Watcher error: %v", err)
		}
	}
}

// handleFileEventWithCheck handles file events with new file detection
func (fs *FileSync) handleFileEventWithCheck(event fsnotify.Event, isNewFile bool) {
	relPath, err := filepath.Rel(fs.config.WatchPath, event.Name)
	if err != nil {
		log.Printf("Error getting relative path for %s: %v", event.Name, err)
		return
	}

	s3Key := filepath.Join(fs.config.S3Prefix, relPath)
	s3Key = strings.ReplaceAll(s3Key, "\\", "/")

	switch {
	case event.Op&fsnotify.Create == fsnotify.Create || event.Op&fsnotify.Write == fsnotify.Write:
		// Handle file creation or modification
		fileInfo, err := os.Stat(event.Name)
		if err != nil {
			log.Printf("Error getting file info for %s: %v", event.Name, err)
			return
		}

		if fileInfo.IsDir() {
			// Add watch to new directory
			if event.Op&fsnotify.Create == fsnotify.Create {
				if err := fs.watcher.Add(event.Name); err != nil {
					log.Printf("Error adding watch to new directory %s: %v", event.Name, err)
				} else {
					log.Printf("Added watch for new directory: %s", event.Name)
				}
			}
			return
		}

		// Check if file has content (avoid 0-byte uploads)
		if fileInfo.Size() == 0 {
			log.Printf("Skipping upload of empty file: %s", event.Name)
			return
		}

		// Upload file to S3
		if err := fs.uploadFile(event.Name, s3Key); err != nil {
			log.Printf("Failed to upload %s: %v", event.Name, err)
			fs.addToRetryQueue("upload", event.Name, s3Key)
		} else {
			if isNewFile {
				log.Printf("Successfully uploaded new file: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
				// NO INVALIDATION for new files
			} else {
				log.Printf("Successfully updated existing file: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
				// Add invalidation for existing files
				fs.addToInvalidationBatch("/" + s3Key)
			}
		}

	case event.Op&fsnotify.Remove == fsnotify.Remove:
		// Handle file deletion
		if err := fs.deleteFromS3(s3Key); err != nil {
			log.Printf("Failed to delete from S3 %s: %v", s3Key, err)
			fs.addToRetryQueue("delete", event.Name, s3Key)
		} else {
			log.Printf("Successfully deleted from S3: s3://%s/%s", fs.config.S3Bucket, s3Key)
			// Add invalidation for deleted files
			fs.addToInvalidationBatch("/" + s3Key)
		}

	case event.Op&fsnotify.Rename == fsnotify.Rename:
		// Handle rename as delete
		if err := fs.deleteFromS3(s3Key); err != nil {
			log.Printf("Failed to delete renamed file from S3 %s: %v", s3Key, err)
			fs.addToRetryQueue("delete", event.Name, s3Key)
		} else {
			log.Printf("Successfully deleted renamed file from S3: s3://%s/%s", fs.config.S3Bucket, s3Key)
			// Add invalidation for renamed files
			fs.addToInvalidationBatch("/" + s3Key)
		}
	}
}

// handleFileEvent handles a single file system event
func (fs *FileSync) handleFileEvent(event fsnotify.Event) {
	relPath, err := filepath.Rel(fs.config.WatchPath, event.Name)
	if err != nil {
		log.Printf("Error getting relative path for %s: %v", event.Name, err)
		return
	}

	s3Key := filepath.Join(fs.config.S3Prefix, relPath)
	s3Key = strings.ReplaceAll(s3Key, "\\", "/") // Ensure forward slashes for S3

	switch {
	case event.Op&fsnotify.Create == fsnotify.Create:
		// Handle file creation (NEW FILES)
		fileInfo, err := os.Stat(event.Name)
		if err != nil {
			log.Printf("Error getting file info for %s: %v", event.Name, err)
			return
		}

		if fileInfo.IsDir() {
			// Add watch to new directory
			if err := fs.watcher.Add(event.Name); err != nil {
				log.Printf("Error adding watch to new directory %s: %v", event.Name, err)
			} else {
				log.Printf("Added watch for new directory: %s", event.Name)
			}
			return
		}

		// Upload new file to S3
		if err := fs.uploadFile(event.Name, s3Key); err != nil {
			log.Printf("Failed to upload %s: %v", event.Name, err)
			fs.addToRetryQueue("upload", event.Name, s3Key)
		} else {
			log.Printf("Successfully uploaded NEW file: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
			// DO NOT add invalidation for new files - they are not cached yet
		}

	case event.Op&fsnotify.Write == fsnotify.Write:
		// Handle file modification (EXISTING FILES - invalidation required)
		fileInfo, err := os.Stat(event.Name)
		if err != nil {
			log.Printf("Error getting file info for %s: %v", event.Name, err)
			return
		}

		if fileInfo.IsDir() {
			return // Ignore directory modifications
		}

		// Upload modified file to S3
		if err := fs.uploadFile(event.Name, s3Key); err != nil {
			log.Printf("Failed to upload modified file %s: %v", event.Name, err)
			fs.addToRetryQueue("upload_with_invalidation", event.Name, s3Key) // Special retry type
		} else {
			log.Printf("Successfully updated EXISTING file: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
			// Add invalidation for modified files
			fs.addToInvalidationBatch("/" + s3Key)
		}

	case event.Op&fsnotify.Remove == fsnotify.Remove:
		// Handle file deletion (invalidation required)
		if err := fs.deleteFromS3(s3Key); err != nil {
			log.Printf("Failed to delete from S3 %s: %v", s3Key, err)
			fs.addToRetryQueue("delete", event.Name, s3Key)
		} else {
			log.Printf("Successfully deleted from S3: s3://%s/%s", fs.config.S3Bucket, s3Key)
			// Add invalidation for deleted files
			fs.addToInvalidationBatch("/" + s3Key)
		}

	case event.Op&fsnotify.Rename == fsnotify.Rename:
		// Handle rename as delete (we'll get a create event for the new name)
		if err := fs.deleteFromS3(s3Key); err != nil {
			log.Printf("Failed to delete renamed file from S3 %s: %v", s3Key, err)
			fs.addToRetryQueue("delete", event.Name, s3Key)
		} else {
			log.Printf("Successfully deleted renamed file from S3: s3://%s/%s", fs.config.S3Bucket, s3Key)
			// Add invalidation for renamed files (old name removal)
			fs.addToInvalidationBatch("/" + s3Key)
		}
	}
}

// uploadFile uploads a file to S3 with correct MIME type detection
func (fs *FileSync) uploadFile(filePath, s3Key string) error {
	// Wait for file to be fully written (check file size stability)
	var lastSize int64 = -1
	for i := 0; i < 10; i++ { // Maximum 10 attempts
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		currentSize := fileInfo.Size()

		// If file size is 0, wait longer
		if currentSize == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// If file size hasn't changed for 2 consecutive checks, assume it's ready
		if currentSize == lastSize {
			break
		}

		lastSize = currentSize
		time.Sleep(50 * time.Millisecond)
	}

	// Final check - ensure file is not empty
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	if fileInfo.Size() == 0 {
		return fmt.Errorf("file is empty or still being written: %s", filePath)
	}

	// Open a file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Detect MIME type by reading first 512 bytes
	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil && err.Error() != "EOF" {
		return err
	}

	// Reset file pointer to beginning
	file.Seek(0, 0)

	// Detect MIME type
	contentType := fs.detectContentType(buffer[:n], filePath)

	log.Printf("Uploading file %s (size: %d bytes, type: %s) to S3", filePath, fileInfo.Size(), contentType)

	_, err = fs.s3Client.PutObject(fs.ctx, &s3.PutObjectInput{
		Bucket:      aws.String(fs.config.S3Bucket),
		Key:         aws.String(s3Key),
		Body:        file,
		ContentType: aws.String(contentType),
	})

	return err
}

// detectContentType detects MIME type based on file content and extension
func (fs *FileSync) detectContentType(data []byte, filename string) string {
	// First, try to detect by file content
	contentType := http.DetectContentType(data)

	// If generic type detected, try to determine by file extension
	if contentType == "application/octet-stream" || contentType == "text/plain; charset=utf-8" {
		ext := strings.ToLower(filepath.Ext(filename))

		switch ext {
		// Images
		case ".jpg", ".jpeg":
			return "image/jpeg"
		case ".png":
			return "image/png"
		case ".gif":
			return "image/gif"
		case ".webp":
			return "image/webp"
		case ".svg":
			return "image/svg+xml"
		case ".bmp":
			return "image/bmp"
		case ".ico":
			return "image/x-icon"
		case ".tiff", ".tif":
			return "image/tiff"

		// Documents
		case ".pdf":
			return "application/pdf"
		case ".doc":
			return "application/msword"
		case ".docx":
			return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
		case ".xls":
			return "application/vnd.ms-excel"
		case ".xlsx":
			return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		case ".ppt":
			return "application/vnd.ms-powerpoint"
		case ".pptx":
			return "application/vnd.openxmlformats-officedocument.presentationml.presentation"

		// Text files
		case ".txt":
			return "text/plain"
		case ".html", ".htm":
			return "text/html"
		case ".css":
			return "text/css"
		case ".js":
			return "application/javascript"
		case ".json":
			return "application/json"
		case ".xml":
			return "application/xml"
		case ".csv":
			return "text/csv"

		// Video
		case ".mp4":
			return "video/mp4"
		case ".avi":
			return "video/x-msvideo"
		case ".mov":
			return "video/quicktime"
		case ".wmv":
			return "video/x-ms-wmv"
		case ".webm":
			return "video/webm"

		// Audio
		case ".mp3":
			return "audio/mpeg"
		case ".wav":
			return "audio/wav"
		case ".ogg":
			return "audio/ogg"
		case ".m4a":
			return "audio/mp4"

		// Archives
		case ".zip":
			return "application/zip"
		case ".rar":
			return "application/vnd.rar"
		case ".tar":
			return "application/x-tar"
		case ".gz":
			return "application/gzip"
		case ".7z":
			return "application/x-7z-compressed"

		default:
			return "application/octet-stream"
		}
	}

	return contentType
}

// deleteFromS3 deletes an object from S3
func (fs *FileSync) deleteFromS3(s3Key string) error {
	_, err := fs.s3Client.DeleteObject(fs.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.config.S3Bucket),
		Key:    aws.String(s3Key),
	})

	return err
}

// encodePathForCloudFront properly URL-encodes path for CloudFront invalidation
func (fs *FileSync) encodePathForCloudFront(path string) string {
	// Ensure path starts with /
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Parse and encode the URL path
	parsedURL, err := url.Parse(path)
	if err != nil {
		// If parsing fails, manually encode spaces and common problematic characters
		encoded := strings.ReplaceAll(path, " ", "%20")
		encoded = strings.ReplaceAll(encoded, "+", "%2B")
		encoded = strings.ReplaceAll(encoded, "&", "%26")
		encoded = strings.ReplaceAll(encoded, "#", "%23")
		encoded = strings.ReplaceAll(encoded, "?", "%3F")
		return encoded
	}

	return parsedURL.EscapedPath()
}

// addToInvalidationBatch adds a path to the invalidation batch
func (fs *FileSync) addToInvalidationBatch(path string) {
	if !fs.config.CloudFrontEnabled {
		return
	}

	// Properly encode the path for CloudFront
	encodedPath := fs.encodePathForCloudFront(path)

	fs.invalidationBatch.paths = append(fs.invalidationBatch.paths, encodedPath)
	log.Printf("Added to invalidation batch: %s (encoded: %s)", path, encodedPath)
}

// invalidationBatchProcessor processes invalidation batches based on configured interval
func (fs *FileSync) invalidationBatchProcessor() {
	interval := fs.config.InvalidationInterval
	if interval < 10 {
		interval = 10 // Minimum 10 seconds
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fs.ctx.Done():
			return
		case <-ticker.C:
			if len(fs.invalidationBatch.paths) > 0 {
				fs.processInvalidationBatch()
			}
		}
	}
}

// processInvalidationBatch creates a CloudFront invalidation for batched paths
func (fs *FileSync) processInvalidationBatch() {
	if len(fs.invalidationBatch.paths) == 0 {
		return
	}

	rawPaths := make([]string, len(fs.invalidationBatch.paths))
	copy(rawPaths, fs.invalidationBatch.paths)

	// Clear the batch
	fs.invalidationBatch.paths = fs.invalidationBatch.paths[:0]
	fs.invalidationBatch.timestamp = time.Now()

	// Optimize paths by using wildcards where appropriate
	optimizedPaths := fs.optimizeInvalidationPaths(rawPaths)

	if err := fs.createInvalidation(optimizedPaths); err != nil {
		log.Printf("Failed to create CloudFront invalidation: %v", err)
		// Add to retry queue (using original paths to be safe, or we could use optimized)
		for _, path := range optimizedPaths {
			fs.addToRetryQueue("invalidate", path, strings.TrimPrefix(path, "/"))
		}
	} else {
		log.Printf("Successfully created CloudFront invalidation for %d original paths (optimized to %d items)",
			len(rawPaths), len(optimizedPaths))
	}
}

// optimizeInvalidationPaths groups paths by directory and uses wildcard if threshold exceeded.
// It performs hierarchical grouping to find common prefixes.
func (fs *FileSync) optimizeInvalidationPaths(paths []string) []string {
	threshold := fs.config.WildcardThreshold
	if threshold <= 0 {
		return paths
	}

	// Remove duplicates
	uniquePathsMap := make(map[string]bool)
	for _, p := range paths {
		uniquePathsMap[p] = true
	}

	uniquePaths := make([]string, 0, len(uniquePathsMap))
	for p := range uniquePathsMap {
		uniquePaths = append(uniquePaths, p)
	}

	if len(uniquePaths) < threshold {
		return uniquePaths
	}

	type dirInfo struct {
		files []string
	}

	dirStats := make(map[string]*dirInfo)

	for _, p := range uniquePaths {
		curr := p
		for {
			curr = filepath.Dir(curr)
			if curr == "." || curr == "/" {
				// Avoid global root wildcard unless absolutely necessary, but track it
				d := "/"
				if dirStats[d] == nil {
					dirStats[d] = &dirInfo{}
				}
				dirStats[d].files = append(dirStats[d].files, p)
				break
			}

			d := curr
			if !strings.HasPrefix(d, "/") {
				d = "/" + d
			}

			if dirStats[d] == nil {
				dirStats[d] = &dirInfo{}
			}
			dirStats[d].files = append(dirStats[d].files, p)
		}
	}

	dirs := make([]string, 0, len(dirStats))
	for d := range dirStats {
		if d == "/" {
			continue // Root wildcard is usually too broad
		}
		dirs = append(dirs, d)
	}

	// Sort directories by length (deepest first)
	// This helps in finding the most specific wildcard that covers the threshold
	// Actually, we want to find the most broad wildcard that covers the threshold to save most money.
	// But if we go broad first, we might invalidate too much.
	// Let's go from broad to deep to maximize savings as requested.
	sort.Slice(dirs, func(i, j int) bool {
		return len(dirs[i]) < len(dirs[j])
	})

	coveredFiles := make(map[string]bool)
	var result []string

	for _, d := range dirs {
		countNotCovered := 0
		for _, f := range dirStats[d].files {
			if !coveredFiles[f] {
				countNotCovered++
			}
		}

		if countNotCovered >= threshold {
			wildcardPath := d
			if !strings.HasSuffix(wildcardPath, "/") {
				wildcardPath += "/"
			}
			wildcardPath += "*"
			result = append(result, wildcardPath)
			for _, f := range dirStats[d].files {
				coveredFiles[f] = true
			}
			log.Printf("Optimized %d paths in %s to wildcard %s", countNotCovered, d, wildcardPath)
		}
	}

	// Add files that didn't get covered by any wildcard
	for _, f := range uniquePaths {
		if !coveredFiles[f] {
			result = append(result, f)
		}
	}

	return result
}

// createInvalidation creates a CloudFront invalidation
func (fs *FileSync) createInvalidation(paths []string) error {
	if !fs.config.CloudFrontEnabled || fs.cloudFrontClient == nil {
		return nil
	}

	// Encode all paths properly
	var encodedPaths []string
	for _, path := range paths {
		encoded := fs.encodePathForCloudFront(path)
		encodedPaths = append(encodedPaths, encoded)
		log.Printf("Invalidating path: %s -> %s", path, encoded)
	}

	// CloudFront has a limit of 3000 paths per invalidation
	const maxPathsPerInvalidation = 3000

	for i := 0; i < len(encodedPaths); i += maxPathsPerInvalidation {
		end := i + maxPathsPerInvalidation
		if end > len(encodedPaths) {
			end = len(encodedPaths)
		}

		batch := encodedPaths[i:end]
		callerReference := fmt.Sprintf("file-sync-%d", time.Now().UnixNano())

		input := &cloudfront.CreateInvalidationInput{
			DistributionId: aws.String(fs.config.CloudFrontDistribution),
			InvalidationBatch: &types.InvalidationBatch{
				CallerReference: aws.String(callerReference),
				Paths: &types.Paths{
					Quantity: aws.Int32(int32(len(batch))),
					Items:    batch,
				},
			},
		}

		result, err := fs.cloudFrontClient.CreateInvalidation(fs.ctx, input)
		if err != nil {
			return fmt.Errorf("failed to create invalidation batch %d: %w", i/maxPathsPerInvalidation+1, err)
		}

		log.Printf("Created CloudFront invalidation %s for %d paths",
			*result.Invalidation.Id, len(batch))
	}

	return nil
}

// isTemporaryFile checks if a file should be ignored (temporary files)
func (fs *FileSync) isTemporaryFile(filename string) bool {
	base := filepath.Base(filename)
	return strings.HasPrefix(base, ".") ||
		strings.HasSuffix(base, ".tmp") ||
		strings.HasSuffix(base, ".swp") ||
		strings.HasSuffix(base, "~") ||
		strings.Contains(base, ".tmp.")
}

// addToRetryQueue adds a failed operation to the retry queue
func (fs *FileSync) addToRetryQueue(opType, filePath, s3Key string) {
	operation := RetryOperation{
		Type:         opType,
		FilePath:     filePath,
		S3Key:        s3Key,
		Timestamp:    time.Now(),
		AttemptCount: 1,
	}

	fs.retryQueue = append(fs.retryQueue, operation)
	fs.saveRetryQueue()
}

// retryLoop periodically processes the retry queue
func (fs *FileSync) retryLoop() {
	ticker := time.NewTicker(time.Duration(fs.config.RetryInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fs.ctx.Done():
			return
		case <-ticker.C:
			fs.processRetryQueue()
		}
	}
}

// processRetryQueue processes all items in the retry queue
func (fs *FileSync) processRetryQueue() {
	if len(fs.retryQueue) == 0 {
		return
	}

	log.Printf("Processing retry queue with %d items", len(fs.retryQueue))

	var remaining []RetryOperation

	for _, operation := range fs.retryQueue {
		// Check if max attempts reached
		if operation.AttemptCount > fs.config.MaxRetryAttempts {
			log.Printf("Max retry attempts (%d) exceeded for operation %s on %s, removing from queue",
				fs.config.MaxRetryAttempts, operation.Type, operation.FilePath)
			continue
		}

		var err error
		success := false

		switch operation.Type {
		case "upload":
			// Check if a file still exists before trying to upload
			if _, statErr := os.Stat(operation.FilePath); statErr == nil {
				err = fs.uploadFile(operation.FilePath, operation.S3Key)
				if err == nil {
					log.Printf("Successfully uploaded file (retry %d): %s -> s3://%s/%s",
						operation.AttemptCount, operation.FilePath, fs.config.S3Bucket, operation.S3Key)
					success = true
				}
			} else {
				// File no longer exists, remove from queue
				log.Printf("File no longer exists, removing from retry queue: %s", operation.FilePath)
				success = true // Don't retry
			}

		case "upload_with_invalidation":
			// Check if file still exists before trying to upload
			if _, statErr := os.Stat(operation.FilePath); statErr == nil {
				err = fs.uploadFile(operation.FilePath, operation.S3Key)
				if err == nil {
					log.Printf("Successfully uploaded file (retry %d): %s -> s3://%s/%s",
						operation.AttemptCount, operation.FilePath, fs.config.S3Bucket, operation.S3Key)
					// Add invalidation for modified files
					fs.addToInvalidationBatch("/" + operation.S3Key)
					success = true
				}
			} else {
				// File no longer exists, remove from queue
				log.Printf("File no longer exists, removing from retry queue: %s", operation.FilePath)
				success = true // Don't retry
			}

		case "delete":
			err = fs.deleteFromS3(operation.S3Key)
			if err == nil {
				log.Printf("Successfully deleted from S3 (retry %d): s3://%s/%s",
					operation.AttemptCount, fs.config.S3Bucket, operation.S3Key)
				success = true
			}

		case "invalidate":
			paths := []string{"/" + operation.S3Key}
			err = fs.createInvalidation(paths)
			if err == nil {
				log.Printf("Successfully created CloudFront invalidation (retry %d): %s",
					operation.AttemptCount, operation.S3Key)
				success = true
			}
		}

		if !success && err != nil {
			log.Printf("Retry %d failed for %s %s: %v",
				operation.AttemptCount, operation.Type, operation.FilePath, err)

			// Increment attempt count and add back to the queue
			operation.AttemptCount++
			operation.Timestamp = time.Now()
			remaining = append(remaining, operation)
		}
	}

	// Update retry queue with remaining operations
	fs.retryQueue = remaining
	fs.saveRetryQueue()

	if len(remaining) > 0 {
		log.Printf("Retry queue now contains %d items", len(remaining))
	} else {
		log.Printf("Retry queue is now empty")
	}
}

// loadRetryQueue loads the retry queue from a file
func (fs *FileSync) loadRetryQueue() error {
	if _, err := os.Stat(fs.config.RetryFile); os.IsNotExist(err) {
		return nil // No retry file exists yet
	}

	data, err := os.ReadFile(fs.config.RetryFile)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &fs.retryQueue)
}

// saveRetryQueue saves the retry queue to file
func (fs *FileSync) saveRetryQueue() error {
	if len(fs.retryQueue) == 0 {
		// Remove retry file if queue is empty
		os.Remove(fs.config.RetryFile)
		return nil
	}

	data, err := json.MarshalIndent(fs.retryQueue, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(fs.config.RetryFile, data, 0644)
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yml", "Path to configuration file")
	flag.Parse()

	// Create a FileSync instance
	fileSync, err := NewFileSync(configPath)
	if err != nil {
		log.Fatalf("Failed to initialize file sync: %v", err)
	}

	// Start the service
	if err := fileSync.Start(); err != nil {
		log.Fatalf("Failed to start service: %v", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	fileSync.Stop()
}
