package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	CloudFrontDistribution string `yaml:"cloudfront_distribution_id"`
	CloudFrontEnabled      bool   `yaml:"cloudfront_enabled"`
}

// RetryOperation represents a pending file operation
type RetryOperation struct {
	Type      string    `json:"type"` // "upload", "delete", or "invalidate"
	FilePath  string    `json:"file_path"`
	S3Key     string    `json:"s3_key"`
	Timestamp time.Time `json:"timestamp"`
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
	recentCreates := make(map[string]time.Time) // Track recent CREATE events

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

			// Track CREATE events
			if event.Op&fsnotify.Create == fsnotify.Create {
				recentCreates[event.Name] = time.Now()
			}

			// For WRITE events, check if there was a recent CREATE
			if event.Op&fsnotify.Write == fsnotify.Write {
				if createTime, exists := recentCreates[event.Name]; exists {
					if time.Since(createTime) < 1*time.Second {
						log.Printf("Ignoring WRITE event for recently created file: %s", event.Name)
						continue
					}
					delete(recentCreates, event.Name)
				}
			}

			// Clean up old CREATE tracking
			cutoff := time.Now().Add(-5 * time.Second)
			for path, createTime := range recentCreates {
				if createTime.Before(cutoff) {
					delete(recentCreates, path)
				}
			}

			// Debounce rapid successive events on the same file
			if timer, exists := debounce[event.Name]; exists {
				timer.Stop()
			}

			debounce[event.Name] = time.AfterFunc(500*time.Millisecond, func() {
				fs.handleFileEvent(event)
				delete(debounce, event.Name)
			})

		case err, ok := <-fs.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("Watcher error: %v", err)
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

		// Mark file as recently created
		fs.recentlyCreated[event.Name] = time.Now()

		// Clean up old entries (older than 5 seconds)
		cutoff := time.Now().Add(-5 * time.Second)
		for path, createTime := range fs.recentlyCreated {
			if createTime.Before(cutoff) {
				delete(fs.recentlyCreated, path)
			}
		}

		// Upload new file to S3
		if err := fs.uploadFile(event.Name, s3Key); err != nil {
			log.Printf("Failed to upload %s: %v", event.Name, err)
			fs.addToRetryQueue("upload", event.Name, s3Key)
		} else {
			log.Printf("Successfully uploaded: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
			// DO NOT add invalidation for new files - they are not cached yet
		}

	case event.Op&fsnotify.Write == fsnotify.Write:
		// Handle file modification
		fileInfo, err := os.Stat(event.Name)
		if err != nil {
			log.Printf("Error getting file info for %s: %v", event.Name, err)
			return
		}

		if fileInfo.IsDir() {
			return // Ignore directory modifications
		}

		// Check if this is a recently created file
		if createTime, exists := fs.recentlyCreated[event.Name]; exists {
			// If file was created within the last 2 seconds, treat as new file
			if time.Since(createTime) < 2*time.Second {
				log.Printf("Skipping invalidation for recently created file: %s", event.Name)

				// Upload the file (in case CREATE didn't complete the upload)
				if err := fs.uploadFile(event.Name, s3Key); err != nil {
					log.Printf("Failed to upload recently created file %s: %v", event.Name, err)
					fs.addToRetryQueue("upload", event.Name, s3Key)
				} else {
					log.Printf("Successfully uploaded recently created file: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
				}
				return
			}
			// Remove from recently created if it's older
			delete(fs.recentlyCreated, event.Name)
		}

		// This is a modification of an existing file - needs invalidation
		if err := fs.uploadFile(event.Name, s3Key); err != nil {
			log.Printf("Failed to upload modified file %s: %v", event.Name, err)
			fs.addToRetryQueue("upload", event.Name, s3Key)
		} else {
			log.Printf("Successfully updated: %s -> s3://%s/%s", event.Name, fs.config.S3Bucket, s3Key)
			// Add invalidation for modified files
			fs.addToInvalidationBatch("/" + s3Key)
		}

	case event.Op&fsnotify.Remove == fsnotify.Remove:
		// Remove from recently created if it exists
		delete(fs.recentlyCreated, event.Name)

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
		// Remove from recently created if it exists
		delete(fs.recentlyCreated, event.Name)

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

// uploadFile uploads a file to S3
func (fs *FileSync) uploadFile(filePath, s3Key string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = fs.s3Client.PutObject(fs.ctx, &s3.PutObjectInput{
		Bucket: aws.String(fs.config.S3Bucket),
		Key:    aws.String(s3Key),
	})

	return err
}

// deleteFromS3 deletes an object from S3
func (fs *FileSync) deleteFromS3(s3Key string) error {
	_, err := fs.s3Client.DeleteObject(fs.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.config.S3Bucket),
		Key:    aws.String(s3Key),
	})

	return err
}

// addToInvalidationBatch adds a path to the invalidation batch
func (fs *FileSync) addToInvalidationBatch(path string) {
	if !fs.config.CloudFrontEnabled {
		return
	}

	fs.invalidationBatch.paths = append(fs.invalidationBatch.paths, path)
	log.Printf("Added to invalidation batch: %s", path)
}

// invalidationBatchProcessor processes invalidation batches every 30 seconds
func (fs *FileSync) invalidationBatchProcessor() {
	ticker := time.NewTicker(30 * time.Second)
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

	paths := make([]string, len(fs.invalidationBatch.paths))
	copy(paths, fs.invalidationBatch.paths)

	// Clear the batch
	fs.invalidationBatch.paths = fs.invalidationBatch.paths[:0]
	fs.invalidationBatch.timestamp = time.Now()

	if err := fs.createInvalidation(paths); err != nil {
		log.Printf("Failed to create CloudFront invalidation: %v", err)
		// Add to retry queue
		for _, path := range paths {
			fs.addToRetryQueue("invalidate", path, strings.TrimPrefix(path, "/"))
		}
	} else {
		log.Printf("Successfully created CloudFront invalidation for %d paths", len(paths))
	}
}

// createInvalidation creates a CloudFront invalidation
func (fs *FileSync) createInvalidation(paths []string) error {
	if !fs.config.CloudFrontEnabled || fs.cloudFrontClient == nil {
		return nil
	}

	// CloudFront has a limit of 3000 paths per invalidation
	const maxPathsPerInvalidation = 3000

	for i := 0; i < len(paths); i += maxPathsPerInvalidation {
		end := i + maxPathsPerInvalidation
		if end > len(paths) {
			end = len(paths)
		}

		batch := paths[i:end]
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
		Type:      opType,
		FilePath:  filePath,
		S3Key:     s3Key,
		Timestamp: time.Now(),
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
		var err error
		var needsInvalidation bool

		switch operation.Type {
		case "upload":
			// Check if file still exists before trying to upload
			if _, statErr := os.Stat(operation.FilePath); statErr == nil {
				err = fs.uploadFile(operation.FilePath, operation.S3Key)
				if err == nil {
					log.Printf("Successfully uploaded (retry): %s -> s3://%s/%s",
						operation.FilePath, fs.config.S3Bucket, operation.S3Key)

					// For retry operations, assume this is an update (not a new file)
					// since new files rarely fail on first attempt
					needsInvalidation = true
				}
			} else {
				// File no longer exists, remove from queue
				log.Printf("File no longer exists, removing from retry queue: %s", operation.FilePath)
				continue
			}

		case "delete":
			err = fs.deleteFromS3(operation.S3Key)
			if err == nil {
				log.Printf("Successfully deleted (retry): s3://%s/%s", fs.config.S3Bucket, operation.S3Key)
				needsInvalidation = true
			}

		case "invalidate":
			err = fs.createInvalidation([]string{operation.FilePath})
			if err == nil {
				log.Printf("Successfully invalidated (retry): %s", operation.FilePath)
			}
		}

		if err != nil {
			log.Printf("Retry failed for %s %s: %v", operation.Type, operation.FilePath, err)
			remaining = append(remaining, operation)
		} else if needsInvalidation {
			// Add invalidation only for operations that require cache clearing
			fs.addToInvalidationBatch("/" + operation.S3Key)
		}
	}

	fs.retryQueue = remaining
	fs.saveRetryQueue()
}

// loadRetryQueue loads the retry queue from file
func (fs *FileSync) loadRetryQueue() error {
	data, err := os.ReadFile(fs.config.RetryFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist, start with empty queue
		}
		return err
	}

	return json.Unmarshal(data, &fs.retryQueue)
}

// saveRetryQueue saves the retry queue to file
func (fs *FileSync) saveRetryQueue() {
	data, err := json.MarshalIndent(fs.retryQueue, "", "  ")
	if err != nil {
		log.Printf("Error marshaling retry queue: %v", err)
		return
	}

	if err := os.WriteFile(fs.config.RetryFile, data, 0644); err != nil {
		log.Printf("Error saving retry queue: %v", err)
	}
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
