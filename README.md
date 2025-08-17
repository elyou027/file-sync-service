# File Synchronization Service

A Go service that monitors ext4 filesystem events (file creation, modification, deletion) in a specified directory and
synchronizes changes to AWS S3 bucket. Designed to run on Linux.

## Features

- **Real-time file monitoring**: Uses fsnotify to watch for filesystem events
- **AWS S3 synchronization**: Automatically uploads created/modified files and deletes removed files
- **Retry mechanism**: Persists failed operations locally for retry when S3 becomes available
- **Recursive directory watching**: Automatically watches new subdirectories
- **Temporary file filtering**: Ignores temporary files (.tmp, .swp, hidden files, etc.)
- **Event debouncing**: Prevents duplicate uploads during rapid file changes
- **Graceful shutdown**: Handles SIGINT/SIGTERM signals properly
- **Configurable via YAML**: Easy configuration management
- **Comprehensive logging**: Logs all upload/delete operations and errors

## Prerequisites

- Go 1.19 or later
- AWS credentials configured (via environment variables, IAM role, or AWS credentials file)
- S3 bucket with appropriate permissions
- Linux system with ext4 filesystem

## Installation

1. Clone or download the service:
```bash
git clone <repository-url>
cd file-sync-service
```

2. Build the service:
```bash
go build -o file-sync-service main.go
```

3. Make it executable:
```bash
chmod +x file-sync-service
```

## Configuration

Create a YAML configuration file (default: `config.yml`):

```yaml
# Directory to watch for file changes (absolute path recommended)
watch_path: "/efs/src_cg_v2/uploads"

# AWS S3 bucket name where files will be synchronized
s3_bucket: "your-s3-bucket-name"

# AWS region where your S3 bucket is located
s3_region: "us-east-1"

# S3 key prefix to prepend to all uploaded files
s3_prefix: "uploads"

# File path where retry queue will be stored
retry_file: "retry_queue.json"

# Retry interval in seconds (how often to retry failed operations)
retry_interval_seconds: 300

# CloudFront Configuration (optional)
# Enable CloudFront cache invalidation
cloudfront_enabled: true

# CloudFront Distribution ID (required if cloudfront_enabled is true)
cloudfront_distribution_id: "E1234567890ABC"
```

### Configuration Parameters

- `watch_path`: Local directory to monitor (use absolute paths)
- `s3_bucket`: Target S3 bucket name
- `s3_region`: AWS region of your S3 bucket
- `s3_prefix`: Optional prefix for S3 keys (can be empty)
- `retry_file`: Local file to store failed operations (default: "retry_queue.json")
- `retry_interval_seconds`: Retry interval in seconds (default: 300)

## AWS Credentials

The service uses AWS SDK v2 default credential chain:

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
2. **AWS credentials file**: `~/.aws/credentials`
3. **IAM role**: Recommended for EC2 instances

### Required S3 Permissions

Your AWS credentials need the following S3 permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::your-bucket-name/*"
        }
    ]
}
```

## Usage

### Basic Usage

Run with default config file (`config.yml`):
```bash
./file-sync-service
```

### Custom Config File

Run with custom configuration file:
```bash
./file-sync-service -config /path/to/your/config.yml
```

### Running as System Service

Create a systemd service file `/etc/systemd/system/file-sync.service`:

```ini
[Unit]
Description=File Synchronization Service
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/service
ExecStart=/path/to/service/file-sync-service -config /path/to/config.yml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl enable file-sync.service
sudo systemctl start file-sync.service
sudo systemctl status file-sync.service
```

## How It Works

1. **Initialization**: Service loads configuration and establishes AWS S3 connection
2. **Directory Watching**: Recursively adds filesystem watches to the target directory
3. **Event Processing**:
    - File creation/modification → Upload to S3
    - File deletion → Delete from S3
    - Directory creation → Add new watch
4. **Retry Mechanism**: Failed operations are saved to local JSON file and retried periodically
5. **Logging**: All operations are logged to stdout with timestamps

### File Mapping

Local file paths are mapped to S3 keys as follows:
- Local: `/watch/path/subfolder/file.txt`
- S3 Key: `{s3_prefix}/subfolder/file.txt`

### Ignored Files

The service automatically ignores:
- Hidden files (starting with `.`)
- Temporary files (`.tmp`, `.swp`, `~` suffix)
- Files containing `.tmp.` in the name

## Logging

The service logs to stdout:
- Service start/stop events
- Directory watch additions
- Successful file uploads/deletions
- Failed operations and retry attempts
- Error messages with details

Example log output:
```
2024/08/07 16:44:00 Starting file synchronization service
2024/08/07 16:44:00 Watching directory: /home/user/documents
2024/08/07 16:44:00 S3 bucket: my-sync-bucket
2024/08/07 16:44:00 Added watch for directory: /home/user/documents
2024/08/07 16:44:00 File synchronization service started successfully
2024/08/07 16:44:15 File event: CREATE /home/user/documents/test.txt
2024/08/07 16:44:15 Successfully uploaded: /home/user/documents/test.txt -> s3://my-sync-bucket/synced-files/test.txt
```

## Error Handling

- **S3 connectivity issues**: Operations are queued for retry
- **File permission errors**: Logged and skipped
- **Network timeouts**: Automatic retry with exponential backoff
- **Invalid configuration**: Service exits with error message

## Troubleshooting

1. **Service won't start**: Check configuration file syntax and AWS credentials
2. **Files not uploading**: Verify S3 bucket permissions and network connectivity
3. **High CPU usage**: Check for filesystem loops or excessive temporary file creation
4. **Missing files**: Check retry queue file for pending operations

## Performance Considerations

- The service uses efficient filesystem event notifications (not polling)
- Debouncing prevents duplicate uploads during rapid file changes
- Goroutines handle concurrent operations
- Memory usage scales with retry queue size

## Building for Production

For optimized production build:
```bash
CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o file-sync-service main.go
```

This creates a static binary that runs on any Linux system without dependencies