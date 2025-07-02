# R2R Folder Upload & Graph Creation Script

This script uploads all files from a specified folder to an R2R collection, extracts entities and relationships, and creates a knowledge graph using concurrent processing.

## Features

- **Concurrent Processing**: Uses multiple async workers for parallel file processing
- **Complete Pipeline**: Each worker handles upload → extraction → graph creation
- **Duplicate Handling**: Automatically skips files that already exist
- **Multiple File Formats**: Supports text, PDF, Office docs, images, audio, video, and more
- **Progress Tracking**: Real-time progress updates and detailed summaries
- **Error Handling**: Robust error handling with detailed logging

## Prerequisites

```bash
pip install r2r tqdm
```

## Configuration

Edit the configuration section at the top of `r2r_folder_upload.py`:

```python
FOLDER_PATH = "test"                          # Path to your folder
COLLECTION_NAME = "test-collection"           # Your collection name
R2R_BASE_URL = "http://fgcz-h-190:7272"     # R2R server URL
MAX_WORKERS = 3                              # Number of concurrent workers
```

## Usage

```bash
python r2r_folder_upload.py
```

## How It Works

1. **File Discovery**: Scans the specified folder for supported file types
2. **Collection Setup**: Creates the collection if it doesn't exist
3. **Concurrent Processing**: 3 workers process files in parallel:
   - Worker picks up next file from queue
   - Uploads file to R2R collection
   - Immediately extracts entities/relationships
   - Moves to next file
4. **Knowledge Graph**: Creates final knowledge graph from all extracted data

## Supported File Types

- **Text**: .txt, .md, .rtf
- **Documents**: .pdf, .docx, .doc, .pptx, .ppt, .xlsx, .xls
- **Data**: .json, .jsonl, .csv, .tsv, .xml
- **Media**: .png, .jpg, .jpeg, .gif, .mp3, .wav, .m4a, .mp4, .avi, .mov
- **Web**: .html, .htm

## Output

The script provides detailed progress information:

```
🚀 Starting R2R Folder Upload & Graph Creation Workflow
📁 Source folder: test
📚 Target collection: test-collection
🔄 Worker 1: Processing file1.txt
✅ Worker 1: Uploaded file1.txt -> document-id
🔍 Worker 1: Extracting entities from file1.txt
✅ Worker 1: Extracted from file1.txt: 5 entities, 3 relationships

📊 Final Summary:
✅ Successfully uploaded: 50 files
✅ Successfully extracted: 50 documents
📊 Total entities found: 250
🔗 Total relationships found: 150
🕸️ Knowledge graph: created
```

## Architecture

- **Async Workers**: Configurable number of concurrent workers (default: 3)
- **Queue-based**: Uses asyncio.Queue for task distribution
- **Thread-safe**: Results aggregated with asyncio.Lock
- **Pipeline Approach**: Each worker handles complete file lifecycle
- **Error Recovery**: Continues processing even if individual files fail

## Customization

- **Worker Count**: Set `MAX_WORKERS` in the configuration section (1-10 recommended)
- **File Types**: Add extensions to `SUPPORTED_EXTENSIONS`
- **R2R Settings**: Adjust API parameters in upload/extraction methods
- **Metadata**: Customize file metadata in the upload function

## Error Handling

- **Duplicates**: Automatically skipped (no deletion)
- **Failed Uploads**: Logged and reported in summary
- **Extraction Errors**: Tracked separately from upload success
- **API Errors**: Graceful fallbacks for different R2R response formats