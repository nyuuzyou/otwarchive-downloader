# OTW Archive Downloader

A high-performance, concurrent tool for downloading and parsing works from any website powered by the OTW-Archive software, including Archive of Our Own (AO3), SquidgeWorld, and other otwarchive instances.

## Features

- **Works with Multiple Sites**: Compatible with any otwarchive-powered website (AO3, SquidgeWorld, etc.)
- **Mass Download**: Efficiently download works using their IDs
- **Concurrent Processing**: Download multiple works simultaneously
- **Proxy Support**: Use SOCKS5 proxies to avoid rate limiting
- **Metadata Extraction**: Extract rich metadata from works (tags, ratings, warnings, etc.)
- **Content Preservation**: Save full text content along with metadata
- **Batch Processing**: Process works in configurable batches for easier management
- **JSONL Output**: Store works in JSON Lines format for easy data analysis
- **Robust Error Handling**: Smart retries, proxy rotation, and connection management

## Installation

```bash
# Clone the repository
git clone https://github.com/nyuuzyou/otwarchive-downloader.git
cd otwarchive-downloader

# Build the executable
go build
```

## Usage

```bash
./otwarchive-downloader [options]
```

### Command-line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--start-id` | Starting work ID | 1 |
| `--end-id` | Ending work ID | 100000 |
| `--batch-size` | Number of IDs per output file | 10000 |
| `--concurrent` | Maximum number of concurrent requests | 4 |
| `--retries` | Number of retry attempts per work | 5 |
| `--proxy-file` | File containing proxies | proxy.txt |
| `--output` | Directory for output files | output |
| `--use-proxies` | Use proxies for connections | false |
| `--timeout` | Timeout for HTTP requests | 60s |
| `--base-url` | Base URL for downloads | https://download.archiveofourown.org/downloads |
| `--user-agent` | User agent string for requests | Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 |

### Using with Different otwarchive Sites

To use with a different otwarchive-powered site, simply change the `-base-url` parameter:

```bash
# For SquidgeWorld
./otwarchive-downloader -base-url="https://squidgeworld.org/downloads"
```

### Proxy Configuration

If using proxies, create a `proxy.txt` file with one proxy per line in either of these formats:
```
host:port:username:password
host:port
```

Example:
```
127.0.0.1:9050:user:pass
proxy.example.com:1080
```

## Output Format

The tool outputs JSONL files (one JSON object per line) with the following structure:

```json
{
  "id": "12345",
  "title": "Example Work Title",
  "metadata": {
    "author": "ExampleAuthor",
    "Rating": "Explicit",
    "Archive Warning": "No Archive Warnings Apply",
    "Category": "F/M",
    "Fandom": "Example Fandom",
    "Relationship": "Character A/Character B",
    "Character": "Character A, Character B",
    "Additional Tags": "Tag A, Tag B",
    "published": "1970-01-01",
    "completed": "2038-01-19",
    "words": "12,345",
    "chapters": "3/3"
  },
  "text": "Full text content of the work..."
}
```

## Example

Download works with IDs from 1000 to 2000 using 8 concurrent connections:

```bash
./otwarchive-downloader -start-id=1000 -end-id=2000 -concurrent=8
```

Use proxies and target a specific otwarchive instance:

```bash
./otwarchive-downloader -start-id=1000 --end-id=10000 -use-proxies -concurrent=64 -base-url="https://download.your-archive-site.org/downloads"
```
