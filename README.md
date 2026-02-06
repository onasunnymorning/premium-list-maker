# Premium List Maker

A Go CLI application for generating premium domain lists for top-level domain registries. The tool manages domain labels in SQLite, supports tagging, and generates premium pricing lists by matching labels to price tiers.

## Features

- **CSV Import**: Import domain labels from CSV files
- **Excel Split**: Split Excel files into CSV files (one per sheet)
- **Tag Management**: Add multiple tags to labels for categorization
- **Auto-tagging**: Automatically generate length-based tags (optional)
- **Premium List Generation**: Generate premium lists by matching labels to tiers
- **Conflict Resolution**: Automatically applies the highest tier when labels match multiple tiers

## Installation

### macOS (via Homebrew)

You can install the tool using our custom Homebrew tap:

```bash
brew install onasunnymorning/tap/premium-list-maker
```

Or manually:

```bash
brew tap onasunnymorning/tap
brew install premium-list-maker
```

### Manual Installation (Build from Source)

```bash
go build -o premium-list-maker ./cmd/premium-list-maker
```

## Usage

### Import Labels from CSV

Import labels from all CSV files in a folder. The first column should contain the domain label (other columns are ignored).

**Import Behavior:**
- **Normalization**: All labels are automatically converted to lowercase.
- **Validation**: Labels are validated before import. Invalid labels are skipped and logged to an error report file.
  - Must be 1-63 characters long
  - Must use only allowed characters (a-z, 0-9, -)
  - Cannot start or end with a hyphen
  - Cannot contain consecutive hyphens (e.g., `ab--cd`) unless it's a valid IDN (starts with `xn--`)
  - Valid IDNs are supported
- **Deduplication**: Duplicate labels in the same file are ignored (first one wins).
- **Tagging**: 
  - Adds length-based tags (len:N) for each label
  - Adds a tag based on the filename (e.g., "1 digit" from "1 digit.csv")

**Error Reporting:**
If any invalid labels are encountered, a full error report is generated in the format `import_errors_YYYYMMDD_HHMMSS.txt`.

```bash
# Import all CSV files from a folder
premium-list-maker import /path/to/folder
```

Example: If you have files like `1 digit.csv`, `2 letter.csv`, `3 letter words.csv` in a folder:
- Labels from `1 digit.csv` will get tags: `len:1` and `1 digit`
- Labels from `2 letter.csv` will get tags: `len:2` and `2 letter`
- Labels from `3 letter words.csv` will get tags: `len:3` and `3 letter words`

Example CSV format:
```csv
STRING,SOURCE,CATEGORY
example,source1,category1
test,source2,category2
domain,source3,category3
```

### Add Tags to Labels

Add one or more tags to a label. Tags are created automatically if they don't exist.

```bash
# Add a single tag
premium-list-maker tag example "dictionary words"

# Add multiple tags
premium-list-maker tag example "dictionary words" "top 5k ES" "Cities 250k+"
```

### Split Excel File into CSV Files

Split an Excel (.xlsx) file into separate CSV files, one for each sheet. Only sheets where the first column appears to contain domain labels are processed.

```bash
premium-list-maker split-xlsx labels.xlsx output-directory/
```

**Behavior:**
- Processes each sheet in the Excel file
- Validates that the first column contains domain labels (alphanumeric, hyphens, dots)
- Skips sheets that don't match the expected format
- Outputs one CSV file per valid sheet (named after the sheet)
- Reports a summary of processed and skipped sheets

**Example Output:**
```
Processed sheet 'Sheet1' -> output-directory/Sheet1.csv
Processed sheet 'Cities' -> output-directory/Cities.csv

Summary:
  Processed: 2 sheet(s)
    - Sheet1
    - Cities
  Skipped: 1 sheet(s)
    - Summary (first column doesn't appear to contain domain labels)
```

### Generate Premium List

Generate a premium list CSV by matching labels to tiers defined in a JSON file.

```bash
premium-list-maker generate tiers.json output.csv
```

The tiers.json file should follow this format:

```json
[
  {
    "tier": 10,
    "tags": ["len:1", "1 digit", "2 letter"],
    "currency": "USD",
    "price_res": 100
  },
  {
    "tier": 9,
    "tags": ["2 AlphaNum", "3 letter"],
    "price_reg": 5000,
    "price_ren": 5000,
    "currency": "USD",
    "price_res": 100
  }
]
```

**Tier Matching Logic:**
- A label matches a tier if it has at least one tag in common with the tier's tags
- If a label matches multiple tiers, the highest tier number is selected
- Labels that don't match any tier are excluded from the output

**Output Format:**
The generated CSV contains the following columns:
- `Label`: The domain label
- `Tier`: The tier number assigned
- `price_reg`: Registration price (if specified)
- `price_ren`: Renewal price (if specified)
- `price_res`: Reservation price (if specified)
- `currency`: Currency code

### Database Path

By default, the tool uses `premium.db` in the current directory. You can specify a different path:

```bash
premium-list-maker --db /path/to/database.db import labels.csv
```

## Workflow

1. **Preparation Stage:**
   ```bash
   # Import all CSV files from a folder (automatically adds length tags and filename-based tags)
   premium-list-maker import /path/to/csv/folder
   
   # Manually add additional tags if needed
   premium-list-maker tag example "dictionary words"
   premium-list-maker tag test "Cities 250k+"
   ```

2. **Generation Stage:**
   ```bash
   # Generate premium list
   premium-list-maker generate tiers.json premium-list.csv
   ```

## Database Schema

The application uses SQLite with the following schema:

- **labels**: Stores domain labels with their length
- **tags**: Stores tag names
- **label_tags**: Junction table linking labels to tags (many-to-many relationship)

## Future Enhancements

- REST API endpoints for programmatic access
- MCP (Model Context Protocol) server endpoints
- Bulk tagging operations
- Query and filter commands
- Statistics and reporting features

## License

MIT

