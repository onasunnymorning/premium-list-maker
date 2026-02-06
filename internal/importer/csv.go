package importer

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	dbpkg "premium-list-maker/internal/db"
	"premium-list-maker/internal/tagger"
)

// Type aliases to ensure types are accessible
type LabelData = dbpkg.LabelData
type TagAssociation = dbpkg.TagAssociation

// ImportStats tracks statistics for CSV import
type ImportStats struct {
	Imported       int // Total labels processed (new + existing)
	NewLabels      int // Newly inserted labels
	ExistingLabels int // Labels that already existed
	Skipped        int
	HeaderSkipped  bool
	Errors         []string
	StartTime      time.Time
	MaxMemoryMB    uint64
}

// ImportCSV imports labels from a CSV file into the database
// The CSV should have labels in the first column
// If autoTag is true, automatically adds length-based tags (len:N)
// If filenameTag is not empty, adds that tag to all imported labels
// Returns ImportStats with detailed statistics
// Uses optimized bulk inserts with pre-loaded data for maximum performance
func ImportCSV(db *dbpkg.DB, csvPath string, autoTag bool, filenameTag string) (*ImportStats, error) {
	stats := &ImportStats{
		StartTime: time.Now(),
		Errors:    make([]string, 0),
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Allow variable number of fields per record
	reader.FieldsPerRecord = -1
	// Reuse record to reduce allocations
	reader.ReuseRecord = true

	lineNum := 0
	heartbeatInterval := 100000
	batchSize := 10000       // Increased batch size for better performance
	lastHeartbeatCount := 0  // Track last heartbeat to avoid duplicate messages
	commitInterval := 100000 // Commit every 100K labels to reduce transaction size

	// Start single transaction for entire file
	tx, err := db.BeginTransaction()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Pre-load all existing label IDs into memory
	existingLabelMap, err := dbpkg.LoadAllLabelIDs(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing label IDs: %w", err)
	}

	// Pre-load all existing tag IDs into memory
	existingTagMap, err := dbpkg.LoadAllTagIDs(tx)
	if err != nil {
		return nil, fmt.Errorf("failed to load existing tag IDs: %w", err)
	}

	// Pre-create/load length tags (1-20) if auto-tagging
	tagCache := make(map[string]int64)
	if autoTag {
		for i := 1; i <= 20; i++ {
			lengthTag := tagger.GenerateLengthTag(i)
			if tagID, exists := existingTagMap[lengthTag]; exists {
				tagCache[lengthTag] = tagID
			} else {
				// Tag doesn't exist, create it
				tagID, err := dbpkg.GetOrCreateTagTx(tx, lengthTag)
				if err != nil {
					return nil, fmt.Errorf("failed to create tag %s: %w", lengthTag, err)
				}
				tagCache[lengthTag] = tagID
				existingTagMap[lengthTag] = tagID // Update map for future batches
			}
		}
	}

	// Pre-create/load filename tag if provided
	var filenameTagID int64
	if filenameTag != "" {
		if tagID, exists := existingTagMap[filenameTag]; exists {
			filenameTagID = tagID
		} else {
			// Tag doesn't exist, create it
			tagID, err := dbpkg.GetOrCreateTagTx(tx, filenameTag)
			if err != nil {
				return nil, fmt.Errorf("failed to create filename tag %s: %w", filenameTag, err)
			}
			filenameTagID = tagID
			existingTagMap[filenameTag] = tagID // Update map for future batches
		}
	}

	// Batch processing buffers
	batch := make([]LabelData, 0, batchSize)
	labelsProcessed := 0

	// Process batch function - optimized to use pre-loaded maps and single transaction
	processBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Bulk insert labels using pre-loaded existingLabelMap
		insertResult, err := db.BulkInsertLabels(tx, batch, existingLabelMap)
		if err != nil {
			return fmt.Errorf("failed to bulk insert labels: %w", err)
		}

		// Update existingLabelMap with newly inserted labels
		for label, id := range insertResult.LabelMap {
			existingLabelMap[label] = id
		}

		stats.NewLabels += insertResult.NewCount
		stats.ExistingLabels += insertResult.ExistingCount
		labelMap := insertResult.LabelMap

		// Prepare tag associations using pre-loaded tag IDs
		associations := make([]TagAssociation, 0, len(batch)*2) // Estimate: length tag + filename tag

		for _, l := range batch {
			labelID, ok := labelMap[l.Label]
			if !ok {
				// Should not happen, but skip if it does
				continue
			}

			// Add length tag if auto-tagging
			if autoTag {
				lengthTag := tagger.GenerateLengthTag(l.Length)
				tagID, ok := tagCache[lengthTag]
				if !ok {
					// Tag not in cache - should have been pre-loaded, but handle gracefully
					if tagIDFromMap, exists := existingTagMap[lengthTag]; exists {
						tagID = tagIDFromMap
						tagCache[lengthTag] = tagID
					} else {
						// Create tag if it doesn't exist (shouldn't happen for length 1-20)
						tagID, err = dbpkg.GetOrCreateTagTx(tx, lengthTag)
						if err != nil {
							return fmt.Errorf("failed to create tag %s: %w", lengthTag, err)
						}
						tagCache[lengthTag] = tagID
						existingTagMap[lengthTag] = tagID
					}
				}
				associations = append(associations, TagAssociation{
					LabelID: labelID,
					TagID:   tagID,
				})
			}

			// Add filename tag if provided
			if filenameTag != "" {
				associations = append(associations, TagAssociation{
					LabelID: labelID,
					TagID:   filenameTagID,
				})
			}
		}

		// Bulk insert tag associations
		if len(associations) > 0 {
			if err := db.BulkAddTagsToLabels(tx, associations); err != nil {
				return fmt.Errorf("failed to bulk add tags: %w", err)
			}
		}

		labelsProcessed += len(batch)
		stats.Imported += len(batch)

		// Commit transaction periodically to reduce transaction size
		if labelsProcessed >= commitInterval {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
			// Start new transaction
			tx, err = db.BeginTransaction()
			if err != nil {
				return fmt.Errorf("failed to begin new transaction: %w", err)
			}
			labelsProcessed = 0
		}

		batch = batch[:0] // Reset batch
		return nil
	}

	// Read and process CSV
	for {
		record, err := reader.Read()
		if err != nil {
			// Check if we've reached EOF
			if err == io.EOF {
				// Process remaining batch
				if err := processBatch(); err != nil {
					errorMsg := fmt.Sprintf("batch processing error: %v", err)
					stats.Errors = append(stats.Errors, errorMsg)
				}
				break
			}
			// For other errors, try to continue but log a warning
			errorMsg := fmt.Sprintf("line %d: %v", lineNum+1, err)
			stats.Errors = append(stats.Errors, errorMsg)
			lineNum++
			continue
		}

		lineNum++

		if len(record) == 0 {
			stats.Skipped++
			continue
		}

		label := strings.ToLower(strings.TrimSpace(record[0]))
		if label == "" {
			stats.Skipped++
			continue
		}

		// Check if this looks like a header row
		if !stats.HeaderSkipped && isHeaderRow(label) {
			stats.HeaderSkipped = true
			stats.Skipped++
			continue
		}

		// Validate label
		if err := ValidateLabel(label); err != nil {
			stats.Skipped++
			// Log the error but continue
			errorMsg := fmt.Sprintf("line %d: skipped invalid label '%s': %v", lineNum, label, err)
			stats.Errors = append(stats.Errors, errorMsg)
			continue
		}

		// Add to batch
		batch = append(batch, LabelData{
			Label:  label,
			Length: len(label),
		})

		// Process batch when it reaches batchSize
		if len(batch) >= batchSize {
			if err := processBatch(); err != nil {
				errorMsg := fmt.Sprintf("batch processing error: %v", err)
				stats.Errors = append(stats.Errors, errorMsg)
				// Continue processing despite error
			}

			// Heartbeat every 100K imports
			if stats.Imported > 0 && stats.Imported >= lastHeartbeatCount+heartbeatInterval {
				fmt.Printf("  [Heartbeat] Imported %d labels\n", stats.Imported)
				lastHeartbeatCount = (stats.Imported / heartbeatInterval) * heartbeatInterval
			}

			// Update max memory periodically
			if stats.Imported%10000 == 0 {
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				memMB := m.Alloc / 1024 / 1024
				if memMB > stats.MaxMemoryMB {
					stats.MaxMemoryMB = memMB
				}
			}
		}
	}

	// Commit final transaction
	if labelsProcessed > 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit final transaction: %w", err)
		}
	}

	// Final memory check
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memMB := m.Alloc / 1024 / 1024
	if memMB > stats.MaxMemoryMB {
		stats.MaxMemoryMB = memMB
	}

	return stats, nil
}

// CountCSVLines counts the total number of lines in a CSV file
func CountCSVLines(csvPath string) (int, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	count := 0
	for {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

// isHeaderRow checks if the first column value looks like a header
func isHeaderRow(firstCol string) bool {
	firstColLower := strings.ToLower(strings.TrimSpace(firstCol))

	// Common header patterns
	headerKeywords := []string{
		"label", "labels",
		"domain", "domains",
		"name", "names",
		"id", "identifier",
		"string", "strings",
		"sld", "second level domain",
		"domain name",
	}

	for _, keyword := range headerKeywords {
		if firstColLower == keyword {
			return true
		}
	}

	// Check if it's all uppercase (common header style)
	if firstCol == strings.ToUpper(firstCol) && len(firstCol) > 1 {
		// But exclude single letters/numbers which could be valid labels
		if len(firstCol) > 2 {
			return true
		}
	}

	return false
}
