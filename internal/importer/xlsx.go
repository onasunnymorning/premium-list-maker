package importer

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// TierConfig represents a tier definition for JSON output
type TierConfig struct {
	Tier int      `json:"tier"`
	Tags []string `json:"tags"`
}

// SplitXLSX splits an Excel file into CSV files, one per sheet
// Only processes sheets where the first column appears to contain domain labels
// If format is "andy", it further splits sheets by "Tier Level" column
// Returns a summary of processed and skipped sheets
func SplitXLSX(xlsxPath, outputDir, format string) error {
	// Open Excel file
	f, err := excelize.OpenFile(xlsxPath)
	if err != nil {
		return fmt.Errorf("failed to open Excel file: %w", err)
	}
	defer f.Close()

	// Get all sheet names
	sheetList := f.GetSheetList()
	if len(sheetList) == 0 {
		return fmt.Errorf("no sheets found in Excel file")
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	var processed []string
	var skipped []string
	foundTiers := make(map[int][]string)

	// Process each sheet
	for _, sheetName := range sheetList {
		rows, err := f.GetRows(sheetName)
		if err != nil {
			fmt.Printf("Warning: failed to read sheet '%s': %v\n", sheetName, err)
			skipped = append(skipped, fmt.Sprintf("%s (read error)", sheetName))
			continue
		}

		// Check if sheet has data and if first column looks like domain labels
		if !isValidLabelSheet(rows) {
			skipped = append(skipped, fmt.Sprintf("%s (first column doesn't appear to contain domain labels)", sheetName))
			continue
		}

		if format == "andy" {
			// Find Tier Level column (search in first row/header)
			tierColIdx := -1
			if len(rows) > 0 {
				for i, col := range rows[0] {
					if strings.EqualFold(strings.TrimSpace(col), "Tier Level") {
						tierColIdx = i
						break
					}
				}
				// Check column F (index 5) if header not found
				if tierColIdx == -1 && len(rows[0]) > 5 {
					tierColIdx = 5
				}
			}

			if tierColIdx == -1 {
				fmt.Printf("Warning: 'Tier Level' column not found in sheet '%s', using default split\n", sheetName)
			} else {
				tiersInSheet, err := splitSheetByTier(rows, sheetName, outputDir, tierColIdx)
				if err != nil {
					fmt.Printf("Warning: failed to split sheet '%s' by tier: %v\n", sheetName, err)
					skipped = append(skipped, fmt.Sprintf("%s (split error)", sheetName))
					continue
				}
				// Collect found tiers and tags
				for tier, filename := range tiersInSheet {
					tag := strings.TrimSuffix(filename, filepath.Ext(filename))
					foundTiers[tier] = append(foundTiers[tier], tag)
				}

				processed = append(processed, sheetName+" (split by tier)")
				continue
			}
		}

		// Default behavior

		// Generate output filename (sanitize sheet name)
		outputFile := sanitizeSheetName(sheetName) + ".csv"
		outputPath := filepath.Join(outputDir, outputFile)

		// Write sheet to CSV
		if err := writeSheetToCSV(rows, outputPath); err != nil {
			fmt.Printf("Warning: failed to write sheet '%s' to CSV: %v\n", sheetName, err)
			skipped = append(skipped, fmt.Sprintf("%s (write error)", sheetName))
			continue
		}

		processed = append(processed, sheetName)
		fmt.Printf("Processed sheet '%s' -> %s\n", sheetName, outputPath)
	}

	// Generate tiers JSON if in "andy" format and tiers were found
	if format == "andy" && len(foundTiers) > 0 {
		if err := generateTiersJSON(foundTiers, outputDir); err != nil {
			fmt.Printf("Warning: failed to generate tiers JSON: %v\n", err)
		} else {
			fmt.Printf("Generated tiers JSON file in %s\n", outputDir)
		}
	}

	// Print summary
	fmt.Println()
	fmt.Printf("Summary:\n")
	fmt.Printf("  Processed: %d sheet(s)\n", len(processed))
	if len(processed) > 0 {
		for _, name := range processed {
			fmt.Printf("    - %s\n", name)
		}
	}
	fmt.Printf("  Skipped: %d sheet(s)\n", len(skipped))
	if len(skipped) > 0 {
		for _, name := range skipped {
			fmt.Printf("    - %s\n", name)
		}
	}

	return nil
}

// splitSheetByTier splits rows into multiple CSVs based on tier column
// Returns map of tier numbers to filenames created
func splitSheetByTier(rows [][]string, sheetName, outputDir string, tierColIdx int) (map[int]string, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	header := rows[0]
	dataRows := rows[1:]

	tiers := make(map[int][][]string)
	nullTierRows := [][]string{}
	minTier := 100 // high starting value

	for _, row := range dataRows {
		if len(row) <= tierColIdx {
			nullTierRows = append(nullTierRows, row)
			continue
		}

		val := strings.TrimSpace(row[tierColIdx])
		if val == "" || val == "0" || strings.EqualFold(val, "null") {
			nullTierRows = append(nullTierRows, row)
			continue
		}

		tier, err := strconv.Atoi(val)
		if err != nil {
			// If not a number, treat as null/0?
			nullTierRows = append(nullTierRows, row)
			continue
		}

		if tier < minTier {
			minTier = tier
		}
		tiers[tier] = append(tiers[tier], row)
	}

	// If no numeric tiers found, default minTier to 1 (based on user feedback)
	if minTier == 100 {
		minTier = 1
	}

	// Assign null tier rows to minTier
	if len(nullTierRows) > 0 {
		tiers[minTier] = append(tiers[minTier], nullTierRows...)
	}

	foundTiers := make(map[int]string)

	// Write files
	for tier, tierRows := range tiers {
		outputFilename := fmt.Sprintf("%s - tier %d.csv", sanitizeSheetName(sheetName), tier)
		outputPath := filepath.Join(outputDir, outputFilename)

		// Create CSV with header
		allRows := append([][]string{header}, tierRows...)
		if err := writeSheetToCSV(allRows, outputPath); err != nil {
			return nil, err
		}
		fmt.Printf("  -> Created %s (%d rows)\n", outputFilename, len(tierRows))
		foundTiers[tier] = outputFilename
	}

	return foundTiers, nil

}

// generateTiersJSON generates a tiers-<date>.json file with found tiers
func generateTiersJSON(foundTiers map[int][]string, outputDir string) error {
	var tierConfigs []TierConfig
	for tier, tags := range foundTiers {
		// Dedup tags just in case
		uniqueTags := make(map[string]bool)
		var cleanTags []string
		for _, tag := range tags {
			if !uniqueTags[tag] {
				uniqueTags[tag] = true
				cleanTags = append(cleanTags, tag)
			}
		}
		sort.Strings(cleanTags)

		tierConfigs = append(tierConfigs, TierConfig{
			Tier: tier,
			Tags: cleanTags,
		})
	}

	// Sort tiers descending
	sort.Slice(tierConfigs, func(i, j int) bool {
		return tierConfigs[i].Tier > tierConfigs[j].Tier
	})

	// JSON filename with date and time
	filename := fmt.Sprintf("tiers-%s.json", time.Now().Format("20060102-150405"))
	outputPath := filepath.Join(outputDir, filename)

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create tiers JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(tierConfigs); err != nil {
		return fmt.Errorf("failed to encode tiers JSON: %w", err)
	}

	return nil
}

// isValidLabelSheet checks if the sheet appears to have domain labels in the first column
// A sheet is considered valid if:
// - It has at least one data row (beyond potential header)
// - The first column contains values that look like domain labels (alphanumeric, hyphens, dots)
func isValidLabelSheet(rows [][]string) bool {
	if len(rows) == 0 {
		return false
	}

	// Check if we have at least one data row
	// Skip potential header row
	startRow := 0
	if len(rows) > 0 && len(rows[0]) > 0 {
		firstCol := strings.TrimSpace(rows[0][0])
		if isHeaderRow(firstCol) {
			startRow = 1
		}
	}

	if startRow >= len(rows) {
		return false
	}

	// Check if we have at least one valid label in the first column
	validCount := 0
	labelPattern := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9.-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$`)

	for i := startRow; i < len(rows) && i < startRow+10; i++ { // Check up to 10 rows
		if len(rows[i]) == 0 {
			continue
		}
		firstCol := strings.TrimSpace(rows[i][0])
		if firstCol == "" {
			continue
		}
		// Check if it looks like a domain label
		if labelPattern.MatchString(firstCol) {
			validCount++
		}
	}

	// Consider valid if at least one row looks like a domain label
	return validCount > 0
}

// sanitizeSheetName sanitizes a sheet name for use as a filename
func sanitizeSheetName(name string) string {
	// Replace invalid filename characters
	invalidChars := regexp.MustCompile(`[<>:"/\\|?*]`)
	sanitized := invalidChars.ReplaceAllString(name, "_")
	// Remove leading/trailing spaces and dots
	sanitized = strings.Trim(sanitized, " .")
	// If empty after sanitization, use a default name
	if sanitized == "" {
		sanitized = "sheet"
	}
	return sanitized
}

// writeSheetToCSV writes a sheet's rows to a CSV file
func writeSheetToCSV(rows [][]string, outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, row := range rows {
		// Ensure row has at least one column
		if len(row) == 0 {
			row = []string{""}
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}
