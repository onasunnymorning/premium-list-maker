package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	premiumListPath     string
	existingDomainsPath string
)

func newDeduplicateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deduplicate",
		Short: "Deduplicate premium list against existing domains",
		Long:  "Filter domains from the premium list that are present in the existing domains list.",
		RunE:  runDeduplicate,
	}

	cmd.Flags().StringVar(&premiumListPath, "premium-list", "", "Path to the premium list file (CSV)")
	cmd.Flags().StringVar(&existingDomainsPath, "existing-domains-list", "", "Path to the existing domains list (CSV)")
	cmd.MarkFlagRequired("premium-list")
	cmd.MarkFlagRequired("existing-domains-list")

	return cmd
}

func runDeduplicate(cmd *cobra.Command, args []string) error {
	// 1. Load existing domains
	fmt.Println("Loading existing domains...")
	existingDomains, err := loadExistingDomains(existingDomainsPath)
	if err != nil {
		return fmt.Errorf("failed to load existing domains: %w", err)
	}
	fmt.Printf("Loaded %d existing domains.\n", len(existingDomains))

	// 2. Process premium list
	fmt.Println("Processing premium list...")

	// Create output filenames
	timestamp := time.Now().Format("20060102-150405")
	premiumDir := filepath.Dir(premiumListPath)
	premiumBase := filepath.Base(premiumListPath)

	sanitizedFilename := fmt.Sprintf("sanitized-%s-%s", timestamp, premiumBase)
	sanitizedPath := filepath.Join(premiumDir, sanitizedFilename)

	catchListFilename := fmt.Sprintf("catch-list-%s.csv", timestamp)
	catchListPath := filepath.Join(premiumDir, catchListFilename)

	// Open input file
	inputFile, err := os.Open(premiumListPath)
	if err != nil {
		return fmt.Errorf("failed to open premium list: %w", err)
	}
	defer inputFile.Close()

	// Open output files
	sanitizedFile, err := os.Create(sanitizedPath)
	if err != nil {
		return fmt.Errorf("failed to create sanitized file: %w", err)
	}
	defer sanitizedFile.Close()

	catchFile, err := os.Create(catchListPath)
	if err != nil {
		return fmt.Errorf("failed to create catch list file: %w", err)
	}
	defer catchFile.Close()

	// Set up CSV reader/writers
	reader := csv.NewReader(inputFile)
	reader.FieldsPerRecord = -1 // Allow variable fields

	sanitizedWriter := csv.NewWriter(sanitizedFile)
	defer sanitizedWriter.Flush()

	catchWriter := csv.NewWriter(catchFile)
	defer catchWriter.Flush()

	// Write header for catch list
	if err := catchWriter.Write([]string{"label", "w"}); err != nil {
		return fmt.Errorf("failed to write catch list header: %w", err)
	}

	// Statistics
	var (
		processedCount int
		removedCount   int
		keptCount      int
		headerSkipped  bool
	)

	// Helper to check if row is header
	isHeader := func(row []string) bool {
		if len(row) == 0 {
			return false
		}
		// Simple heuristic: check if first column is "label" or "domain" (case insensitive)
		firstCol := strings.ToLower(strings.TrimSpace(row[0]))
		return firstCol == "label" || firstCol == "domain" || firstCol == "labels" || firstCol == "domains"
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading premium list: %w", err)
		}

		processedCount++

		// Handle header: always write to sanitized, skip check
		if !headerSkipped && isHeader(record) {
			if err := sanitizedWriter.Write(record); err != nil {
				return fmt.Errorf("failed to write header to sanitized list: %w", err)
			}
			headerSkipped = true
			continue
		}

		if len(record) == 0 {
			continue
		}

		label := strings.TrimSpace(record[0])
		normalizedLabel := strings.ToLower(label)

		if existingDomains[normalizedLabel] {
			// Found in existing list - add to catch list
			if err := catchWriter.Write([]string{label, "w"}); err != nil {
				return fmt.Errorf("failed to write to catch list: %w", err)
			}
			removedCount++
		} else {
			// Not found - keep in sanitized list
			if err := sanitizedWriter.Write(record); err != nil {
				return fmt.Errorf("failed to write to sanitized list: %w", err)
			}
			keptCount++
		}
	}

	fmt.Printf("Processing complete!\n")
	fmt.Printf("  - Processed: %d\n", processedCount)
	fmt.Printf("  - Removed:   %d (saved to %s)\n", removedCount, catchListFilename)
	fmt.Printf("  - Kept:      %d (saved to %s)\n", keptCount, sanitizedFilename)

	return nil
}

func loadExistingDomains(path string) (map[string]bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	domains := make(map[string]bool)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(record) > 0 {
			label := strings.TrimSpace(record[0])
			// Skip empty lines or likely headers if midway (though simplified reading assumes header might be processed or just ignored as a non-match)
			// For existing domains list, we probably want to skip header if it exists.
			// Let's assume typical lowercase check for "label"
			if strings.ToLower(label) == "label" || strings.ToLower(label) == "domain" {
				continue
			}

			domains[strings.ToLower(label)] = true
		}
	}

	return domains, nil
}
