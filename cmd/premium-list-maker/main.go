package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"premium-list-maker/internal/db"
	"premium-list-maker/internal/generator"
	"premium-list-maker/internal/importer"

	"github.com/spf13/cobra"
)

var (
	dbPath string

	// Build information (injected by GoReleaser)
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// FileImportStats tracks statistics for a single file import
type FileImportStats struct {
	Filename       string
	Imported       int // Total processed
	NewLabels      int // Newly inserted
	ExistingLabels int // Already existed
	Skipped        int
	HeaderSkipped  bool
	Errors         []string
	Duration       time.Duration
}

// TotalStats tracks overall import statistics
type TotalStats struct {
	FilesProcessed int
	FilesSkipped   int
	LabelsImported int // Total processed
	NewLabels      int // Newly inserted labels
	ExistingLabels int // Labels that already existed
	LabelsSkipped  int
	TotalErrors    []string
	MaxMemoryMB    uint64
	FileStats      []FileImportStats
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "premium-list-maker",
		Short: "Generate premium lists for domain registries",
		Long:  "A tool for managing domain labels, tags, and generating premium pricing lists",
	}

	// Global flag for database path
	rootCmd.PersistentFlags().StringVarP(&dbPath, "db", "d", "premium.db", "path to SQLite database file")

	// Import command
	importCmd := &cobra.Command{
		Use:   "import <folder>",
		Short: "Import labels from all CSV files in a folder",
		Long:  "Import domain labels from all CSV files in the specified folder. The first column should contain the label. Automatically adds length-based tags and filename-based tags.",
		Args:  cobra.ExactArgs(1),
		RunE:  runImport,
	}
	rootCmd.AddCommand(importCmd)

	// Tag command
	tagCmd := &cobra.Command{
		Use:   "tag <label> <tag1> [tag2...]",
		Short: "Add tags to a label",
		Long:  "Add one or more tags to a label. Tags are created if they don't exist.",
		Args:  cobra.MinimumNArgs(2),
		RunE:  runTag,
	}
	rootCmd.AddCommand(tagCmd)

	// Generate command
	var format string
	var tld string

	generateCmd := &cobra.Command{
		Use:   "generate <tiers.json> <output.csv>",
		Short: "Generate premium list from tiers configuration",
		Long:  "Generate a premium list CSV by matching labels to tiers. Highest tier wins in case of conflicts.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGenerate(cmd, args, format, tld)
		},
	}
	generateCmd.Flags().StringVar(&format, "format", "default", "Output format (default, cnic-new)")
	generateCmd.Flags().StringVar(&tld, "tld", "", "TLD/Suffix (required for cnic-new format)")
	rootCmd.AddCommand(generateCmd)

	// Split XLSX command
	splitXlsxCmd := &cobra.Command{
		Use:   "split-xlsx <xlsx-file> <output-dir>",
		Short: "Split an Excel file into CSV files (one per sheet)",
		Long:  "Splits an Excel (.xlsx) file into separate CSV files, one for each sheet. Only processes sheets where the first column appears to contain domain labels.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSplitXLSX(cmd, args, format)
		},
	}
	splitXlsxCmd.Flags().StringVar(&format, "format", "default", "Output format (default, andy)")
	rootCmd.AddCommand(splitXlsxCmd)

	// Deduplicate command
	deduplicateCmd := newDeduplicateCmd()
	rootCmd.AddCommand(deduplicateCmd)

	// Version command
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("premium-list-maker version %s\n", version)
			fmt.Printf("commit: %s\n", commit)
			fmt.Printf("built at: %s\n", date)
		},
	}
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runImport(cmd *cobra.Command, args []string) error {
	startTime := time.Now()
	folderPath := args[0]

	// Open database
	database, err := db.New(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	// Read directory
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("failed to read folder: %w", err)
	}

	// Find all CSV files
	var csvFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".csv") {
			csvFiles = append(csvFiles, entry.Name())
		}
	}

	if len(csvFiles) == 0 {
		return fmt.Errorf("no CSV files found in folder: %s", folderPath)
	}

	fmt.Printf("Found %d CSV file(s) to import\n", len(csvFiles))

	// Track overall statistics
	totalStats := TotalStats{
		TotalErrors: make([]string, 0),
		FileStats:   make([]FileImportStats, 0),
	}

	// Import each CSV file
	for _, csvFile := range csvFiles {
		csvPath := filepath.Join(folderPath, csvFile)

		// Extract filename tag (filename without .csv extension)
		filenameTag := strings.TrimSuffix(csvFile, ".csv")
		filenameTag = strings.TrimSuffix(filenameTag, ".CSV")

		// Count lines in file for display
		lineCount, err := importer.CountCSVLines(csvPath)
		if err != nil {
			// If we can't count lines, just proceed without the count
			fmt.Printf("\nImporting %s (tag: %s)...\n", csvFile, filenameTag)
		} else {
			fmt.Printf("\nImporting %s (tag: %s, %d lines)...\n", csvFile, filenameTag, lineCount)
		}

		fileStartTime := time.Now()

		// Import with auto-tag always enabled and filename tag
		stats, err := importer.ImportCSV(database, csvPath, true, filenameTag)
		if err != nil {
			fmt.Printf("Error importing %s: %v\n", csvFile, err)
			totalStats.FilesSkipped++
			totalStats.TotalErrors = append(totalStats.TotalErrors, fmt.Sprintf("%s: %v", csvFile, err))
			continue
		}

		fileDuration := time.Since(fileStartTime)
		totalStats.FilesProcessed++
		totalStats.LabelsImported += stats.Imported
		totalStats.NewLabels += stats.NewLabels
		totalStats.ExistingLabels += stats.ExistingLabels
		totalStats.LabelsSkipped += stats.Skipped
		totalStats.TotalErrors = append(totalStats.TotalErrors, stats.Errors...)
		if stats.MaxMemoryMB > totalStats.MaxMemoryMB {
			totalStats.MaxMemoryMB = stats.MaxMemoryMB
		}

		totalStats.FileStats = append(totalStats.FileStats, FileImportStats{
			Filename:       csvFile,
			Imported:       stats.Imported,
			NewLabels:      stats.NewLabels,
			ExistingLabels: stats.ExistingLabels,
			Skipped:        stats.Skipped,
			HeaderSkipped:  stats.HeaderSkipped,
			Errors:         stats.Errors,
			Duration:       fileDuration,
		})
	}

	// Final memory check
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	finalMemMB := m.Alloc / 1024 / 1024
	if finalMemMB > totalStats.MaxMemoryMB {
		totalStats.MaxMemoryMB = finalMemMB
	}

	// Print comprehensive summary report
	totalDuration := time.Since(startTime)
	printSummaryReport(&totalStats, totalDuration, len(csvFiles))

	return nil
}

func printSummaryReport(stats *TotalStats, totalDuration time.Duration, totalFiles int) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("IMPORT SUMMARY REPORT")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Printf("\n📊 Overall Statistics:\n")
	fmt.Printf("  Total Files:           %d\n", totalFiles)
	fmt.Printf("  Files Processed:       %d\n", stats.FilesProcessed)
	fmt.Printf("  Files Skipped:         %d\n", stats.FilesSkipped)
	fmt.Printf("  Labels Processed:      %d\n", stats.LabelsImported)
	if stats.ExistingLabels > 0 {
		fmt.Printf("    - New Labels:        %d\n", stats.NewLabels)
		fmt.Printf("    - Existing Labels:   %d (already in database)\n", stats.ExistingLabels)
	} else {
		fmt.Printf("  New Labels:            %d\n", stats.NewLabels)
	}
	fmt.Printf("  Labels Skipped:        %d\n", stats.LabelsSkipped)
	fmt.Printf("  Total Runtime:         %v\n", totalDuration.Round(time.Second))
	fmt.Printf("  Peak Memory Usage:     %d MB\n", stats.MaxMemoryMB)

	if len(stats.FileStats) > 0 {
		fmt.Printf("\n📁 Per-File Breakdown:\n")
		for _, fileStat := range stats.FileStats {
			fmt.Printf("  %s:\n", fileStat.Filename)
			if fileStat.ExistingLabels > 0 {
				fmt.Printf("    Processed: %d (New: %d, Existing: %d), Skipped: %d, Duration: %v\n",
					fileStat.Imported, fileStat.NewLabels, fileStat.ExistingLabels, fileStat.Skipped, fileStat.Duration.Round(time.Second))
			} else {
				fmt.Printf("    Imported: %d, Skipped: %d, Duration: %v\n",
					fileStat.Imported, fileStat.Skipped, fileStat.Duration.Round(time.Second))
			}
			if fileStat.HeaderSkipped {
				fmt.Printf("    (Header row skipped)\n")
			}
			if len(fileStat.Errors) > 0 {
				fmt.Printf("    Errors: %d\n", len(fileStat.Errors))
			}
		}
	}

	if len(stats.TotalErrors) > 0 {
		fmt.Printf("\n⚠️  Errors Encountered: %d\n", len(stats.TotalErrors))

		// Create error report file
		timestamp := time.Now().Format("20060102_150405")
		reportFilename := fmt.Sprintf("import_errors_%s.txt", timestamp)

		file, err := os.Create(reportFilename)
		if err != nil {
			fmt.Printf("    Failed to create error report file: %v\n", err)
			// Fallback to printing first 10
			if len(stats.TotalErrors) <= 10 {
				for _, err := range stats.TotalErrors {
					fmt.Printf("    - %s\n", err)
				}
			} else {
				for _, err := range stats.TotalErrors[:10] {
					fmt.Printf("    - %s\n", err)
				}
				fmt.Printf("    ... and %d more errors\n", len(stats.TotalErrors)-10)
			}
		} else {
			defer file.Close()

			// Write header
			fmt.Fprintf(file, "IMPORT ERROR REPORT\n")
			fmt.Fprintf(file, "Generated: %s\n", time.Now().Format(time.RFC1123))
			fmt.Fprintf(file, "Total Errors: %d\n\n", len(stats.TotalErrors))

			// Write all errors
			for _, errStr := range stats.TotalErrors {
				fmt.Fprintf(file, "- %s\n", errStr)
			}

			fmt.Printf("    --> Full error list saved to: %s\n", reportFilename)

			// Still print a few for immediate feedback
			limit := 5
			if len(stats.TotalErrors) < limit {
				limit = len(stats.TotalErrors)
			}
			for i := 0; i < limit; i++ {
				fmt.Printf("    - %s\n", stats.TotalErrors[i])
			}
			if len(stats.TotalErrors) > limit {
				fmt.Printf("    ... (%d more errors in report file)\n", len(stats.TotalErrors)-limit)
			}
		}
	}

	fmt.Println(strings.Repeat("=", 80))
}

func runTag(cmd *cobra.Command, args []string) error {
	label := args[0]
	tags := args[1:]

	// Open database
	database, err := db.New(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	// Get or create label
	labelID, err := database.GetLabelID(label)
	if err != nil {
		// Label doesn't exist, create it
		length := len(label)
		labelID, err = database.InsertLabel(label, length)
		if err != nil {
			return fmt.Errorf("failed to create label: %w", err)
		}
	}

	// Add tags
	for _, tagName := range tags {
		tagID, err := database.GetOrCreateTag(tagName)
		if err != nil {
			return fmt.Errorf("failed to get or create tag %s: %w", tagName, err)
		}
		if err := database.AddTagToLabel(labelID, tagID); err != nil {
			return fmt.Errorf("failed to add tag %s to label: %w", tagName, err)
		}
	}

	fmt.Printf("Added %d tag(s) to label '%s'\n", len(tags), label)
	return nil
}

func runGenerate(cmd *cobra.Command, args []string, format, tld string) error {
	tiersPath := args[0]
	outputPath := args[1]

	// Ensure output directory exists
	outputDir := filepath.Dir(outputPath)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	// Open database
	database, err := db.New(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer database.Close()

	// Generate premium list
	if err := generator.GeneratePremiumList(database, tiersPath, outputPath, format, tld); err != nil {
		return err
	}

	return nil
}

func runSplitXLSX(cmd *cobra.Command, args []string, format string) error {
	xlsxPath := args[0]
	outputDir := args[1]

	// Split XLSX file
	if err := importer.SplitXLSX(xlsxPath, outputDir, format); err != nil {
		return err
	}

	return nil
}
