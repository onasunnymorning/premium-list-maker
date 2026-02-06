package generator

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"premium-list-maker/internal/db"
	"premium-list-maker/internal/models"
)

// PremiumListEntry represents a single entry in the premium list output
type PremiumListEntry struct {
	Label    string
	Tier     int
	PriceReg *float64
	PriceRen *float64
	PriceRes *float64
	Currency string
}

// GeneratePremiumList generates a premium list CSV from tiers.json
func GeneratePremiumList(db *db.DB, tiersPath, outputPath, format, tld string) error {
	// Load tiers from JSON
	tiers, err := loadTiers(tiersPath)
	if err != nil {
		return fmt.Errorf("failed to load tiers: %w", err)
	}

	// Validate method args if needed
	if format == "cnic-new" && tld == "" {
		return fmt.Errorf("tld is required for cnic-new format")
	}

	// Get all labels with their tags
	labelsWithTags, err := db.GetAllLabelsWithTags()
	if err != nil {
		return fmt.Errorf("failed to get labels: %w", err)
	}

	// Match labels to tiers
	entries := make([]PremiumListEntry, 0)
	for label, tags := range labelsWithTags {
		bestTier := findBestTier(tags, tiers)
		if bestTier != nil {
			entries = append(entries, PremiumListEntry{
				Label:    label,
				Tier:     bestTier.Tier,
				PriceReg: bestTier.PriceReg,
				PriceRen: bestTier.PriceRen,
				PriceRes: bestTier.PriceRes,
				Currency: bestTier.Currency,
			})
		}
	}

	// Write to CSV based on format
	if format == "cnic-new" {
		if err := writeCNicNewCSV(entries, outputPath, tld); err != nil {
			return fmt.Errorf("failed to write CSV: %w", err)
		}
	} else {
		// Default format
		if err := writeCSV(entries, outputPath); err != nil {
			return fmt.Errorf("failed to write CSV: %w", err)
		}
	}

	fmt.Printf("Generated premium list with %d entries (format: %s)\n", len(entries), format)
	return nil
}

// loadTiers loads tiers from a JSON file
func loadTiers(path string) ([]models.Tier, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read tiers file: %w", err)
	}

	var tiers []models.Tier
	if err := json.Unmarshal(data, &tiers); err != nil {
		return nil, fmt.Errorf("failed to parse tiers JSON: %w", err)
	}

	return tiers, nil
}

// findBestTier finds the highest tier that matches the given tags
// Returns nil if no tier matches
func findBestTier(labelTags []string, tiers []models.Tier) *models.Tier {
	// Create a set of label tags for efficient lookup
	tagSet := make(map[string]bool)
	for _, tag := range labelTags {
		tagSet[tag] = true
	}

	var bestTier *models.Tier
	bestTierNum := -1

	// Find all matching tiers and select the one with highest tier number
	for i := range tiers {
		tier := &tiers[i]
		if hasMatchingTag(tier.Tags, tagSet) {
			if tier.Tier > bestTierNum {
				bestTier = tier
				bestTierNum = tier.Tier
			}
		}
	}

	return bestTier
}

// hasMatchingTag checks if any tier tag matches any label tag
func hasMatchingTag(tierTags []string, labelTagSet map[string]bool) bool {
	for _, tierTag := range tierTags {
		if labelTagSet[tierTag] {
			return true
		}
	}
	return false
}

// writeCSV writes the premium list entries to a CSV file
func writeCSV(entries []PremiumListEntry, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"Label", "Tier", "price_reg", "price_ren", "price_res", "currency"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write entries
	for _, entry := range entries {
		record := []string{
			entry.Label,
			fmt.Sprintf("%d", entry.Tier),
			floatPtrToString(entry.PriceReg),
			floatPtrToString(entry.PriceRen),
			floatPtrToString(entry.PriceRes),
			entry.Currency,
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// writeCNicNewCSV writes the premium list entries in the new cnic format
func writeCNicNewCSV(entries []PremiumListEntry, path, tld string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	// label,suffix,type,currency,amount
	header := []string{"label", "suffix", "type", "currency", "amount"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write entries
	for _, entry := range entries {
		// Row 1: Registration
		if entry.PriceReg != nil {
			if err := writer.Write([]string{
				entry.Label,
				tld,
				"Registration",
				strings.ToUpper(entry.Currency),
				fmt.Sprintf("%.2f", *entry.PriceReg),
			}); err != nil {
				return fmt.Errorf("failed to write registration record: %w", err)
			}
		}

		// Row 2: Renewal
		if entry.PriceRen != nil {
			if err := writer.Write([]string{
				entry.Label,
				tld,
				"Renewal",
				strings.ToUpper(entry.Currency),
				fmt.Sprintf("%.2f", *entry.PriceRen),
			}); err != nil {
				return fmt.Errorf("failed to write renewal record: %w", err)
			}
		}

		// Row 3: Restore
		if entry.PriceRes != nil {
			if err := writer.Write([]string{
				entry.Label,
				tld,
				"Restore",
				strings.ToUpper(entry.Currency),
				fmt.Sprintf("%.2f", *entry.PriceRes),
			}); err != nil {
				return fmt.Errorf("failed to write restore record: %w", err)
			}
		}
	}

	return nil
}

// floatPtrToString converts a float pointer to string, or empty string if nil
func floatPtrToString(f *float64) string {
	if f == nil {
		return ""
	}
	return fmt.Sprintf("%.2f", *f)
}
