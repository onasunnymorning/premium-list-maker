package importer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestSplitXLSX_AndyFormat(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir, err := os.MkdirTemp("", "split-xlsx-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a mock Excel file
	xlsxPath := filepath.Join(tmpDir, "test.xlsx")
	f := excelize.NewFile()

	// Create "Sheet1" with Tier Level column
	sheetName := "Sheet1"
	index, err := f.NewSheet(sheetName)
	if err != nil {
		t.Fatalf("failed to create sheet: %v", err)
	}
	f.SetActiveSheet(index)

	// Set headers
	f.SetCellValue(sheetName, "A1", "Label")
	f.SetCellValue(sheetName, "B1", "Source")
	f.SetCellValue(sheetName, "C1", "Category")
	f.SetCellValue(sheetName, "D1", "Length")
	f.SetCellValue(sheetName, "E1", "SLD")
	f.SetCellValue(sheetName, "F1", "Tier Level")

	// Set data
	// Row 2: Tier 10
	f.SetCellValue(sheetName, "A2", "test1.co")
	f.SetCellValue(sheetName, "F2", "10")

	// Row 3: Tier 5
	f.SetCellValue(sheetName, "A3", "test2.co")
	f.SetCellValue(sheetName, "F3", "5")

	// Row 4: Tier 10
	f.SetCellValue(sheetName, "A4", "test3.co")
	f.SetCellValue(sheetName, "F4", "10")

	// Row 5: Null Tier (should go to lowest tier = 5)
	f.SetCellValue(sheetName, "A5", "test4.co")
	f.SetCellValue(sheetName, "F5", "")

	// Row 6: "0" Tier (should go to lowest tier = 5)
	f.SetCellValue(sheetName, "A6", "test5.co")
	f.SetCellValue(sheetName, "F6", "0")

	if err := f.SaveAs(xlsxPath); err != nil {
		t.Fatalf("failed to save excel file: %v", err)
	}

	// Output directory
	outDir := filepath.Join(tmpDir, "output")

	// Run SplitXLSX with "andy" format
	if err := SplitXLSX(xlsxPath, outDir, "andy"); err != nil {
		t.Fatalf("SplitXLSX failed: %v", err)
	}

	// Verify output files
	// Expecting: "Sheet1 - tier 10.csv" and "Sheet1 - tier 5.csv"
	// Tier 10 should have test1.co, test3.co
	// Tier 5 should have test2.co, test4.co, test5.co

	tier10Path := filepath.Join(outDir, "Sheet1 - tier 10.csv")
	checkFileExists(t, tier10Path)
	checkFileContains(t, tier10Path, "test1.co")
	checkFileContains(t, tier10Path, "test3.co")
	checkFileNotContains(t, tier10Path, "test2.co")

	tier5Path := filepath.Join(outDir, "Sheet1 - tier 5.csv")
	checkFileExists(t, tier5Path)
	checkFileContains(t, tier5Path, "test2.co")
	checkFileContains(t, tier5Path, "test4.co") // null
	checkFileContains(t, tier5Path, "test5.co") // 0
	checkFileNotContains(t, tier5Path, "test1.co")

	// Verify tiers JSON file
	matches, err := filepath.Glob(filepath.Join(outDir, "tiers-*.json"))
	if err != nil {
		t.Fatalf("failed to glob tiers json: %v", err)
	}
	if len(matches) == 0 {
		t.Errorf("expected tiers-*.json file to be created")
	} else {
		tiersJSONPath := matches[0]
		checkFileContains(t, tiersJSONPath, `"tier": 10`)
		checkFileContains(t, tiersJSONPath, `"Sheet1 - tier 10"`)
		checkFileContains(t, tiersJSONPath, `"tier": 5`)
		checkFileContains(t, tiersJSONPath, `"Sheet1 - tier 5"`)
	}
}

func TestSplitXLSX_AndyFormat_LowestOne(t *testing.T) {
	// Test case where no lowest tier exists (only nulls) -> default to 1 as per user request
	tmpDir, err := os.MkdirTemp("", "split-xlsx-test-min")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	xlsxPath := filepath.Join(tmpDir, "test.xlsx")
	f := excelize.NewFile()
	sheetName := "Sheet1"
	index, err := f.NewSheet(sheetName)
	if err != nil {
		t.Fatalf("failed to create sheet: %v", err)
	}
	f.SetActiveSheet(index)

	f.SetCellValue(sheetName, "A1", "Label")
	f.SetCellValue(sheetName, "F1", "Tier Level")
	f.SetCellValue(sheetName, "A2", "null.co")
	f.SetCellValue(sheetName, "F2", "")

	if err := f.SaveAs(xlsxPath); err != nil {
		t.Fatalf("failed to save excel file: %v", err)
	}

	outDir := filepath.Join(tmpDir, "output")
	if err := SplitXLSX(xlsxPath, outDir, "andy"); err != nil {
		t.Fatalf("SplitXLSX failed: %v", err)
	}

	// Expecting "Sheet1 - tier 1.csv"
	tier1Path := filepath.Join(outDir, "Sheet1 - tier 1.csv")
	checkFileExists(t, tier1Path)
	checkFileContains(t, tier1Path, "null.co")

	// Verify tiers JSON file
	matches, err := filepath.Glob(filepath.Join(outDir, "tiers-*.json"))
	if err != nil {
		t.Fatalf("failed to glob tiers json: %v", err)
	}
	if len(matches) == 0 {
		t.Errorf("expected tiers-*.json file to be created")
	} else {
		tiersJSONPath := matches[0]
		checkFileContains(t, tiersJSONPath, `"tier": 1`)
		checkFileContains(t, tiersJSONPath, `"Sheet1 - tier 1"`)
	}
}

func checkFileExists(t *testing.T, path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected file %s to exist, but it does not", path)
	}
}

func checkFileContains(t *testing.T, path, content string) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("failed to read file %s: %v", path, err)
		return
	}
	if !strings.Contains(string(bytes), content) {
		t.Errorf("file %s expected to contain %q, but got:\n%s", path, content, string(bytes))
	}
}

func checkFileNotContains(t *testing.T, path, content string) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("failed to read file %s: %v", path, err)
		return
	}
	if strings.Contains(string(bytes), content) {
		t.Errorf("file %s expected NOT to contain %q, but it does", path, content)
	}
}
