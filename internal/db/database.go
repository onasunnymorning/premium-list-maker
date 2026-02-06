package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// DB wraps the database connection
type DB struct {
	conn *sql.DB
}

// LabelData represents a label to be inserted
type LabelData struct {
	Label  string
	Length int
}

// TagAssociation represents a tag to be associated with a label
type TagAssociation struct {
	LabelID int64
	TagID   int64
}

// New creates a new database connection and initializes the schema
func New(dbPath string) (*DB, error) {
	conn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db := &DB{conn: conn}

	// Optimize SQLite for bulk inserts
	if err := db.optimizeForBulkInsert(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to optimize database: %w", err)
	}

	if err := db.initSchema(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return db, nil
}

// optimizeForBulkInsert sets SQLite pragmas for better bulk insert performance
func (db *DB) optimizeForBulkInsert() error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL",    // Write-Ahead Logging for better concurrency
		"PRAGMA synchronous = NORMAL",  // Faster than FULL, still safe
		"PRAGMA cache_size = -64000",   // 64MB cache (negative = KB)
		"PRAGMA temp_store = MEMORY",   // Store temp tables in memory
		"PRAGMA mmap_size = 268435456", // 256MB memory-mapped I/O
		"PRAGMA foreign_keys = ON",     // Keep foreign keys enabled
	}

	for _, pragma := range pragmas {
		if _, err := db.conn.Exec(pragma); err != nil {
			return fmt.Errorf("failed to set %s: %w", pragma, err)
		}
	}

	return nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}

// initSchema creates the database tables if they don't exist
func (db *DB) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS labels (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		label TEXT UNIQUE NOT NULL,
		length INTEGER NOT NULL
	);
	
	CREATE INDEX IF NOT EXISTS idx_labels_label ON labels(label);

	CREATE TABLE IF NOT EXISTS tags (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS label_tags (
		label_id INTEGER NOT NULL,
		tag_id INTEGER NOT NULL,
		PRIMARY KEY (label_id, tag_id),
		FOREIGN KEY (label_id) REFERENCES labels(id) ON DELETE CASCADE,
		FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_label_tags_label_id ON label_tags(label_id);
	CREATE INDEX IF NOT EXISTS idx_label_tags_tag_id ON label_tags(tag_id);
	`

	_, err := db.conn.Exec(schema)
	return err
}

// InsertLabel inserts a label into the database, returns the label ID
func (db *DB) InsertLabel(label string, length int) (int64, error) {
	result, err := db.conn.Exec(
		"INSERT OR IGNORE INTO labels (label, length) VALUES (?, ?)",
		label, length,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to insert label: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get last insert id: %w", err)
	}

	// If ID is 0, label already exists, so fetch it
	if id == 0 {
		err = db.conn.QueryRow(
			"SELECT id FROM labels WHERE label = ?",
			label,
		).Scan(&id)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch existing label: %w", err)
		}
	}

	return id, nil
}

// GetOrCreateTag gets a tag ID, creating the tag if it doesn't exist
// This version uses db.conn and should NOT be called inside a transaction
func (db *DB) GetOrCreateTag(tagName string) (int64, error) {
	var tagID int64
	err := db.conn.QueryRow(
		"SELECT id FROM tags WHERE name = ?",
		tagName,
	).Scan(&tagID)

	if err == sql.ErrNoRows {
		// Tag doesn't exist, create it
		result, err := db.conn.Exec(
			"INSERT INTO tags (name) VALUES (?)",
			tagName,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to create tag: %w", err)
		}
		tagID, err = result.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("failed to get tag id: %w", err)
		}
		return tagID, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to query tag: %w", err)
	}

	return tagID, nil
}

// GetOrCreateTagTx gets a tag ID, creating the tag if it doesn't exist
// This version uses the provided transaction and should be called inside a transaction
func GetOrCreateTagTx(tx *sql.Tx, tagName string) (int64, error) {
	var tagID int64
	err := tx.QueryRow(
		"SELECT id FROM tags WHERE name = ?",
		tagName,
	).Scan(&tagID)

	if err == sql.ErrNoRows {
		// Tag doesn't exist, create it
		result, err := tx.Exec(
			"INSERT INTO tags (name) VALUES (?)",
			tagName,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to create tag: %w", err)
		}
		tagID, err = result.LastInsertId()
		if err != nil {
			return 0, fmt.Errorf("failed to get tag id: %w", err)
		}
		return tagID, nil
	} else if err != nil {
		return 0, fmt.Errorf("failed to query tag: %w", err)
	}

	return tagID, nil
}

// AddTagToLabel adds a tag to a label
func (db *DB) AddTagToLabel(labelID, tagID int64) error {
	_, err := db.conn.Exec(
		"INSERT OR IGNORE INTO label_tags (label_id, tag_id) VALUES (?, ?)",
		labelID, tagID,
	)
	if err != nil {
		return fmt.Errorf("failed to add tag to label: %w", err)
	}
	return nil
}

// BeginTransaction starts a new transaction
func (db *DB) BeginTransaction() (*sql.Tx, error) {
	return db.conn.Begin()
}

// LoadAllLabelIDs loads all existing label IDs into a map for fast lookup
// Returns a map of label -> labelID
func LoadAllLabelIDs(tx *sql.Tx) (map[string]int64, error) {
	labelMap := make(map[string]int64)

	rows, err := tx.Query("SELECT id, label FROM labels")
	if err != nil {
		return nil, fmt.Errorf("failed to query labels: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			return nil, fmt.Errorf("failed to scan label: %w", err)
		}
		labelMap[label] = id
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating labels: %w", err)
	}

	return labelMap, nil
}

// LoadAllTagIDs loads all existing tag IDs into a map for fast lookup
// Returns a map of tag name -> tagID
func LoadAllTagIDs(tx *sql.Tx) (map[string]int64, error) {
	tagMap := make(map[string]int64)

	rows, err := tx.Query("SELECT id, name FROM tags")
	if err != nil {
		return nil, fmt.Errorf("failed to query tags: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("failed to scan tag: %w", err)
		}
		tagMap[name] = id
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tags: %w", err)
	}

	return tagMap, nil
}

// BulkInsertResult contains the results of a bulk insert operation
type BulkInsertResult struct {
	LabelMap      map[string]int64
	NewCount      int // Number of newly inserted labels
	ExistingCount int // Number of labels that already existed
}

// BulkInsertLabels inserts multiple labels efficiently
// existingLabelMap should contain all existing label IDs (pre-loaded)
// Separates new labels from existing ones and uses bulk INSERT for new labels only
// Returns a map of label -> labelID and counts of new vs existing labels
func (db *DB) BulkInsertLabels(tx *sql.Tx, labels []LabelData, existingLabelMap map[string]int64) (*BulkInsertResult, error) {
	if len(labels) == 0 {
		return &BulkInsertResult{LabelMap: make(map[string]int64)}, nil
	}

	result := &BulkInsertResult{
		LabelMap: make(map[string]int64, len(labels)),
	}

	// Separate new labels from existing ones
	newLabels := make([]LabelData, 0, len(labels))

	// Track labels seen in this batch to avoid duplicates within the insert
	seenInBatch := make(map[string]bool)

	for _, l := range labels {
		// Check if it exists in DB
		if id, exists := existingLabelMap[l.Label]; exists {
			// Label already exists - use pre-loaded ID
			result.LabelMap[l.Label] = id
			result.ExistingCount++
		} else {
			// Check if we've already seen it in this batch
			if seenInBatch[l.Label] {
				// We already added it to newLabels list, so it will be inserted.
				// We don't have the ID yet, but we will get it after insert.
				// For now, we just skip adding it to newLabels again.
				continue
			}

			// Label is new - add to insert list
			newLabels = append(newLabels, l)
			seenInBatch[l.Label] = true
		}
	}

	result.NewCount = len(newLabels)

	// If no new labels, return early
	if len(newLabels) == 0 {
		return result, nil
	}

	// Build bulk INSERT with VALUES clause for new labels
	// SQLite supports up to 999 parameters, so we may need to chunk
	const maxParams = 999
	const valuesPerRow = 2                            // label and length
	const maxRowsPerInsert = maxParams / valuesPerRow // 499 rows per insert

	for i := 0; i < len(newLabels); i += maxRowsPerInsert {
		end := i + maxRowsPerInsert
		if end > len(newLabels) {
			end = len(newLabels)
		}
		chunk := newLabels[i:end]

		// Build INSERT statement with VALUES clause
		query := "INSERT INTO labels (label, length) VALUES "
		args := make([]interface{}, 0, len(chunk)*2)

		for j, l := range chunk {
			if j > 0 {
				query += ","
			}
			query += "(?, ?)"
			args = append(args, l.Label, l.Length)
		}

		// Use RETURNING id to get the exact IDs of inserted rows
		query += " RETURNING id"

		// Execute bulk insert
		rows, err := tx.Query(query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to bulk insert labels: %w", err)
		}

		// Scan returned IDs
		// The IDs are returned in the same order as the inserts
		// We trust SQLite to maintain this order for the RETURNING clause on INSERT
		idx := 0
		for rows.Next() {
			if idx >= len(chunk) {
				rows.Close()
				return nil, fmt.Errorf("retrieved more IDs than inserted rows")
			}

			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan returned id: %w", err)
			}

			result.LabelMap[chunk[idx].Label] = id
			idx++
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating returned ids: %w", err)
		}

		if idx != len(chunk) {
			return nil, fmt.Errorf("expected %d IDs, got %d", len(chunk), idx)
		}
	}

	return result, nil
}

// BulkAddTagsToLabels adds multiple tag associations efficiently using bulk INSERT
// Uses INSERT OR IGNORE to handle duplicates idempotently
// Foreign key constraints are validated automatically by SQLite
func (db *DB) BulkAddTagsToLabels(tx *sql.Tx, associations []TagAssociation) error {
	if len(associations) == 0 {
		return nil
	}

	// SQLite supports up to 999 parameters, so we may need to chunk
	const maxParams = 999
	const valuesPerRow = 2                            // label_id and tag_id
	const maxRowsPerInsert = maxParams / valuesPerRow // 499 rows per insert

	for i := 0; i < len(associations); i += maxRowsPerInsert {
		end := i + maxRowsPerInsert
		if end > len(associations) {
			end = len(associations)
		}
		chunk := associations[i:end]

		// Build INSERT statement with VALUES clause
		query := "INSERT OR IGNORE INTO label_tags (label_id, tag_id) VALUES "
		args := make([]interface{}, 0, len(chunk)*2)

		for j, assoc := range chunk {
			if j > 0 {
				query += ","
			}
			query += "(?, ?)"
			args = append(args, assoc.LabelID, assoc.TagID)
		}

		// Execute bulk insert
		if _, err := tx.Exec(query, args...); err != nil {
			return fmt.Errorf("failed to bulk insert tag associations: %w", err)
		}
	}

	return nil
}

// GetLabelID gets the ID of a label by its name
func (db *DB) GetLabelID(label string) (int64, error) {
	var id int64
	err := db.conn.QueryRow(
		"SELECT id FROM labels WHERE label = ?",
		label,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("label not found: %s", label)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to query label: %w", err)
	}
	return id, nil
}

// GetAllLabelsWithTags returns all labels with their associated tags
func (db *DB) GetAllLabelsWithTags() (map[string][]string, error) {
	query := `
		SELECT l.label, COALESCE(GROUP_CONCAT(t.name), '') as tags
		FROM labels l
		LEFT JOIN label_tags lt ON l.id = lt.label_id
		LEFT JOIN tags t ON lt.tag_id = t.id
		GROUP BY l.id, l.label
	`

	rows, err := db.conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query labels: %w", err)
	}
	defer rows.Close()

	labels := make(map[string][]string)
	for rows.Next() {
		var label string
		var tagsStr string
		if err := rows.Scan(&label, &tagsStr); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		var tags []string
		if tagsStr != "" {
			// Split comma-separated tags
			// SQLite GROUP_CONCAT uses comma by default
			for _, tag := range splitTags(tagsStr) {
				if tag != "" {
					tags = append(tags, tag)
				}
			}
		}
		labels[label] = tags
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return labels, nil
}

// splitTags splits a comma-separated string of tags
func splitTags(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	tags := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			tags = append(tags, trimmed)
		}
	}
	return tags
}
