package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const manifestName = "MANIFEST.json"

// manifest is the source of truth for which segments exist. It is replaced
// atomically (write temp file, fsync, rename, fsync dir), so after a crash it
// is always either the old or the new version, never a partial one.
type manifest struct {
	// Segments lists segment IDs in flush order.
	Segments []uint64 `json:"segments"`
	// NextSegment is the ID the next flushed segment will get.
	NextSegment uint64 `json:"next_segment"`
	// FlushedWAL is the highest WAL ID whose rows are fully contained in a
	// listed segment. WAL files with an ID <= FlushedWAL are obsolete.
	FlushedWAL uint64 `json:"flushed_wal"`
}

func loadManifest(dir string) (manifest, error) {
	m := manifest{NextSegment: 1}
	b, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("store: corrupt manifest: %w", err)
	}
	return m, nil
}

func saveManifest(dir string, m manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, manifestName+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestName)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
