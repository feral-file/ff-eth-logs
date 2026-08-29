package backfill

import (
	"crypto/md5" //nolint:gosec // GCS reports MD5 for objects; used for integrity, not authentication
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ManifestName is the file finish requires at the export root.
const ManifestName = "manifest.json"

// Manifest is the trusted description of an export, written from the source
// of the data (BigQuery row counts per partition, GCS object checksums) —
// not from the files on the loading host. finish refuses to publish coverage
// unless the database, the Parquet footers and the local files all match it.
//
// Reason: the loader can only compare a copy with itself. Footer row counts
// prove the files were loaded completely, but not that the export contained
// every log for its block interval — a deliberately partial export (the
// one-day rehearsal) has internally consistent files. The interval and the
// per-partition counts here come from the query that produced the export,
// so a partial or wrong export cannot match them, and the checksums catch a
// truncated or altered copy. See docs/operations.md for how it is generated.
type Manifest struct {
	Export string `json:"export"`
	Source string `json:"source"`
	Blocks struct {
		First uint64 `json:"first"`
		Last  uint64 `json:"last"`
		Rows  int64  `json:"rows"`
	} `json:"blocks"`
	Logs struct {
		Rows  int64            `json:"rows"`
		Parts map[string]int64 `json:"parts"` // "NNN" → rows in logs/part=NNN
	} `json:"logs"`
	Files map[string]ManifestFile `json:"files"` // path relative to the export root
}

// ManifestFile is one export object as GCS reports it.
type ManifestFile struct {
	Size int64  `json:"size"`
	MD5  string `json:"md5"` // base64, as in GCS object metadata
}

// readManifest loads and sanity-checks the manifest at dir.
func readManifest(dir string) (*Manifest, error) {
	raw, err := os.ReadFile(filepath.Join(dir, ManifestName)) //nolint:gosec // operator-supplied export directory
	if err != nil {
		return nil, fmt.Errorf("%s is required at the export root and must come from the export's source, not from the copy (docs/operations.md): %w", ManifestName, err)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", ManifestName, err)
	}
	if m.Blocks.Last < m.Blocks.First || m.Blocks.Rows != int64(m.Blocks.Last-m.Blocks.First+1) { //nolint:gosec // fits int64
		return nil, fmt.Errorf("%s: blocks %d-%d with %d rows is not one contiguous run", ManifestName, m.Blocks.First, m.Blocks.Last, m.Blocks.Rows)
	}
	var sum int64
	for _, n := range m.Logs.Parts {
		sum += n
	}
	if sum != m.Logs.Rows {
		return nil, fmt.Errorf("%s: partition rows sum to %d, logs.rows is %d", ManifestName, sum, m.Logs.Rows)
	}
	if len(m.Files) == 0 {
		return nil, fmt.Errorf("%s lists no files", ManifestName)
	}
	return &m, nil
}

// partRows returns the manifest's row count for a partition, or an error
// when the manifest does not cover it.
func (m *Manifest) partRows(part uint64) (int64, error) {
	n, ok := m.Logs.Parts[fmt.Sprintf("%03d", part)]
	if !ok {
		return 0, fmt.Errorf("%s has no entry for logs/part=%03d", ManifestName, part)
	}
	return n, nil
}

// verifyFiles checks that every file the manifest lists exists locally with
// the recorded size and MD5, and that no unlisted Parquet file is present.
func (m *Manifest) verifyFiles(dir string) error {
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		want := m.Files[name]
		size, sum, err := fileSizeAndMD5(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			return fmt.Errorf("%s listed in %s: %w", name, ManifestName, err)
		}
		if size != want.Size || sum != want.MD5 {
			return fmt.Errorf("%s differs from %s (size %d vs %d, md5 %s vs %s); the copy is truncated or altered", name, ManifestName, size, want.Size, sum, want.MD5)
		}
	}
	local, err := filepath.Glob(filepath.Join(dir, "*", "*.parquet"))
	if err != nil {
		return err
	}
	nested, err := filepath.Glob(filepath.Join(dir, "*", "*", "*.parquet"))
	if err != nil {
		return err
	}
	for _, path := range append(local, nested...) {
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if _, ok := m.Files[filepath.ToSlash(rel)]; !ok {
			return fmt.Errorf("%s is not listed in %s; the copy holds files the export did not produce", rel, ManifestName)
		}
	}
	return nil
}

// fileSizeAndMD5 streams one file, returning its size and base64 MD5.
func fileSizeAndMD5(path string) (int64, string, error) {
	f, err := os.Open(path) //nolint:gosec // operator-supplied export directory
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = f.Close() }()
	h := md5.New() //nolint:gosec // integrity check against GCS metadata, not a security boundary
	size, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return size, base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

// manifestPartDir is the export-relative directory of a partition.
func manifestPartDir(part uint64) string {
	return strings.Join([]string{"logs", fmt.Sprintf("part=%03d", part)}, "/")
}
