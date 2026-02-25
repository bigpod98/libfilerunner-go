package internal

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var ErrNoFileAvailable = errors.New("no file available")

// DirectoryBackend implements a simple file-queue workflow on top of local directories.
// Files are claimed by moving them from InputDir to InProgressDir.
type DirectoryBackend struct {
	InputDir      string
	InProgressDir string
	FailedDir     string
}

// ClaimedFile represents a file currently parked in the in-progress directory.
type ClaimedFile struct {
	name string
	path string
}

func NewDirectoryBackend(inputDir, inProgressDir, failedDir string) (*DirectoryBackend, error) {
	if strings.TrimSpace(inputDir) == "" {
		return nil, errors.New("input directory is required")
	}
	if strings.TrimSpace(inProgressDir) == "" {
		return nil, errors.New("in-progress directory is required")
	}
	if strings.TrimSpace(failedDir) == "" {
		return nil, errors.New("failed directory is required")
	}

	return &DirectoryBackend{
		InputDir:      inputDir,
		InProgressDir: inProgressDir,
		FailedDir:     failedDir,
	}, nil
}

// EnsureDirectories creates the queue directories if they do not exist.
func (b *DirectoryBackend) EnsureDirectories() error {
	for _, dir := range []string{b.InputDir, b.InProgressDir, b.FailedDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

// ClaimNext moves the next file from input to in-progress and returns a handle.
// Selection is lexicographic by file name and ignores directories.
func (b *DirectoryBackend) ClaimNext(ctx context.Context) (*ClaimedFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(b.InputDir)
	if err != nil {
		return nil, err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		src := filepath.Join(b.InputDir, name)
		dst, err := uniqueDestination(b.InProgressDir, name)
		if err != nil {
			return nil, err
		}

		if err := os.Rename(src, dst); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// File disappeared between listing and claim; try the next one.
				continue
			}
			return nil, err
		}

		return &ClaimedFile{
			name: filepath.Base(dst),
			path: dst,
		}, nil
	}

	return nil, ErrNoFileAvailable
}

func (f *ClaimedFile) Name() string {
	return f.name
}

func (f *ClaimedFile) Path() string {
	return f.path
}

func (f *ClaimedFile) Open() (io.ReadCloser, error) {
	return os.Open(f.path)
}

func (f *ClaimedFile) Delete() error {
	return os.Remove(f.path)
}

// MoveToFailed moves the in-progress file into the failed directory.
// If the target file already exists, a unique suffix is added.
func (f *ClaimedFile) MoveToFailed(failedDir string) (string, error) {
	dst, err := uniqueDestination(failedDir, f.name)
	if err != nil {
		return "", err
	}
	if err := os.Rename(f.path, dst); err != nil {
		return "", err
	}
	f.path = dst
	f.name = filepath.Base(dst)
	return dst, nil
}

func uniqueDestination(dir, name string) (string, error) {
	base := filepath.Base(name)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)

	candidate := filepath.Join(dir, base)
	if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
		return candidate, nil
	} else if err != nil {
		return "", err
	}

	for i := 1; ; i++ {
		next := filepath.Join(dir, stem+"_"+itoa(i)+ext)
		if _, err := os.Stat(next); errors.Is(err, os.ErrNotExist) {
			return next, nil
		} else if err != nil {
			return "", err
		}
	}
}

func itoa(i int) string {
	// Avoid pulling in strconv for a tiny internal helper.
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + (i % 10))
		i /= 10
	}
	return string(buf[pos:])
}
