package internal

import (
	"fmt"
	"path/filepath"
	"strings"
)

func validateNonOverlappingPrefixes(inputPrefix, inProgressPrefix, failedPrefix string) error {
	normalized := []namedTarget{
		{name: "input", value: normalizePrefix(inputPrefix)},
		{name: "in-progress", value: normalizePrefix(inProgressPrefix)},
		{name: "failed", value: normalizePrefix(failedPrefix)},
	}

	for i := 0; i < len(normalized); i++ {
		for j := i + 1; j < len(normalized); j++ {
			left := normalized[i]
			right := normalized[j]
			if strings.HasPrefix(left.value, right.value) || strings.HasPrefix(right.value, left.value) {
				return fmt.Errorf("%s and %s prefixes must not overlap", left.name, right.name)
			}
		}
	}

	return nil
}

func validateNonOverlappingDirectories(inputDir, inProgressDir, failedDir string) error {
	absTargets := []namedTarget{
		{name: "input", value: inputDir},
		{name: "in-progress", value: inProgressDir},
		{name: "failed", value: failedDir},
	}

	for i := range absTargets {
		absPath, err := filepath.Abs(filepath.Clean(absTargets[i].value))
		if err != nil {
			return fmt.Errorf("resolve %s directory: %w", absTargets[i].name, err)
		}
		absTargets[i].value = absPath
	}

	for i := 0; i < len(absTargets); i++ {
		for j := i + 1; j < len(absTargets); j++ {
			left := absTargets[i]
			right := absTargets[j]
			overlap, err := pathsOverlap(left.value, right.value)
			if err != nil {
				return err
			}
			if overlap {
				return fmt.Errorf("%s and %s directories must not overlap", left.name, right.name)
			}
		}
	}

	return nil
}

func pathsOverlap(a, b string) (bool, error) {
	relAB, err := filepath.Rel(a, b)
	if err != nil {
		return false, nil
	}
	if relAB == "." || !isParentTraversal(relAB) {
		return true, nil
	}

	relBA, err := filepath.Rel(b, a)
	if err != nil {
		return false, nil
	}
	if relBA == "." || !isParentTraversal(relBA) {
		return true, nil
	}

	return false, nil
}

func isParentTraversal(rel string) bool {
	if rel == ".." {
		return true
	}
	prefix := ".." + string(filepath.Separator)
	return strings.HasPrefix(rel, prefix)
}

type namedTarget struct {
	name  string
	value string
}
