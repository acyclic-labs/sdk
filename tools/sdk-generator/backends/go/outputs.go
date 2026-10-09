package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

func collectOutputs(root string) ([]string, map[string]string, error) {
	var paths []string
	hashes := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Base(path) == "generation-receipt.json" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		paths = append(paths, rel)
		hashes[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	sort.Strings(paths)
	return paths, hashes, err
}

// newOutputPath requires an existing parent and an absent output leaf. Resolve
// aliases before checking overlap; os.Mkdir later rejects a raced existing leaf.
// This producer never removes an existing output, even after a failed run.
