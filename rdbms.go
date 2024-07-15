// Copyright © 2024 ROBON Inc. All rights reserved.
// This software is licensed under PolyForm Shield License 1.0.0
// https://polyformproject.org/licenses/shield/1.0.0/

package main

import (
	"context"
	"io"
	"sync"
)

// DataSourceName は、ドライバが必要とする接続のための情報です。
type DataSourceName interface {
	// DSN は、sql.DB.Open() に渡す文字列を返します。
	DSN() string
}

// MetadataExtractor は、メタデータを抽出します。
type MetadataExtractor interface {
	// Run は、メータデータの抽出を実行します。
	Run(ctx context.Context, dsn DataSourceName, out io.Writer) error
	// FindSchema は、スキーマの一覧を取得する。
	FindSchema(ctx context.Context, dsn DataSourceName) ([]string, error)
	// SetConfig は、Config を保持します。
	SetConfig(config *Config)
}

var (
	extractorsMu sync.RWMutex
	extractors   = make(map[string]MetadataExtractor)
)

// register は、MetadataExtractor をドライバー名で登録します。
func register(name string, extractor MetadataExtractor) {
	extractorsMu.Lock()
	defer extractorsMu.Unlock()
	// nil チェック、二重登録チェックは自パッケージ内のみのため省略
	extractors[name] = extractor
}

// GetExtractor は、ドライバーに対応した MetadataExtractor を返します。
func GetExtractor(name string) MetadataExtractor {
	extractorsMu.RLock()
	ext, ok := extractors[name]
	extractorsMu.RUnlock()
	if ok {
		return ext
	}
	return nil
}

// MetadataInProcess は、Pipeline を流れる Metadata と error です。
type MetadataInProcess struct {
	Data Metadata
	Err  error
}

// writeCSV は、メタデータを Zip ファイルに出力します。
func writeCSV(ctx context.Context,
	input <-chan MetadataInProcess, out io.Writer) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-input:
			if !ok {
				return nil
			}
			_, err := out.Write([]byte(m.Data.ToCSVString()))
			if err != nil {
				return err
			}
		}
	}
}
