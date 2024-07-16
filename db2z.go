// Copyright © 2024 ROBON Inc. All rights reserved.
// This software is licensed under PolyForm Shield License 1.0.0
// https://polyformproject.org/licenses/shield/1.0.0/

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func init() {
	register(Db2Driver+".SYSIBM", &ZDb2Extractor{})
}

// ZDb2Extractor は、PostgreSQL から Metadata を抽出します。
type ZDb2Extractor struct {
	pool   *sql.DB
	config *Config
}

// Run は、メータデータの抽出を実行します。MetadataExtractor の実装です。
func (e *ZDb2Extractor) Run(ctx context.Context,
	dsn DataSourceName, out io.Writer) error {

	myCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	e.pool, err = sql.Open(Db2Driver, dsn.DSN())
	if err != nil {
		return err
	}
	defer e.pool.Close()

	tableCh := e.extractTables(myCtx)
	columnCh := e.extractColumns(myCtx, tableCh)
	return writeCSV(myCtx, columnCh, out)
}

// extractTables は、テーブル情報を抽出します。
// https://www.ibm.com/docs/ja/db2-for-zos/13?topic=tables-systables
func (e *ZDb2Extractor) extractTables(ctx context.Context,
) <-chan MetadataInProcess {

	output := make(chan MetadataInProcess)
	go func() {
		defer close(output)

		cols, err := ColumnList(ctx, e.pool, `
			SELECT NAME
			FROM SYSIBM.SYSCOLUMNS
			WHERE TBCREATOR='SYSIBM'
			  AND TBNAME='SYSTABLES'
			ORDER BY COLNO`)
		if err != nil {
			output <- MetadataInProcess{Err: err}
			return
		}

		query := NewQuery(cols, fmt.Sprintf(
			`FROM SYSIBM.SYSTABLES
			WHERE TYPE != 'A'
              AND CREATOR in %s
			ORDER BY CREATOR, NAME`,
			e.config.TargetSchemaInClause(),
		))

		rows, err := query.Exec(ctx, e.pool)
		if err != nil {
			output <- MetadataInProcess{Err: err}
			return
		}
		defer rows.Close()

		for rows.Next() {
			m, err := query.Scan(rows)
			if err != nil {
				output <- MetadataInProcess{Err: err}
				return
			}
			select {
			case <-ctx.Done():
				return
			case output <- MetadataInProcess{Data: *e.toMetadata(m)}:
			}
		}
	}()
	return output
}

// toMetadata は、information_schema.tables の行の map から Metadata を作ります。
func (e *ZDb2Extractor) toMetadata(m map[string]string) *Metadata {
	meta := &Metadata{
		MetaType: 1, // core.TableData
		Lang:     e.config.Lang,
	}
	if v, ok := m["NAME"]; ok {
		meta.Name = v
	}
	if v, ok := m["CREATOR"]; ok && meta.Name != "" {
		meta.FormalName = strings.TrimSpace(v) + "." + meta.Name
	}
	if v, ok := m["REMARKS"]; ok {
		for _, str := range e.config.Remarks {
			switch str {
			case "Alias":
				meta.Alias = v
			case "Description":
				meta.Description = v
			}
		}
	}
	return meta
}

// extractColumns は、カラム情報を抽出します。
// https://www.ibm.com/docs/ja/db2-for-zos/13?topic=tables-syscolumns
func (e *ZDb2Extractor) extractColumns(ctx context.Context,
	input <-chan MetadataInProcess) <-chan MetadataInProcess {

	output := make(chan MetadataInProcess)
	go func() {
		defer close(output)

		cols, err := ColumnList(ctx, e.pool, `
			SELECT NAME 
			FROM SYSIBM.SYSCOLUMNS 
			WHERE TBCREATOR='SYSIBM'
			  AND TBNAME='SYSCOLUMNS'
			ORDER BY COLNO`)
		if err != nil {
			output <- MetadataInProcess{Err: err}
			return
		}

		query := NewQuery(cols, fmt.Sprintf(
			`FROM SYSIBM.SYSCOLUMNS
			WHERE TBCREATOR in %s
			ORDER BY TBCREATOR, TBNAME, COLNO`,
			e.config.TargetSchemaInClause(),
		))

		rows, err := query.Exec(ctx, e.pool)
		if err != nil {
			output <- MetadataInProcess{Err: err}
			return
		}
		defer rows.Close()

		var meta *Metadata
		var col *Column
		var formalName string
		for rows.Next() {
			if meta == nil {
				select {
				case <-ctx.Done():
					return
				case mip := <-input:
					if mip.Err != nil {
						output <- mip
						return
					}
					meta = &mip.Data
					if col != nil {
						if meta.FormalName != formalName {
							err = fmt.Errorf("meta.FormalName(%s) != formalName(%s)",
								meta.FormalName, formalName)
							output <- MetadataInProcess{Err: err}
						}
						meta.Columns = append(meta.Columns, *col)
					}
				}
			}
			m, err := query.Scan(rows)
			if err != nil {
				output <- MetadataInProcess{Err: err}
				return
			}
			col, formalName = e.toColumn(m)
			if meta.FormalName == formalName {
				meta.Columns = append(meta.Columns, *col)
			} else {
				select {
				case <-ctx.Done():
					return
				case output <- MetadataInProcess{Data: *meta}:
					meta = nil
				}
			}
		}
		if meta != nil {
			select {
			case <-ctx.Done():
				return
			case output <- MetadataInProcess{Data: *meta}:
			}
		}
	}()
	return output
}

// toColumn は、information_schema.columns の行の map から Column と
// Metadata.FormalName を作ります。
func (e *ZDb2Extractor) toColumn(m map[string]string) (*Column, string) {
	col := &Column{}
	if v, ok := m["NAME"]; ok {
		col.Name = v
	}
	if v, ok := m["COLTYPE"]; ok {
		col.Type = strings.TrimSpace(v)
	}
	if v, ok := m["NULLS"]; ok {
		if v == "Y" {
			col.Mode = 0
		} else {
			col.Mode = 1
		}
	}
	if v, ok := m["COLNO"]; ok {
		i, err := strconv.Atoi(v)
		if err == nil {
			col.Order = i
		}
	}

	var formalName string
	if v, ok := m["TBCREATOR"]; ok {
		formalName = strings.TrimSpace(v)
	}
	formalName += "."
	if v, ok := m["TBNAME"]; ok {
		formalName += v
	}
	if v, ok := m["KEYSEQ"]; ok {
		i, err := strconv.Atoi(v)
		if err == nil {
			col.KeyType.Constraint = 1
			col.KeyType.Order = i
		}
	}
	if v, ok := m["REMARKS"]; ok {
		for _, str := range e.config.Remarks {
			switch str {
			case "Alias":
				col.Alias = v
			case "Description":
				col.Description = v
			}
		}
	}
	if v, ok := m["LABEL"]; ok {
		col.Alias = v
	}
	return col, formalName
}

// FindSchema は、スキーマの一覧を取得する。
func (e *ZDb2Extractor) FindSchema(ctx context.Context, dsn DataSourceName) ([]string, error) {
	db, err := sql.Open(Db2Driver, dsn.DSN())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
	    SELECT CREATOR
		FROM SYSIBM.SYSTABLES
		GROUP BY CREATOR`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []string{}
	for rows.Next() {
		var column string
		rows.Scan(&column)
		result = append(result, column)
	}
	return result, nil
}

func (e *ZDb2Extractor) SetConfig(config *Config) {
	e.config = config
}
