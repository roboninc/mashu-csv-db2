// Copyright © 2024 ROBON Inc. All rights reserved.
// This software is licensed under PolyForm Shield License 1.0.0
// https://polyformproject.org/licenses/shield/1.0.0/

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// ColumnList は、対象テーブルの対象カラム名のリストを取得します。
func ColumnList(ctx context.Context, db *sql.DB, query string) ([]string, error) {
	rows, err := db.QueryContext(ctx, query)
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

// Query は、バージョン間でのカラム数の差異を動的に解決するクエリ
type Query struct {
	row  *Row
	stmt string
}

// NewQuery は、指定されたカラムリストと FROM 句以下の SELECT 文に対応する Query を返します。
func NewQuery(columns []string, stmt string) *Query {
	return &Query{
		row:  NewRow(columns),
		stmt: stmt,
	}
}

// Stmt は、SELECT 文を生成します。
func (q *Query) Stmt() string {
	return fmt.Sprintf("SELECT %s %s", q.row.Names(), q.stmt)
}

// Exec は、SELECT 文を DB に送ります。
func (q *Query) Exec(ctx context.Context, db *sql.DB, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(ctx, q.Stmt(), args...)
}

// Scan は、結果行を指定したカラム名をキーとする map として返します。
func (q *Query) Scan(rows *sql.Rows) (map[string]string, error) {
	err := rows.Scan(q.row.pointers...)
	if err != nil {
		return nil, err
	}
	return q.row.Map(), nil
}

// Row は、Query 対象のカラム名と値を保持します。
type Row struct {
	names    []string
	values   []ColumnValue
	pointers []interface{}
}

// NewRow は、指定されたカラム名リストに対応する Row を作ります。
func NewRow(names []string) *Row {
	values := make([]ColumnValue, len(names))
	pointers := make([]interface{}, len(names))
	for i := 0; i < len(values); i++ {
		pointers[i] = &values[i]
	}
	return &Row{
		names:    names,
		values:   values,
		pointers: pointers,
	}
}

// Names は、Query で使用するカラム名のカンマ区切り文字列を返します。
func (r *Row) Names() string {
	return strings.Join(r.names, ",")
}

// Values は、Scan で使用するカラム値の Scanner 配列を返します。
func (r *Row) Values() []interface{} {
	return r.pointers
}

// Map は、Query 結果から Tag を構成するための map を返します。
func (r *Row) Map() map[string]string {
	m := make(map[string]string, len(r.names))
	for i, name := range r.names {
		v := string(r.values[i])
		if v != "" {
			m[name] = v
		}
	}
	return m
}

// ColumnValue は、NULL を空文字列として Scan 可能な文字列
type ColumnValue string

// Scan は、sql.Scanner インターフェースの実装です。
// sql.NullString 実装を流用しています。
func (v *ColumnValue) Scan(value interface{}) error {
	if v == nil {
		return errors.New("destination pointer is nil")
	}
	if value == nil {
		*v = ColumnValue("")
		return nil
	}

	switch src := value.(type) {
	case string:
		*v = ColumnValue(src)
		return nil
	case []byte:
		*v = ColumnValue(string(src))
		return nil
	case time.Time:
		*v = ColumnValue(src.Format(time.RFC3339Nano))
		return nil
	}

	sv := reflect.ValueOf(value)
	switch sv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		*v = ColumnValue(strconv.FormatInt(sv.Int(), 10))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		*v = ColumnValue(strconv.FormatUint(sv.Uint(), 10))
		return nil
	case reflect.Float64:
		*v = ColumnValue(strconv.FormatFloat(sv.Float(), 'g', -1, 64))
		return nil
	case reflect.Float32:
		*v = ColumnValue(strconv.FormatFloat(sv.Float(), 'g', -1, 32))
		return nil
	case reflect.Bool:
		*v = ColumnValue(strconv.FormatBool(sv.Bool()))
		return nil
	}
	*v = ColumnValue(fmt.Sprintf("%v", value))
	return nil
}
