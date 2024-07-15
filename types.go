// Copyright © 2024 ROBON Inc. All rights reserved.
// This software is licensed under PolyForm Shield License 1.0.0
// https://polyformproject.org/licenses/shield/1.0.0/

package main

import (
	"fmt"
	"strings"
)

// Config は、このツールの設定情報です。
type Config struct {
	Hostname     string   `json:"hostname"`
	Database     string   `json:"database"`
	Port         int      `json:"port"`
	UserID       string   `json:"userid"`
	Password     string   `json:"password"`
	Lang         string   `json:"lang"`
	Remarks      []string `json:"remarks"`
	CSVFile      string   `json:"csvfile"`
	SystemSchema string   `json:"systemSchema"`
	TargetSchema []string `json:"targetSchema"`
}

// Db2DSN は、Config から DSN を作ります。
func (c *Config) Db2DSN() *Db2DSN {
	return &Db2DSN{
		Hostname: c.Hostname,
		Database: c.Database,
		Port:     c.Port,
		UserID:   c.UserID,
		Password: c.Password,
	}
}

func (c *Config) TargetSchemaInClause() string {
	return fmt.Sprintf("('%s')", strings.Join(c.TargetSchema, "', '"))
}

// Metadata は、テーブルのようなひとまとまりのデータに対するメタ情報です。
type Metadata struct {
	// Name は、メタデータ名です。
	Name string
	// Alias は、メタデータの別名です。論理名を想定しています。
	Alias string
	// FormalName は、メタデータの正式名（メタデータソース内で重複しない名前）です。
	// 手入力のメタデータの場合は Name=FormalName です
	FormalName string
	// Description は、説明です。
	Description string
	// MetaType は、Metadata の種別です。
	MetaType int
	// Lang は、ISO639-1 言語コードです。
	Lang string
	// Columns は、Metadata を構成する Column です。【可変長】
	Columns []Column
}

// MetaTypeName は、MetaType の文字列表現を返す
func (m Metadata) MetaTypeName() string {
	str := ""
	switch m.MetaType {
	case 1:
		str = "Table"
	case 2:
		str = "Model"
	case 3:
		str = "Stream"
	case 4:
		str = "File"
	}
	return str
}

// ToCSVString は、Metadata の CSV 表現を返す
func (m Metadata) ToCSVString() string {
	buf := strings.Builder{}
	// 20: メタデータ
	//   - Type*:       データタイプ
	//   - ID*:         メタデータID。IDを空にしてインポートしたときはメタデータを新規登録します。同じ名前のメタデータは新規登録できません。
	//   - Name:        メタデータ名
	//   - Alias:       メタデータの別名
	//   - Description: メタデータの説明。改行やカンマを含む場合は " で囲んでください。
	//   - Language:    メタデータの言語
	//     - ja:          日本語
	//     - en:          英語
	//   - MetaType:    メタデータタイプ
	//     - Table:       テーブル形式のメタデータ
	//     - Model:       モデル形式のメタデータ
	//     - Stream:      ストリーム形式のメタデータ
	//     - File:        ファイル形式のメタデータ
	buf.WriteString(fmt.Sprintf("20,,%s,%s,%s,%s,%s\n",
		m.FormalName,
		m.Alias,
		m.Description,
		m.Lang,
		m.MetaTypeName(),
	))
	for _, c := range m.Columns {
		// 30: メタデータのカラム
		//   - Type*:       データタイプ
		//   - ID*:         メタデータID。IDを空にしてインポートしたときはカラムを新規登録します。同じ名前のカラムは新規登録できません。
		//   - Name:        カラム名
		//   - Alias:       カラムの別名
		//   - Description: カラムの説明。改行やカンマを含む場合は " で囲んでください。
		//   - DataType:    カラムの型
		//   - ColumnType:  カラムの種類
		//     - Nullable:    任意
		//     - Required:    必須
		//     - Repeated:    複数
		//   - Constraint:  キーの制約
		//     - Primary:     主キー
		buf.WriteString(fmt.Sprintf("30,,%s,%s,%s,%s,%s,%s\n",
			c.Name,
			c.Alias,
			c.Description,
			c.Type,
			c.ModeName(),
			c.KeyType.ConstraintName(),
		))
	}
	buf.WriteString("\n")
	return buf.String()
}

// Column は、データ項目のメタ情報です。
type Column struct {
	// Name は、カラム名です。
	Name string
	// Alias は、カラムの別名です。論理名を想定しています。
	Alias string
	// Description は、説明です。
	Description string
	// Type は、Column のデータ型です。
	Type string
	// Mode は、Column の多重度の種別です。
	Mode int
	// Order は、DB に設定されたカラムの順番(1 スタート)
	Order int
	// KeyType は、カラムに設定されたキーのタイプ
	KeyType KeyType
}

// ModeName は、Mode の文字列表現を返す
func (c Column) ModeName() string {
	str := ""
	switch c.Mode {
	case 0:
		str = "Nullable"
	case 1:
		str = "Required"
	case 2:
		str = "Repeated"
	}
	return str
}

// KeyType は、カラムに設定されたキーのタイプ
type KeyType struct {
	// Constraint は、キーの制約(Primary, Unique など)
	Constraint int `json:"constraint" dynamodbav:"constraint"`
	// Order は、複合キーの場合の順序(1 スタート)
	Order int `json:"order" dynamodb:"order"`
}

// ConstraintName は、Constraint の文字列表現を返す
func (k KeyType) ConstraintName() string {
	str := ""
	switch k.Constraint {
	case 1:
		return "Primary"
	}
	return str
}
