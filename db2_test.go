// Copyright © 2024 ROBON Inc. All rights reserved.
// This software is licensed under PolyForm Shield License 1.0.0
// https://polyformproject.org/licenses/shield/1.0.0/

package main

import (
	"context"
	"os"
	"testing"
)

func TestDb2Run(t *testing.T) {
	ctx := context.Background()
	config := &Config{
		Hostname:     "localhost",
		Database:     "SAMPLE",
		Port:         50000,
		UserID:       "db2inst1",
		Password:     "password",
		Lang:         "ja",
		Remarks:      []string{"Alias", "Description"},
		CSVFile:      "testdata/mashu.csv",
		SystemSchema: "SYSCAT",
		TargetSchema: []string{"DB2INST1"},
	}
	output, err := os.Create(config.CSVFile)
	if err != nil {
		t.Fatalf("os.Open() error :%s", err)
	}
	defer output.Close()

	extractor := GetExtractor(Db2Driver + "." + config.SystemSchema)
	extractor.SetConfig(config)
	err = extractor.Run(ctx, config.Db2DSN(), output)
	if err != nil {
		t.Fatalf("Run() error :%s", err)
	}
}

func TestDb2FindScehma(t *testing.T) {
	ctx := context.Background()
	config := &Config{
		Hostname:     "localhost",
		Database:     "SAMPLE",
		Port:         50000,
		UserID:       "db2inst1",
		Password:     "password",
		Lang:         "ja",
		Remarks:      []string{"Alias", "Description"},
		CSVFile:      "testdata/mashu.csv",
		SystemSchema: "SYSCAT",
		TargetSchema: []string{"DB2INST1"},
	}
	extractor := GetExtractor(Db2Driver + "." + config.SystemSchema)
	extractor.SetConfig(config)
	list, err := extractor.FindSchema(ctx, config.Db2DSN())
	if err != nil {
		t.Fatalf("Run() error :%s", err)
	}
	t.Logf("%#v", list)
}
