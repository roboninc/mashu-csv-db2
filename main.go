// Copyright © 2024 ROBON Inc. All rights reserved.
// This software is licensed under PolyForm Shield License 1.0.0
// https://polyformproject.org/licenses/shield/1.0.0/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/ibmdb/go_ibm_db"
)

// Db2Driver は、DB2 のドライバー名です。
const Db2Driver = "go_ibm_db"

func main() {
	ctx := context.Background()

	data, err := os.ReadFile("config.json")
	if err != nil {
		fmt.Printf("config.json read error (%#v)\n", err)
		os.Exit(-1)
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Printf("config.json unmarshal error (%#v)\n", err)
		os.Exit(-2)
	}

	extractor := GetExtractor(Db2Driver + "." + config.SystemSchema)
	extractor.SetConfig(&config)

	if len(config.TargetSchema) == 0 {
		list, err := extractor.FindSchema(ctx, config.Db2DSN())
		if err != nil {
			fmt.Printf("FindSchema error (%#v)\n", err)
			os.Exit(-3)
		}
		config.TargetSchema = list
		b, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			fmt.Printf("config.jcon marshal error (%#v)\n", err)
			os.Exit(-4)
		}
		os.WriteFile("config.json", b, 0666)
		fmt.Print("add targetSchema to config.json ;)\n")
	} else {
		output, err := os.Create(config.CSVFile)
		if err != nil {
			fmt.Printf("csvfile create error (%#v)\n", err)
			os.Exit(-5)
		}
		defer output.Close()

		err = extractor.Run(ctx, config.Db2DSN(), output)
		if err != nil {
			fmt.Printf("Run error (%#v)\n", err)
		}
		fmt.Printf("Let's import %s into Mashu (^^)b\n", config.CSVFile)
	}
}
