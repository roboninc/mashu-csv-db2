# Copyright © 2024 ROBON Inc. All rights reserved.
# This software is licensed under PolyForm Shield License 1.0.0
# https://polyformproject.org/licenses/shield/1.0.0/

.PHONY: build test clean

build:
	go build -o dest/mashu-csv-db2

test:
	go test -v -cover

clean:
	rm -f dest/mashu-csv-db2
	rm -f testdata/*.csv
