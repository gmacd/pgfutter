package pgfutter

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"
)

func containsDelimiter(col string) bool {
	return strings.Contains(col, ";") || strings.Contains(col, ",") ||
		strings.Contains(col, "|") || strings.Contains(col, "\t") ||
		strings.Contains(col, "^") || strings.Contains(col, "~")
}

// Parse columns from first header row or from flags
func parseColumns(reader *csv.Reader, skipHeader bool, fields string) ([]string, error) {
	var err error
	var columns []string
	if fields != "" {
		columns = strings.Split(fields, ",")

		if skipHeader {
			reader.Read() //Force consume one row
		}
	} else {
		columns, err = reader.Read()
		if err != nil {
			return nil, err
		}
	}

	for _, col := range columns {
		if containsDelimiter(col) {
			return columns, errors.New("Please specify the correct delimiter with -d.\nHeader column contains a delimiter character: " + col)
		}
	}

	for i, col := range columns {
		columns[i] = postgresify(col)
	}

	return columns, nil
}

func copyCSVRows(i *Import, reader *csv.Reader, ignoreErrors bool, delimiter string, columns []string) (error, int, int) {
	success := 0
	failed := 0

	for {
		cols := make([]interface{}, len(columns))
		record, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			line := strings.Join(record, delimiter)
			failed++

			if ignoreErrors {
				os.Stderr.WriteString(string(line))
				continue
			} else {
				err = errors.New(fmt.Sprintf("%s: %s", err, line))
				return err, success, failed
			}
		}

		//Loop ensures we don't insert too many values and that
		//values are properly converted into empty interfaces
		for i, col := range record {
			cols[i] = col
		}

		err = i.AddRow(cols...)

		if err != nil {
			line := strings.Join(record, delimiter)
			failed++

			if ignoreErrors {
				os.Stderr.WriteString(string(line))
				continue
			} else {
				err = errors.New(fmt.Sprintf("%s: %s", err, line))
				return err, success, failed
			}
		}

		success++
	}

	return nil, success, failed
}

func importCsvFromFile(
	filename string,
	connStr string,
	schema string,
	tableName string,
	ignoreErrors bool,
	skipHeader bool,
	fields string,
	delimiter string) error {

	db, err := connect(connStr, schema)
	if err != nil {
		return err
	}
	defer db.Close()

	var reader io.Reader
	if filename != "" {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		reader = file
	} else {
		reader = os.Stdin
	}

	return ImportCSV(
		reader,
		db,
		schema,
		tableName,
		ignoreErrors,
		skipHeader,
		fields,
		delimiter)
}

func ImportCSV(
	reader io.Reader,
	db *sql.DB,
	schema string,
	tableName string,
	ignoreErrors bool,
	skipHeader bool,
	fields string,
	delimiter string) error {

	csvReader := csv.NewReader(reader)

	csvReader.Comma, _ = utf8.DecodeRuneInString(delimiter)
	csvReader.LazyQuotes = true

	columns, err := parseColumns(csvReader, skipHeader, fields)
	if err != nil {
		return fmt.Errorf("Couldn't parse columns: %v", err)
	}

	csvReader.FieldsPerRecord = len(columns)

	importer, err := NewCSVImport(db, schema, tableName, columns)
	if err != nil {
		return fmt.Errorf("Couldn't prepare CSV import: %v", err)
	}

	var success, failed int
	err, success, failed = copyCSVRows(importer, csvReader, ignoreErrors, delimiter, columns)

	if err != nil {
		lineNumber := success + failed
		if !skipHeader {
			lineNumber++
		}
		return fmt.Errorf("Error copying from CSV to table at line %d.  Error: %v", lineNumber, err)
	} else {
		fmt.Printf("%d rows imported into %s.%s", success, schema, tableName)

		if ignoreErrors && failed > 0 {
			fmt.Printf("%d rows could not be imported into %s.%s and have been written to stderr.", failed, schema, tableName)
		}

		return importer.Commit()
	}
}
