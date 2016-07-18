package pgfutter

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/kennygrant/sanitize"
)

//tries to create the schema and ignores failures to do so.
//versions after Postgres 9.3 support the "IF NOT EXISTS" sql syntax
func tryCreateSchema(db *sql.DB, importSchema string) {
	createSchema, err := db.Prepare(fmt.Sprintf("CREATE SCHEMA %s", importSchema))

	if err == nil {
		createSchema.Exec()
	}
}

//setup a database connection and create the import schema
func connect(connStr string, importSchema string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}

	tryCreateSchema(db, importSchema)
	return db, nil
}

//Makes sure that a string is a valid PostgreSQL identifier
func postgresify(identifier string) string {
	str := sanitize.BaseName(identifier)
	str = strings.ToLower(identifier)
	str = strings.TrimSpace(str)

	replacements := map[string]string{
		" ": "_",
		"/": "_",
		".": "_",
		":": "_",
		";": "_",
		"|": "_",
		"-": "_",
		",": "_",
		"#": "_",

		"[":  "",
		"]":  "",
		"{":  "",
		"}":  "",
		"(":  "",
		")":  "",
		"?":  "",
		"!":  "",
		"$":  "",
		"%":  "",
		"*":  "",
		"\"": "",
	}
	for oldString, newString := range replacements {
		str = strings.Replace(str, oldString, newString, -1)
	}

	if len(str) == 0 {
		str = fmt.Sprintf("_col%d", rand.Intn(10000))
	} else {
		firstLetter := string(str[0])
		if _, err := strconv.Atoi(firstLetter); err == nil {
			str = "_" + str
		}
	}

	return str
}

//build sql connection string from arguments
func ParseConnStr(user, dbname, password, host, port string, useSsl bool) string {
	otherParams := "sslmode=disable connect_timeout=5"
	if useSsl {
		otherParams = "sslmode=require connect_timeout=5"
	}
	return fmt.Sprintf(
		"user=%s dbname=%s password='%s' host=%s port=%s %s",
		user,
		dbname,
		password,
		host,
		port,
		otherParams,
	)
}

//create table with a single JSON or JSONB column data
func createJSONTable(db *sql.DB, schema string, tableName string, column string, dataType string) (*sql.Stmt, error) {
	fullyQualifiedTable := fmt.Sprintf("%s.%s", schema, tableName)
	tableSchema := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s %s)", fullyQualifiedTable, column, dataType)

	statement, err := db.Prepare(tableSchema)
	return statement, fmt.Errorf("Couldn't create table with command: %v  Error: %v", tableSchema, err)
}

//create table with TEXT columns
func createTable(db *sql.DB, schema string, tableName string, columns []string) (*sql.Stmt, error) {
	columnTypes := make([]string, len(columns))
	for i, col := range columns {
		columnTypes[i] = fmt.Sprintf("%s TEXT", col)
	}
	columnDefinitions := strings.Join(columnTypes, ", ")
	fullyQualifiedTable := fmt.Sprintf("%s.%s", schema, tableName)
	tableSchema := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", fullyQualifiedTable, columnDefinitions)

	if statement, err := db.Prepare(tableSchema); err != nil {
		return nil, fmt.Errorf("Couldn't create table with command: %v  Error: %v", tableSchema, err)
	} else {
		return statement, nil
	}
}
