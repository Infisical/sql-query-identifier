# sql-query-identifier (Go)

This is a Go port of the [NPM sql-query-identifier library](https://github.com/coresql/sql-query-identifier). It identifies the type of each statement in a SQL query string, providing the start and end position of each statement.

## Features

- **Multi-Statement Support:** Parses a string containing multiple SQL statements separated by semicolons.
- **Dialect-Aware:** Supports multiple SQL dialects, including MySQL, PostgreSQL, MSSQL, and more.
- **Statement Identification:** Identifies a wide range of statement types (e.g., `SELECT`, `INSERT`, `CREATE TABLE`).
- **Execution Type Analysis:** Classifies statements by their behavior (`LISTING`, `MODIFICATION`).
- **Parameter Extraction:** Identifies positional (`?`, `$1`) and named (`:name`) parameters.
- **Strict & Non-Strict Modes:** Choose whether to error on unknown statement types or classify them as `UNKNOWN`.

## Installation

```sh
go get github.com/Infisical/sql-query-identifier
```

## Usage

The following example demonstrates how to parse a query string and print the identified statements as a JSON object.

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"

	sqlqueryidentifier "github.com/Infisical/sql-query-identifier"
)

func main() {
	query := `
		INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');
		SELECT * FROM Persons;
`

	// Use Dialect constants for type safety
	dialect := sqlqueryidentifier.DialectMySQL
	strict := false

	options := sqlqueryidentifier.IdentifyOptions{
		Dialect: &dialect,
		Strict:  &strict,
	}

	results, err := sqlqueryidentifier.Identify(query, options)
	if err != nil {
		log.Fatalf("Failed to identify query: %v", err)
	}

	// Marshal the results to JSON for clear output
	jsonOutput, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal results to JSON: %v", err)
	}

	fmt.Println(string(jsonOutput))
}
```

### API

`Identify(query string, options IdentifyOptions) ([]IdentifyResult, error)`

-   `query (string)`: The raw SQL string to be processed.
-   `options (IdentifyOptions)`: Configuration for the parser.
    -   `Strict (*bool)`: If `false`, will classify unknown statements as `UNKNOWN` instead of returning an error. Defaults to `true`.
    -   `Dialect (*Dialect)`: The SQL dialect to use for parsing. Defaults to `generic`.

### Supported Dialects

-   `mssql`
-   `sqlite`
-   `mysql`
-   `oracle`
-   `psql`
-   `bigquery`
-   `generic` (default)

## Supported Statement Types

#### Data Manipulation
- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `TRUNCATE`

#### Data Definition
- `CREATE_DATABASE`
- `CREATE_SCHEMA`
- `CREATE_TABLE`
- `CREATE_VIEW`
- `CREATE_TRIGGER`
- `CREATE_FUNCTION`
- `CREATE_INDEX`
- `CREATE_PROCEDURE`
- `DROP_DATABASE`
- `DROP_SCHEMA`
- `DROP_TABLE`
- `DROP_VIEW`
- `DROP_TRIGGER`
- `DROP_FUNCTION`
- `DROP_INDEX`
- `DROP_PROCEDURE`
- `ALTER_DATABASE`
- `ALTER_SCHEMA`
- `ALTER_TABLE`
- `ALTER_VIEW`
- `ALTER_TRIGGER`
- `ALTER_FUNCTION`
- `ALTER_INDEX`
- `ALTER_PROCEDURE`

#### SHOW (MySQL and generic dialects)
- `SHOW_BINARY`
- `SHOW_BINLOG`
- `SHOW_CHARACTER`
- `SHOW_COLLATION`
- `SHOW_COLUMNS`
- `SHOW_CREATE`
- `SHOW_DATABASES`
- `SHOW_ENGINE`
- `SHOW_ENGINES`
- `SHOW_ERRORS`
- `SHOW_EVENTS`
- `SHOW_FUNCTION`
- `SHOW_GRANTS`
- `SHOW_INDEX`
- `SHOW_MASTER`
- `SHOW_OPEN`
- `SHOW_PLUGINS`
- `SHOW_PRIVILEGES`
- `SHOW_PROCEDURE`
- `SHOW_PROCESSLIST`
- `SHOW_PROFILE`
- `SHOW_PROFILES`
- `SHOW_RELAYLOG`
- `SHOW_REPLICAS`
- `SHOW_SLAVE`
- `SHOW_REPLICA`
- `SHOW_STATUS`
- `SHOW_TABLE`
- `SHOW_TABLES`
- `SHOW_TRIGGERS`
- `SHOW_VARIABLES`
- `SHOW_WARNINGS`

#### Other
- `ANON_BLOCK` (BigQuery and Oracle dialects only)
- `UNKNOWN` (only available if strict mode is disabled)

## Execution Types

Execution types classify the behavior of a query.

-   `LISTING`: The query lists or retrieves data.
-   `MODIFICATION`: The query modifies the database structure or data.
-   `INFORMATION`: The query shows information, such as profiling data.
-   `ANON_BLOCK`: The query is an anonymous block which may contain multiple statements.
-   `UNKNOWN`: The query type could not be determined (only available if strict mode is disabled).

## How It Works

This library uses AST and parser techniques to identify the SQL query type. It does not validate the entire query; instead, it validates only the required tokens to identify the statement type.

The identification process is:
1.  **Tokenizing:** The input string is broken down into tokens (keywords, strings, comments, etc.).
2.  **Parsing:** The stream of tokens is parsed to identify the statement boundaries and types.
    -   Comments and string contents are ignored to prevent false positives.
    -   Keywords are expected at the beginning of a statement.
    -   Semicolons are used to identify the end of a statement.

Because the library does not perform a full SQL validation, it is recommended to use it on queries that have already been successfully executed by a SQL client.

## License

This project is licensed under the MIT License.
