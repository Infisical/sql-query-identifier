package sqlqueryidentifier

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// merges consecutive 'unknown', 'whitespace', and 'string' tokens
// into a single 'unknown' token
func aggregateUnknownTokens(tokens []Token) []Token {
	if len(tokens) == 0 {
		return []Token{}
	}

	var result []Token

	for i, token := range tokens {
		if len(result) == 0 {
			result = append(result, token)
			continue
		}

		prev := &result[len(result)-1]
		var next *Token
		if i+1 < len(tokens) {
			next = &tokens[i+1]
		}

		isCurrUnknown := token.Type == "unknown"
		isCurrWhitespace := token.Type == "whitespace"
		isCurrString := token.Type == "string"
		isPrevUnknown := prev.Type == "unknown"
		isNextUnknown := next != nil && next.Type == "unknown"

		isCurrWhitespaceAfterUnknown := isCurrWhitespace && isPrevUnknown
		isCurrWhitespaceBeforeUnknown := isCurrWhitespace && isNextUnknown
		isCurrStringAfterUnknown := isCurrString && isPrevUnknown
		isCurrStringBeforeUnknown := isCurrString && isNextUnknown

		isKnowTokenBeforeUnknown :=
			(isCurrWhitespaceBeforeUnknown || isCurrStringBeforeUnknown) && !isPrevUnknown

		isNewToken :=
			isKnowTokenBeforeUnknown ||
				(!isCurrWhitespaceAfterUnknown &&
					!isCurrStringAfterUnknown &&
					(!isCurrUnknown || !isPrevUnknown))

		if isNewToken {
			newToken := token
			if isCurrWhitespaceBeforeUnknown {
				newToken.Type = "unknown"
			}
			result = append(result, newToken)
			continue
		}

		prev.End = token.End
		prev.Value += token.Value
	}

	return result
}

func TestParse(t *testing.T) {
	assertResult := func(t *testing.T, actual, expected *ParseResult) {
		t.Helper()
		actual.Tokens = nil

		if !reflect.DeepEqual(expected.Body, actual.Body) {
			t.Errorf("Expected body %#v, but got %#v", expected.Body, actual.Body)
		}
		if expected.Type != actual.Type {
			t.Errorf("Expected type %s, but got %s", expected.Type, actual.Type)
		}
		if expected.Start != actual.Start {
			t.Errorf("Expected start %d, but got %d", expected.Start, actual.Start)
		}
		if expected.End != actual.End {
			t.Errorf("Expected end %d, but got %d", expected.End, actual.End)
		}
	}

	t.Run("Single Statements", func(t *testing.T) {
		t.Run("given is a not recognized statement", func(t *testing.T) {
			t.Run("should throw an error including the unknown statement", func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("The code did not panic but was expected to")
					} else {
						err, ok := r.(string)
						if !ok {
							t.Errorf("Expected panic with string, got %T", r)
							return
						}
						expectedError := `Invalid statement parser "LIST"`
						if err != expectedError {
							t.Errorf("Expected panic with message %q, got %q", expectedError, err)
						}
					}
				}()
				Parse("LIST * FROM Persons", true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
			})

			t.Run("with strict disabled", func(t *testing.T) {
				t.Run("should parse if first token is unknown", func(t *testing.T) {
					actual := Parse("LIST * FROM foo", false, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
					expected := &ParseResult{
						Type:  "QUERY",
						Start: 0,
						End:   14,
						Body: []ConcreteStatement{
							{
								Type:          StatementUnknown,
								ExecutionType: ExecutionUnknown,
								Start:         0,
								End:           14,
								Tables:        []string{},
								Parameters:    []string{},
							},
						},
					}
					assertResult(t, actual, expected)
				})

				t.Run("should parse if first token is invalid keyword", func(t *testing.T) {
					actual := Parse("AS bar LEFT JOIN foo", false, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
					expected := &ParseResult{
						Type:  "QUERY",
						Start: 0,
						End:   19,
						Body: []ConcreteStatement{
							{
								Type:          StatementUnknown,
								ExecutionType: ExecutionUnknown,
								Start:         0,
								End:           19,
								Tables:        []string{},
								Parameters:    []string{},
							},
						},
					}
					assertResult(t, actual, expected)
				})
			})
		})

		t.Run("given queries with a single statement", func(t *testing.T) {
			t.Run("should parse \"SELECT\" statement", func(t *testing.T) {
				actual := Parse("SELECT * FROM Persons", true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   20,
					Body: []ConcreteStatement{
						{
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Start:         0,
							End:           20,
							Tables:        []string{"Persons"},
							Parameters:    []string{},
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"select\" statement", func(t *testing.T) {
				actual := Parse("select * FROM Persons", true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   20,
					Body: []ConcreteStatement{
						{
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Start:         0,
							End:           20,
							Tables:        []string{"Persons"},
							Parameters:    []string{},
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"CREATE TABLE\" statement", func(t *testing.T) {
				actual := Parse("CREATE TABLE Persons (PersonID int, Name varchar(255));", true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   54,
					Body: []ConcreteStatement{
						{
							Type:          StatementCreateTable,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           54,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			for _, tempType := range []string{"TEMP", "TEMPORARY"} {
				t.Run(fmt.Sprintf(`it should parse "CREATE %s TABLE" statement`, tempType), func(t *testing.T) {
					for _, dialectInfo := range []struct {
						name    string
						dialect Dialect
					}{
						{"psql", DialectPSQL},
						{"sqlite", DialectSQLite},
					} {
						t.Run(fmt.Sprintf("for %s", dialectInfo.name), func(t *testing.T) {
							query := fmt.Sprintf("CREATE %s TABLE Persons (PersonID int, Name varchar(255));", tempType)
							actual := Parse(query, true, dialectInfo.dialect, false, DefaultParamTypesFor(DialectGeneric))
							expected := &ParseResult{
								Type:  "QUERY",
								Start: 0,
								End:   54 + len(tempType) + 1,
								Body: []ConcreteStatement{
									{
										Type:          StatementCreateTable,
										ExecutionType: ExecutionModification,
										Start:         0,
										End:           54 + len(tempType) + 1,
										Tables:        []string{},
										Parameters:    []string{},
										EndStatement:  func() *string { s := ";"; return &s }(),
									},
								},
							}
							assertResult(t, actual, expected)
						})
					}
				})
			}

			t.Run("should parse \"CREATE VIRTUAL TABLE\" statement for sqlite", func(t *testing.T) {
				query := "CREATE VIRTUAL TABLE Persons (PersonID int, Name varchar(255));"
				actual := Parse(query, true, DialectSQLite, false, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   62,
					Body: []ConcreteStatement{
						{
							Type:          StatementCreateTable,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           62,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"CREATE DATABASE\" statement", func(t *testing.T) {
				query := "CREATE DATABASE Profile;"
				actual := Parse(query, true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   23,
					Body: []ConcreteStatement{
						{
							Type:          StatementCreateDatabase,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           23,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"DROP TABLE\" statement", func(t *testing.T) {
				query := "DROP TABLE Persons;"
				actual := Parse(query, true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   18,
					Body: []ConcreteStatement{
						{
							Type:          StatementDropTable,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           18,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"DROP DATABASE\" statement", func(t *testing.T) {
				query := "DROP DATABASE Profile;"
				actual := Parse(query, true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   21,
					Body: []ConcreteStatement{
						{
							Type:          StatementDropDatabase,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           21,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"INSERT\" statement", func(t *testing.T) {
				query := "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');"
				actual := Parse(query, true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   55,
					Body: []ConcreteStatement{
						{
							Type:          StatementInsert,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           55,
							Tables:        []string{"Persons"},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"UPDATE\" statement", func(t *testing.T) {
				query := "UPDATE Persons SET Name = 'John' WHERE PersonID = 1;"
				actual := Parse(query, true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   51,
					Body: []ConcreteStatement{
						{
							Type:          StatementUpdate,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           51,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"DELETE\" statement", func(t *testing.T) {
				query := "DELETE FROM Persons WHERE PersonID = 1;"
				actual := Parse(query, true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   38,
					Body: []ConcreteStatement{
						{
							Type:          StatementDelete,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           38,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("should parse \"TRUNCATE\" statement", func(t *testing.T) {
				query := "TRUNCATE TABLE Persons;"
				actual := Parse(query, true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
				expected := &ParseResult{
					Type:  "QUERY",
					Start: 0,
					End:   22,
					Body: []ConcreteStatement{
						{
							Type:          StatementTruncate,
							ExecutionType: ExecutionModification,
							Start:         0,
							End:           22,
							Tables:        []string{},
							Parameters:    []string{},
							EndStatement:  func() *string { s := ";"; return &s }(),
						},
					},
				}
				assertResult(t, actual, expected)
			})

			t.Run("with parameters", func(t *testing.T) {
				t.Run("should extract the parameters", func(t *testing.T) {
					query := "select x from a where x = ?"
					actual := Parse(query, true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
					actual.Tokens = aggregateUnknownTokens(actual.Tokens)

					expectedTokens := []Token{
						{
							Type:  "keyword",
							Value: "select",
							Start: 0,
							End:   5,
						},
						{
							Type:  "unknown",
							Value: " x from a where x = ",
							Start: 6,
							End:   25,
						},
						{
							Type:  "parameter",
							Value: "?",
							Start: 26,
							End:   26,
						},
					}

					if !reflect.DeepEqual(actual.Tokens, expectedTokens) {
						t.Errorf("Expected tokens %#v, but got %#v", expectedTokens, actual.Tokens)
					}

					expectedParameters := []string{"?"}
					if !reflect.DeepEqual(actual.Body[0].Parameters, expectedParameters) {
						t.Errorf("Expected parameters %#v, but got %#v", expectedParameters, actual.Body[0].Parameters)
					}
				})

				t.Run("should extract PSQL parameters", func(t *testing.T) {
					query := "select x from a where x = $1"
					actual := Parse(query, true, DialectPSQL, true, DefaultParamTypesFor(DialectPSQL))
					actual.Tokens = aggregateUnknownTokens(actual.Tokens)

					expectedTokens := []Token{
						{
							Type:  "keyword",
							Value: "select",
							Start: 0,
							End:   5,
						},
						{
							Type:  "unknown",
							Value: " x from a where x = ",
							Start: 6,
							End:   25,
						},
						{
							Type:  "parameter",
							Value: "$1",
							Start: 26,
							End:   27,
						},
					}

					if !reflect.DeepEqual(actual.Tokens, expectedTokens) {
						t.Errorf("Expected tokens %#v, but got %#v", expectedTokens, actual.Tokens)
					}

					expectedParameters := []string{"$1"}
					if !reflect.DeepEqual(actual.Body[0].Parameters, expectedParameters) {
						t.Errorf("Expected parameters %#v, but got %#v", expectedParameters, actual.Body[0].Parameters)
					}
				})

				t.Run("should extract multiple PSQL parameters", func(t *testing.T) {
					query := "select x from a where x = $1 and y = $2"
					actual := Parse(query, true, DialectPSQL, true, DefaultParamTypesFor(DialectPSQL))
					actual.Tokens = aggregateUnknownTokens(actual.Tokens)

					expectedTokens := []Token{
						{
							Type:  "keyword",
							Value: "select",
							Start: 0,
							End:   5,
						},
						{
							Type:  "unknown",
							Value: " x from a where x = ",
							Start: 6,
							End:   25,
						},
						{
							Type:  "parameter",
							Value: "$1",
							Start: 26,
							End:   27,
						},
						{
							Type:  "unknown",
							Value: " and y = ",
							Start: 28,
							End:   36,
						},
						{
							Type:  "parameter",
							Value: "$2",
							Start: 37,
							End:   38,
						},
					}

					if !reflect.DeepEqual(actual.Tokens, expectedTokens) {
						t.Errorf("Expected tokens %#v, but got %#v", expectedTokens, actual.Tokens)
					}

					expectedParameters := []string{"$1", "$2"}
					if !reflect.DeepEqual(actual.Body[0].Parameters, expectedParameters) {
						t.Errorf("Expected parameters %#v, but got %#v", expectedParameters, actual.Body[0].Parameters)
					}
				})

				t.Run("should extract mssql parameters", func(t *testing.T) {
					query := "select x from a where x = :foo"
					actual := Parse(query, true, DialectMSSQL, true, DefaultParamTypesFor(DialectMSSQL))
					actual.Tokens = aggregateUnknownTokens(actual.Tokens)

					expectedTokens := []Token{
						{
							Type:  "keyword",
							Value: "select",
							Start: 0,
							End:   5,
						},
						{
							Type:  "unknown",
							Value: " x from a where x = ",
							Start: 6,
							End:   25,
						},
						{
							Type:  "parameter",
							Value: ":foo",
							Start: 26,
							End:   29,
						},
					}

					if !reflect.DeepEqual(actual.Tokens, expectedTokens) {
						t.Errorf("Expected tokens %#v, but got %#v", expectedTokens, actual.Tokens)
					}

					expectedParameters := []string{":foo"}
					if !reflect.DeepEqual(actual.Body[0].Parameters, expectedParameters) {
						t.Errorf("Expected parameters %#v, but got %#v", expectedParameters, actual.Body[0].Parameters)
					}
				})

				t.Run("should not identify params in a comment", func(t *testing.T) {
					query := "-- comment ?"
					actual := Parse(query, true, DialectGeneric, false, DefaultParamTypesFor(DialectGeneric))
					expected := &ParseResult{
						Type:  "QUERY",
						Start: 0,
						End:   11,
						Body:  []ConcreteStatement{},
					}
					assertResult(t, actual, expected)
				})

				t.Run("should not identify params in a string", func(t *testing.T) {
					query := "select '$1'"
					actual := Parse(query, true, DialectPSQL, true, DefaultParamTypesFor(DialectPSQL))

					expectedTokens := []Token{
						{
							Type:  "keyword",
							Value: "select",
							Start: 0,
							End:   5,
						},
						{
							Type:  "whitespace",
							Value: " ",
							Start: 6,
							End:   6,
						},
						{
							Type:  "string",
							Value: "'$1'",
							Start: 7,
							End:   10,
						},
					}

					if !reflect.DeepEqual(actual.Tokens, expectedTokens) {
						t.Errorf("Expected tokens %#v, but got %#v", expectedTokens, actual.Tokens)
					}

				})

				t.Run("should extract multiple mssql parameters", func(t *testing.T) {
					query := "select x from a where x = :foo and y = :bar"
					actual := Parse(query, true, DialectMSSQL, true, DefaultParamTypesFor(DialectMSSQL))
					actual.Tokens = aggregateUnknownTokens(actual.Tokens)

					expectedTokens := []Token{
						{
							Type:  "keyword",
							Value: "select",
							Start: 0,
							End:   5,
						},
						{
							Type:  "unknown",
							Value: " x from a where x = ",
							Start: 6,
							End:   25,
						},
						{
							Type:  "parameter",
							Value: ":foo",
							Start: 26,
							End:   29,
						},
						{
							Type:  "unknown",
							Value: " and y = ",
							Start: 30,
							End:   38,
						},
						{
							Type:  "parameter",
							Value: ":bar",
							Start: 39,
							End:   42,
						},
					}

					if !reflect.DeepEqual(actual.Tokens, expectedTokens) {
						t.Errorf("Expected tokens %#v, but got %#v", expectedTokens, actual.Tokens)
					}

					expectedParameters := []string{":foo", ":bar"}
					if !reflect.DeepEqual(actual.Body[0].Parameters, expectedParameters) {
						t.Errorf("Expected parameters %#v, but got %#v", expectedParameters, actual.Body[0].Parameters)
					}
				})
			})
		})
	})

	t.Run("Multiple Statements", func(t *testing.T) {
		t.Run("should parse a query with different statements in a single line", func(t *testing.T) {
			query := "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');SELECT * FROM Persons"
			actual := Parse(query, true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
			expected := &ParseResult{
				Type:  "QUERY",
				Start: 0,
				End:   76,
				Body: []ConcreteStatement{
					{
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Start:         0,
						End:           55,
						EndStatement:  func() *string { s := ";"; return &s }(),
						Parameters:    []string{},
						Tables:        []string{"Persons"},
					},
					{
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Start:         56,
						End:           76,
						Parameters:    []string{},
						Tables:        []string{"Persons"},
					},
				},
			}
			assertResult(t, actual, expected)
		})

		t.Run("should identify a query with different statements in multiple lines", func(t *testing.T) {
			query := "\n        INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');\n        SELECT * FROM Persons';\n      "
			actual := Parse(query, true, DialectGeneric, true, DefaultParamTypesFor(DialectGeneric))
			expected := &ParseResult{
				Type:  "QUERY",
				Start: 0,
				End:   103,
				Body: []ConcreteStatement{
					{
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Start:         9,
						End:           64,
						EndStatement:  func() *string { s := ";"; return &s }(),
						Parameters:    []string{},
						Tables:        []string{"Persons"},
					},
					{
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Start:         74,
						End:           103,
						Parameters:    []string{},
						Tables:        []string{"Persons"},
					},
				},
			}
			assertResult(t, actual, expected)
		})
	})

	t.Run("Parser for bigquery", func(t *testing.T) {
		t.Run("control structures", func(t *testing.T) {
			sqls := []string{
				`CASE
								WHEN
										EXISTS(SELECT 1 FROM schema.products_a WHERE product_id = target_product_id)
										THEN SELECT 'found product in products_a table';
								WHEN
										EXISTS(SELECT 1 FROM schema.products_b WHERE product_id = target_product_id)
										THEN SELECT 'found product in products_b table';
								ELSE
										SELECT 'did not find product';
						END CASE;`,
				`IF EXISTS(SELECT 1 FROM schema.products
								WHERE product_id = target_product_id) THEN
								SELECT CONCAT('found product ', CAST(target_product_id AS STRING));
								ELSEIF EXISTS(SELECT 1 FROM schema.more_products
																WHERE product_id = target_product_id) THEN
								SELECT CONCAT('found product from more_products table',
								CAST(target_product_id AS STRING));
								ELSE
								SELECT CONCAT('did not find product ', CAST(target_product_id AS STRING));
						END IF;`,
				`LOOP
								SET x = x + 1;
								IF x >= 10 THEN
										LEAVE;
								END IF;
						END LOOP;`,
				`REPEAT
								SET x = x + 1;
								SELECT x;
								UNTIL x >= 3
						END REPEAT;`,
				`WHILE x < 0 DO
										SET x = x + 1;
										SELECT x;
						END WHILE;`,
				`FOR record IN
								(SELECT word, word_count
								FROM bigquery-public-data.samples.shakespeare
								LIMIT 5)
						DO
								SELECT record.word, record.word_count;
						END FOR;`,
			}

			for _, sql := range sqls {
				firstWord := strings.Split(strings.TrimSpace(sql), " ")[0]
				t.Run(fmt.Sprintf("parses %s structure", firstWord), func(t *testing.T) {
					query := sql + "\nSELECT 1;"
					result := Parse(query, false, DialectBigQuery, false, DefaultParamTypesFor(DialectGeneric))
					if len(result.Body) != 2 {
						t.Fatalf("Expected 2 statements, but got %d", len(result.Body))
					}
					if result.Body[0].Type != StatementUnknown {
						t.Errorf("Expected first statement to be UNKNOWN, but got %s", result.Body[0].Type)
					}
					if result.Body[1].Type != StatementSelect {
						t.Errorf("Expected second statement to be SELECT, but got %s", result.Body[1].Type)
					}
					parsedSQL := query[result.Body[0].Start : result.Body[0].End+1]
					if strings.TrimSpace(parsedSQL) != strings.TrimSpace(sql) {
						t.Errorf("Parsed SQL does not match original SQL.\nExpected: %q\nGot: %q", sql, parsedSQL)
					}
				})
			}
		})

		t.Run("parses BEGIN statement as ANON_BLOCK", func(t *testing.T) {
			result := Parse(`BEGIN SELECT 1; END; SELECT 1;`, false, DialectBigQuery, false, DefaultParamTypesFor(DialectGeneric))
			if len(result.Body) != 2 {
				t.Fatalf("Expected 2 statements, got %d", len(result.Body))
			}
			if result.Body[0].Type != StatementAnonBlock {
				t.Errorf("Expected first statement to be ANON_BLOCK, got %s", result.Body[0].Type)
			}
			if result.Body[1].Type != StatementSelect {
				t.Errorf("Expected second statement to be SELECT, got %s", result.Body[1].Type)
			}
		})

		t.Run("parses BEGIN TRANSACTION as UNKNOWN", func(t *testing.T) {
			result := Parse(`BEGIN TRANSACTION; SELECT 1; COMMIT;`, false, DialectBigQuery, false, DefaultParamTypesFor(DialectGeneric))
			if len(result.Body) != 3 {
				t.Fatalf("Expected 3 statements, got %d", len(result.Body))
			}
			if result.Body[0].Type != StatementUnknown {
				t.Errorf("Expected first statement to be UNKNOWN, got %s", result.Body[0].Type)
			}
			if result.Body[1].Type != StatementSelect {
				t.Errorf("Expected second statement to be SELECT, got %s", result.Body[1].Type)
			}
			if result.Body[2].Type != StatementUnknown {
				t.Errorf("Expected third statement to be UNKNOWN, got %s", result.Body[2].Type)
			}
		})
	})

	t.Run("Parser for oracle", func(t *testing.T) {
		t.Run("Given a CASE Statement", func(t *testing.T) {
			t.Run("should parse a simple case statement", func(t *testing.T) {
				sql := `SELECT CASE WHEN a = 'a' THEN 'foo' ELSE 'bar' END CASE from table;`
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 1 {
					t.Errorf("Expected 1 statement, got %d", len(result.Body))
				}
			})
		})

		t.Run("given an anonymous block with an OUT param", func(t *testing.T) {
			t.Run("should treat a simple block as a single query", func(t *testing.T) {
				sql := "BEGIN\n          SELECT\n            cols.column_name INTO :variable\n          FROM\n            example_table;\n        END"
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 1 {
					t.Fatalf("Expected 1 statement, got %d", len(result.Body))
				}
				if result.Body[0].Type != StatementAnonBlock {
					t.Errorf("Expected statement type to be ANON_BLOCK, got %s", result.Body[0].Type)
				}
				if result.Body[0].Start != 0 {
					t.Errorf("Expected start to be 0, got %d", result.Body[0].Start)
				}
				if result.Body[0].End != 119 {
					t.Errorf("Expected end to be 119, got %d", result.Body[0].End)
				}
			})

			t.Run("should easily identify two blocks", func(t *testing.T) {
				sql := "BEGIN\n          SELECT\n            cols.column_name INTO :variable\n          FROM\n            example_table;\n        END;\n\n        BEGIN\n          SELECT\n            cols.column_name INTO :variable\n          FROM\n            example_table;\n        END\n        "
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))

				if len(result.Body) != 2 {
					t.Fatalf("Expected 2 statements, got %d", len(result.Body))
				}
				if result.Body[0].Type != StatementAnonBlock {
					t.Errorf("Expected statement 1 type to be ANON_BLOCK, got %s", result.Body[0].Type)
				}
				if result.Body[0].Start != 0 {
					t.Errorf("Expected statement 1 start to be 0, got %d", result.Body[0].Start)
				}
				if result.Body[0].End != 120 {
					t.Errorf("Expected statement 1 end to be 120, got %d", result.Body[0].End)
				}
				if result.Body[1].Type != StatementAnonBlock {
					t.Errorf("Expected statement 2 type to be ANON_BLOCK, got %s", result.Body[1].Type)
				}
				if result.Body[1].Start != 131 {
					t.Errorf("Expected statement 2 start to be 131, got %d", result.Body[1].Start)
				}
				if result.Body[1].End != 259 {
					t.Errorf("Expected statement 2 end to be 259, got %d", result.Body[1].End)
				}
			})

			t.Run("should identify a block query and a normal query together", func(t *testing.T) {
				sql := "BEGIN\n      SELECT\n      cols.column_name INTO :variable\n      FROM\n      example_table;\n      END;\n\n      select * from another_thing\n      "
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 2 {
					t.Fatalf("Expected 2 statements, got %d", len(result.Body))
				}
				if result.Body[0].Type != StatementAnonBlock {
					t.Errorf("Expected statement 1 type to be ANON_BLOCK, got %s", result.Body[0].Type)
				}
				if result.Body[0].Start != 0 {
					t.Errorf("Expected statement 1 start to be 0, got %d", result.Body[0].Start)
				}
				if result.Body[0].End != 98 {
					t.Errorf("Expected statement 1 end to be 98, got %d", result.Body[0].End)
				}
				if result.Body[1].Type != StatementSelect {
					t.Errorf("Expected statement 2 type to be SELECT, got %s", result.Body[1].Type)
				}
				if result.Body[1].Start != 107 {
					t.Errorf("Expected statement 2 start to be 107, got %d", result.Body[1].Start)
				}
			})
		})

		t.Run("given an anonymous block with a variable", func(t *testing.T) {
			t.Run("should treat a block with DECLARE and another query as two separate queries", func(t *testing.T) {
				sql := "DECLARE\n          PK_NAME VARCHAR(200);\n        BEGIN\n          SELECT\n            cols.column_name INTO PK_NAME\n          FROM\n            example_table;\n        END;\n\n        select * from foo;\n      "
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 2 {
					t.Fatalf("Expected 2 statements, got %d", len(result.Body))
				}
				if result.Body[0].Type != StatementAnonBlock {
					t.Errorf("Expected statement 1 type to be ANON_BLOCK, got %s", result.Body[0].Type)
				}
				if result.Body[0].Start != 0 {
					t.Errorf("Expected statement 1 start to be 0, got %d", result.Body[0].Start)
				}
				if result.Body[0].End != 166 {
					t.Errorf("Expected statement 1 end to be 166, got %d", result.Body[0].End)
				}
				if result.Body[1].Type != StatementSelect {
					t.Errorf("Expected statement 2 type to be SELECT, got %s", result.Body[1].Type)
				}
				if result.Body[1].Start != 177 {
					t.Errorf("Expected statement 2 start to be 177, got %d", result.Body[1].Start)
				}
			})

			t.Run("Should treat a block with two queries as a single query", func(t *testing.T) {
				sql := "\n        DECLARE\n          PK_NAME VARCHAR(200);\n          FOO integer;\n\n        BEGIN\n          SELECT\n            cols.column_name INTO PK_NAME\n          FROM\n            example_table;\n          SELECT 1 INTO FOO from other_example;\n        END;\n      "
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 1 {
					t.Errorf("Expected 1 statement, got %d", len(result.Body))
				}
			})

			t.Run("Should treat a complex block as a single query", func(t *testing.T) {
				sql := "        DECLARE\n          PK_NAME VARCHAR(200);\n\n        BEGIN\n          EXECUTE IMMEDIATE ('CREATE SEQUENCE \"untitled_table3_seq\"');\n\n        SELECT\n          cols.column_name INTO PK_NAME\n        FROM\n          all_constraints cons,\n          all_cons_columns cols\n        WHERE\n          cons.constraint_type = 'P'\n          AND cons.constraint_name = cols.constraint_name\n          AND cons.owner = cols.owner\n          AND cols.table_name = 'untitled_table3';\n\n        execute immediate (\n          'create or replace trigger \"untitled_table3_autoinc_trg\"  BEFORE INSERT on \"untitled_table3\"  for each row  declare  checking number := 1;  begin    if (:new.\"' || PK_NAME || '\" is null) then      while checking >= 1 loop        select \"untitled_table3_seq\".nextval into :new.\"' || PK_NAME || '\" from dual;        select count(\"' || PK_NAME || '\") into checking from \"untitled_table3\"        where \"' || PK_NAME || '\" = :new.\"' || PK_NAME || '\";      end loop;    end if;  end;'\n        );\n        END;\n      "
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 1 {
					t.Errorf("Expected 1 statement, got %d", len(result.Body))
				}
			})

			t.Run("should identify a compound statement with a nested compound statement as a single statement", func(t *testing.T) {
				sql := "DECLARE\n          n_emp_id EMPLOYEES.EMPLOYEE_ID%%TYPE := &emp_id1;\n        BEGIN\n          DECLARE\n            n_emp_id employees.employee_id%%TYPE := &emp_id2;\n            v_name   employees.first_name%%TYPE;\n          BEGIN\n            SELECT first_name, CASE foo WHEN 'a' THEN 1 ELSE 2 END CASE as other\n            INTO v_name\n            FROM employees\n            WHERE employee_id = n_emp_id;\n\n            DBMS_OUTPUT.PUT_LINE('First name of employee ' || n_emp_id ||\n                                              ' is ' || v_name);\n            EXCEPTION\n              WHEN no_data_found THEN\n                DBMS_OUTPUT.PUT_LINE('Employee ' || n_emp_id || ' not found');\n          END;\n        END;"
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 1 {
					t.Errorf("Expected 1 statement, got %d", len(result.Body))
				}
			})

			t.Run("should identify a block query after a create table query", func(t *testing.T) {
				sql := "create table\n          \"untitled_table8\" (\n            \"id\" integer not null primary key,\n            \"created_at\" varchar(255) not null\n          );\n\n        DECLARE\n          PK_NAME VARCHAR(200);\n\n        BEGIN\n          EXECUTE IMMEDIATE ('CREATE SEQUENCE \"untitled_table8_seq\"');\n\n        SELECT\n          cols.column_name INTO PK_NAME\n        FROM\n          all_constraints cons,\n          all_cons_columns cols\n        WHERE\n          cons.constraint_type = 'P'\n          AND cons.constraint_name = cols.constraint_name\n          AND cons.owner = cols.owner\n          AND cols.table_name = 'untitled_table8';\n\n        execute immediate (\n          'create or replace trigger \"untitled_table8_autoinc_trg\"  BEFORE INSERT on \"untitled_table8\"  for each row  declare  checking number := 1;  begin    if (:new.\"' || PK_NAME || '\" is null) then      while checking >= 1 loop        select \"untitled_table8_seq\".nextval into :new.\"' || PK_NAME || '\" from dual;        select count(\"' || PK_NAME || '\") into checking from \"untitled_table8\"        where \"' || PK_NAME || '\" = :new.\"' || PK_NAME || '\";      end loop;    end if;  end;'\n        );\n\n        END;"
				result := Parse(sql, false, DialectOracle, false, DefaultParamTypesFor(DialectGeneric))
				if len(result.Body) != 2 {
					t.Fatalf("Expected 2 statements, got %d", len(result.Body))
				}
				if result.Body[0].Type != StatementCreateTable {
					t.Errorf("Expected statement 1 type to be CREATE_TABLE, got %s", result.Body[0].Type)
				}
				if result.Body[1].Type != StatementAnonBlock {
					t.Errorf("Expected statement 2 type to be ANON_BLOCK, got %s", result.Body[1].Type)
				}
			})
		})
	})
}
