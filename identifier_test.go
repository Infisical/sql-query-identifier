package sqlqueryidentifier

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var (
	dialect = func(d Dialect) *Dialect {
		return &d
	}
	strict = func(b bool) *bool {
		return &b
	}
)

var AllDialects = []Dialect{
	DialectGeneric,
	DialectMySQL,
	DialectPSQL,
	DialectSQLite,
	DialectMSSQL,
	DialectBigQuery,
	DialectOracle,
}

// helper function to reduce boilerplate in tests
func assertIdentifyResults(t *testing.T, query string, options IdentifyOptions, expected []IdentifyResult, expectedError string) {
	t.Helper()
	actual, err := Identify(query, options)

	if expectedError != "" {
		if err == nil {
			t.Fatalf("Expected error to contain %q, but got nil.\nQuery: %q\nOptions: %#v", expectedError, query, options)
		}
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("Expected error to contain %q, but got %q.\nQuery: %q\nOptions: %#v", expectedError, err.Error(), query, options)
		}
		return
	}

	if err != nil {
		t.Fatalf("Unexpected error: %v.\nQuery: %q\nOptions: %#v", err, query, options)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("\nExpected: %#v\nBut got:  %#v\nQuery: %q\nOptions: %#v", expected, actual, query, options)
	}
}

type identifyTestCase struct {
	name          string
	query         string
	options       IdentifyOptions
	expected      []IdentifyResult
	expectedError string
}

func TestIdentify(t *testing.T) {
	t.Run("Inner statements", func(t *testing.T) {
		testCases := []identifyTestCase{
			{
				name:  "should identify a query with inner statements in a single line",
				query: "INSERT INTO Customers (CustomerName, Country) SELECT SupplierName, Country FROM Suppliers",
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           88,
						Text:          "INSERT INTO Customers (CustomerName, Country) SELECT SupplierName, Country FROM Suppliers",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name:  "should identify a query with inner statements in a single line and a comment block in the middle",
				query: "INSERT INTO Customers (CustomerName, Country) /* comment */ SELECT SupplierName, Country FROM Suppliers",
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           102,
						Text:          "INSERT INTO Customers (CustomerName, Country) /* comment */ SELECT SupplierName, Country FROM Suppliers",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name:  "should identify a query with inner statements in multiple lines",
				query: "\n        INSERT INTO Customers (CustomerName, Country)\n        SELECT SupplierName, Country FROM Suppliers;\n      ",
				expected: []IdentifyResult{
					{
						Start:         9,
						End:           106,
						Text:          "INSERT INTO Customers (CustomerName, Country)\n        SELECT SupplierName, Country FROM Suppliers;",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name:  "should identify a query with inner statements in multiple lines and inline comment in the middle",
				query: "\n        INSERT INTO Customers (CustomerName, Country)\n        -- comment\n        SELECT SupplierName, Country FROM Suppliers;\n      ",
				expected: []IdentifyResult{
					{
						Start:         9,
						End:           125,
						Text:          "INSERT INTO Customers (CustomerName, Country)\n        -- comment\n        SELECT SupplierName, Country FROM Suppliers;",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
			})
		}
	})

	t.Run("Single statement", func(t *testing.T) {
		t.Run("Basic SELECT statements", func(t *testing.T) {
			testCases := []identifyTestCase{
				{
					name:  "should identify SELECT statement",
					query: "SELECT * FROM Persons",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           20,
							Text:          "SELECT * FROM Persons",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify SELECT statement with quoted string",
					query: "SELECT 'This is a ''quoted string' FROM Persons",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           46,
							Text:          "SELECT 'This is a ''quoted string' FROM Persons",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify SELECT statement with quoted table",
					query: "SELECT * FROM \"Pers;'ons\"",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           24,
							Text:          "SELECT * FROM \"Pers;'ons\"",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "should identify SELECT statement with quoted table in mssql",
					query:   "SELECT * FROM [Pers;'ons]",
					options: IdentifyOptions{Dialect: dialect(DialectMSSQL)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           24,
							Text:          "SELECT * FROM [Pers;'ons]",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range testCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		for _, typeName := range []string{"DATABASE", "SCHEMA"} {
			t.Run(fmt.Sprintf("identify CREATE %s statements", typeName), func(t *testing.T) {
				query := fmt.Sprintf("CREATE %s Profile;", typeName)
				var expectedStatementType StatementType
				switch typeName {
				case "DATABASE":
					expectedStatementType = StatementCreateDatabase
				case "SCHEMA":
					expectedStatementType = StatementCreateSchema
				}

				expectedResult := IdentifyResult{
					Start:         0,
					End:           len(query) - 1,
					Text:          query,
					Type:          expectedStatementType,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}

				t.Run("should identify statement", func(t *testing.T) {
					assertIdentifyResults(t, query, IdentifyOptions{}, []IdentifyResult{expectedResult}, "")
				})

				t.Run("should throw error for sqlite", func(t *testing.T) {
					options := IdentifyOptions{Dialect: dialect(DialectSQLite)}
					expectedError := fmt.Sprintf(`Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") instead of type="keyword" value="%s" (currentStep=1)`, typeName)
					assertIdentifyResults(t, query, options, nil, expectedError)
				})
			})
		}

		t.Run("CREATE TABLE statement", func(t *testing.T) {
			query := "CREATE TABLE Persons (PersonID int, Name varchar(255));"
			expected := []IdentifyResult{
				{
					Start:         0,
					End:           54,
					Text:          query,
					Type:          StatementCreateTable,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				},
			}
			assertIdentifyResults(t, query, IdentifyOptions{}, expected, "")
		})

		t.Run("identify CREATE VIEW statements", func(t *testing.T) {
			t.Run("should identify CREATE VIEW statement", func(t *testing.T) {
				query := "CREATE VIEW vista AS SELECT 'Hello World';"
				expected := []IdentifyResult{
					{
						Start:         0,
						End:           41,
						Text:          query,
						Type:          StatementCreateView,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
				}
				assertIdentifyResults(t, query, IdentifyOptions{}, expected, "")
			})

			t.Run("identifying CREATE MATERIALIZED VIEW statement", func(t *testing.T) {
				query := "CREATE MATERIALIZED VIEW vista AS SELECT 'Hello World';"
				expectedResult := IdentifyResult{
					Start:         0,
					End:           54,
					Text:          query,
					Type:          StatementCreateView,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}
				supportedDialects := map[Dialect]bool{
					DialectBigQuery: true,
					DialectPSQL:     true,
					DialectMSSQL:    true,
				}
				for _, d := range AllDialects {
					t.Run(fmt.Sprintf("should identify for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						if supportedDialects[d] {
							assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
						} else {
							expectedError := `instead of type="keyword" value="MATERIALIZED" (currentStep=1)`
							assertIdentifyResults(t, query, options, nil, expectedError)
						}
					})
				}
			})

			t.Run("identify CREATE OR REPLACE VIEW statement", func(t *testing.T) {
				query := "CREATE OR REPLACE VIEW vista AS SELECT 'Hello world';"
				expectedResult := IdentifyResult{
					Start:         0,
					End:           52,
					Text:          query,
					Type:          StatementCreateView,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}
				supportedDialects := map[Dialect]bool{
					DialectBigQuery: true,
					DialectGeneric:  true,
					DialectMySQL:    true,
					DialectPSQL:     true,
				}
				for _, d := range AllDialects {
					t.Run(fmt.Sprintf("should identify for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						if supportedDialects[d] {
							assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
						} else {
							switch d {
							case DialectSQLite:
								expectedError := `instead of type="unknown" value="OR" (currentStep=1)`
								assertIdentifyResults(t, query, options, nil, expectedError)
							case DialectMSSQL:
								expectedError := `instead of type="unknown" value="REPLACE" (currentStep=1)`
								assertIdentifyResults(t, query, options, nil, expectedError)
							}
						}
					})
				}
			})

			t.Run("identify CREATE TEMP VIEW statement", func(t *testing.T) {
				query := "CREATE TEMP VIEW vista AS SELECT 'Hello world';"
				expectedResult := IdentifyResult{
					Start:         0,
					End:           46,
					Text:          query,
					Type:          StatementCreateView,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}
				supportedDialects := map[Dialect]bool{
					DialectSQLite: true,
					DialectPSQL:   true,
				}
				for _, d := range AllDialects {
					t.Run(fmt.Sprintf("should identify for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						if supportedDialects[d] {
							assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
						} else {
							expectedError := `instead of type="unknown" value="TEMP" (currentStep=1)`
							assertIdentifyResults(t, query, options, nil, expectedError)
						}
					})
				}
			})

			t.Run("identify CREATE TEMPORARY VIEW statement", func(t *testing.T) {
				query := "CREATE TEMPORARY VIEW vista AS SELECT 'Hello world';"
				expectedResult := IdentifyResult{
					Start:         0,
					End:           51,
					Text:          query,
					Type:          StatementCreateView,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}
				supportedDialects := map[Dialect]bool{
					DialectSQLite: true,
					DialectPSQL:   true,
				}
				for _, d := range AllDialects {
					t.Run(fmt.Sprintf("should identify for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						if supportedDialects[d] {
							assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
						} else {
							expectedError := `instead of type="unknown" value="TEMPORARY" (currentStep=1)`
							assertIdentifyResults(t, query, options, nil, expectedError)
						}
					})
				}
			})

			t.Run("identify CREATE VIEW with algorithm for mysql", func(t *testing.T) {
				baseQuery := "CREATE ALGORITHM = %s VIEW vista AS SELECT 'Hello World';"
				mysqlOptions := IdentifyOptions{Dialect: dialect(DialectMySQL)}

				testCases := []struct {
					algorithm string
				}{
					{"UNDEFINED"},
					{"MERGE"},
					{"TEMPTABLE"},
				}

				for _, tc := range testCases {
					name := fmt.Sprintf("should identify CREATE ALGORITHM = %s", tc.algorithm)
					t.Run(name, func(t *testing.T) {
						query := fmt.Sprintf(baseQuery, tc.algorithm)
						expected := []IdentifyResult{
							{
								Start:         0,
								End:           54 + len(tc.algorithm),
								Text:          query,
								Type:          StatementCreateView,
								ExecutionType: ExecutionModification,
								Parameters:    []string{},
								Tables:        []string{},
							},
						}
						assertIdentifyResults(t, query, mysqlOptions, expected, "")
					})
				}
			})

			t.Run("identify CREATE VIEW with SQL SECURITY for mysql", func(t *testing.T) {
				baseQuery := "CREATE SQL SECURITY %s VIEW vista AS SELECT 'Hello World';"
				mysqlOptions := IdentifyOptions{Dialect: dialect(DialectMySQL)}

				testCases := []struct {
					security string
				}{
					{"DEFINER"},
					{"INVOKER"},
				}

				for _, tc := range testCases {
					name := fmt.Sprintf("should identify SQL SECURITY %s", tc.security)
					t.Run(name, func(t *testing.T) {
						query := fmt.Sprintf(baseQuery, tc.security)
						expected := []IdentifyResult{
							{
								Start:         0,
								End:           55 + len(tc.security),
								Text:          query,
								Type:          StatementCreateView,
								ExecutionType: ExecutionModification,
								Parameters:    []string{},
								Tables:        []string{},
							},
						}
						assertIdentifyResults(t, query, mysqlOptions, expected, "")
					})
				}
			})
		})

		t.Run("identify CREATE TRIGGER statements", func(t *testing.T) {
			triggerTestCases := []identifyTestCase{
				{
					name:    "should identify sqlite CREATE TRIGGER statement",
					query:   "CREATE TRIGGER sqlmods AFTER UPDATE ON bar FOR EACH ROW WHEN old.yay IS NULL BEGIN UPDATE bar SET yay = 1 WHERE rowid = NEW.rowid; END;",
					options: IdentifyOptions{Dialect: dialect(DialectSQLite)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           134,
							Text:          "CREATE TRIGGER sqlmods AFTER UPDATE ON bar FOR EACH ROW WHEN old.yay IS NULL BEGIN UPDATE bar SET yay = 1 WHERE rowid = NEW.rowid; END;",
							Type:          StatementCreateTrigger,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify sqlite CREATE TRIGGER statement with case",
					query: `CREATE TRIGGER DeleteProduct
								BEFORE DELETE ON Product
								BEGIN
												SELECT CASE WHEN (SELECT Inventory.InventoryID FROM Inventory WHERE Inventory.ProductID = OLD.ProductID and Inventory.Quantity=0) IS NULL
												THEN RAISE(ABORT,'Error code 82')
												END;
												-- If RAISE was called, next isntructions are not executed.
												DELETE from inventory where inventory.ProductID=OLD.ProductID;
								END;`,
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   447,
							Text: `CREATE TRIGGER DeleteProduct
								BEFORE DELETE ON Product
								BEGIN
												SELECT CASE WHEN (SELECT Inventory.InventoryID FROM Inventory WHERE Inventory.ProductID = OLD.ProductID and Inventory.Quantity=0) IS NULL
												THEN RAISE(ABORT,'Error code 82')
												END;
												-- If RAISE was called, next isntructions are not executed.
												DELETE from inventory where inventory.ProductID=OLD.ProductID;
								END;`,
							Type:          StatementCreateTrigger,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify SQLSERVER CREATE TRIGGER statement",
					query: `CREATE TRIGGER Purchasing.LowCredit ON Purchasing.PurchaseOrderHeader
										AFTER INSERT
										AS
										IF (ROWCOUNT_BIG() = 0)
										RETURN;
										IF EXISTS (SELECT *
																				FROM Purchasing.PurchaseOrderHeader AS p
																				JOIN inserted AS i
																				ON p.PurchaseOrderID = i.PurchaseOrderID
																				JOIN Purchasing.Vendor AS v
																				ON v.BusinessEntityID = p.VendorID
																				WHERE v.CreditRating = 5
																				)
										BEGIN
										RAISERROR ('A vendor''s credit rating is too low to accept new
										purchase orders.', 16, 1);
										ROLLBACK TRANSACTION;
										RETURN
										END;`,
					options: IdentifyOptions{Dialect: dialect(DialectMSSQL)},
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   707,
							Text: `CREATE TRIGGER Purchasing.LowCredit ON Purchasing.PurchaseOrderHeader
										AFTER INSERT
										AS
										IF (ROWCOUNT_BIG() = 0)
										RETURN;
										IF EXISTS (SELECT *
																				FROM Purchasing.PurchaseOrderHeader AS p
																				JOIN inserted AS i
																				ON p.PurchaseOrderID = i.PurchaseOrderID
																				JOIN Purchasing.Vendor AS v
																				ON v.BusinessEntityID = p.VendorID
																				WHERE v.CreditRating = 5
																				)
										BEGIN
										RAISERROR ('A vendor''s credit rating is too low to accept new
										purchase orders.', 16, 1);
										ROLLBACK TRANSACTION;
										RETURN
										END;`,
							Type:          StatementCreateTrigger,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify postgres CREATE TRIGGER statement",
					query: "CREATE TRIGGER view_insert INSTEAD OF INSERT ON my_view FOR EACH ROW EXECUTE PROCEDURE view_insert_row();",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           104,
							Text:          "CREATE TRIGGER view_insert INSTEAD OF INSERT ON my_view FOR EACH ROW EXECUTE PROCEDURE view_insert_row();",
							Type:          StatementCreateTrigger,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range triggerTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("identify PROCEDURE statements", func(t *testing.T) {
			t.Run("identify CREATE PROCEDURE statements", func(t *testing.T) {
				query := `CREATE PROCEDURE mydataset.create_customer()
														BEGIN
																DECLARE id STRING;
																SET id = GENERATE_UUID();
																INSERT INTO mydataset.customers (customer_id)
																		VALUES(id);
																SELECT FORMAT("Created customer %s", id);
														END`
				expectedResult := IdentifyResult{
					Start:         0,
					End:           308,
					Text:          query,
					Type:          StatementCreateProcedure,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}

				supportedDialects := []Dialect{
					DialectBigQuery,
					DialectGeneric,
					DialectMSSQL,
					DialectMySQL,
					DialectOracle,
					DialectPSQL,
				}

				for _, d := range supportedDialects {
					t.Run(fmt.Sprintf("should identify statement for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
					})
				}

				t.Run("should throw error for sqlite", func(t *testing.T) {
					options := IdentifyOptions{Dialect: dialect(DialectSQLite)}
					expectedError := `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") instead of type="keyword" value="PROCEDURE" (currentStep=1)`
					assertIdentifyResults(t, query, options, nil, expectedError)
				})
			})

			t.Run("identify CREATE OR REPLACE PROCEDURE statements", func(t *testing.T) {
				query := `CREATE OR REPLACE PROCEDURE mydataset.create_customer()
												BEGIN
														DECLARE id STRING;
														SET id = GENERATE_UUID();
														INSERT INTO mydataset.customers (customer_id)
																VALUES(id);
														SELECT FORMAT("Created customer %s", id);
												END`
				expectedResult := IdentifyResult{
					Start:         0,
					End:           305,
					Text:          query,
					Type:          StatementCreateProcedure,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}

				supportedDialects := map[Dialect]bool{
					DialectBigQuery: true,
					DialectMySQL:    true,
					DialectPSQL:     true,
					DialectOracle:   true,
					DialectGeneric:  true,
				}

				for _, d := range AllDialects {
					t.Run(fmt.Sprintf("should identify for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						if supportedDialects[d] {
							assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
						} else {
							var expectedError string
							switch d {
							case DialectSQLite:
								expectedError = `instead of type="unknown" value="OR" (currentStep=1)`
							case DialectMSSQL:
								expectedError = `instead of type="unknown" value="REPLACE" (currentStep=1)`
							default:
								expectedError = `instead of type="unknown" value="OR" (currentStep=1)`
							}
							assertIdentifyResults(t, query, options, nil, expectedError)
						}
					})
				}
			})

			t.Run("identify CREATE OR ALTER PROCEDURE statements", func(t *testing.T) {
				query := `CREATE OR ALTER PROCEDURE mydataset.create_customer()
										BEGIN
												DECLARE id STRING;
												SET id = GENERATE_UUID();
												INSERT INTO mydataset.customers (customer_id)
														VALUES(id);
												SELECT FORMAT("Created customer %s", id);
										END`
				expectedResult := IdentifyResult{
					Start:         0,
					End:           289,
					Text:          query,
					Type:          StatementCreateProcedure,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}
				t.Run("should identify statement with OR ALTER for mssql", func(t *testing.T) {
					assertIdentifyResults(t, query, IdentifyOptions{Dialect: dialect(DialectMSSQL)}, []IdentifyResult{expectedResult}, "")
				})
			})

			t.Run("identify DROP PROCEDURE statements", func(t *testing.T) {
				query := `DROP PROCEDURE mydataset.create_customer`
				expectedResult := IdentifyResult{
					Start:         0,
					End:           39,
					Text:          query,
					Type:          StatementDropProcedure,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}

				supportedDialects := []Dialect{
					DialectBigQuery,
					DialectGeneric,
					DialectMSSQL,
					DialectMySQL,
					DialectOracle,
					DialectPSQL,
				}

				for _, d := range supportedDialects {
					t.Run(fmt.Sprintf("should identify the statement for %s", d), func(t *testing.T) {
						assertIdentifyResults(t, query, IdentifyOptions{Dialect: dialect(d)}, []IdentifyResult{expectedResult}, "")
					})
				}

				t.Run("should error for sqlite", func(t *testing.T) {
					options := IdentifyOptions{Dialect: dialect(DialectSQLite)}
					expectedError := `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") instead of type="keyword" value="PROCEDURE" (currentStep=1)`
					assertIdentifyResults(t, query, options, nil, expectedError)
				})
			})

			t.Run("identify ALTER PROCEDURE statements", func(t *testing.T) {
				query := `ALTER PROCEDURE mydataset.create_customer`
				expectedResult := IdentifyResult{
					Start:         0,
					End:           40,
					Text:          query,
					Type:          StatementAlterProcedure,
					ExecutionType: ExecutionModification,
					Parameters:    []string{},
					Tables:        []string{},
				}

				supportedDialects := map[Dialect]bool{
					DialectGeneric: true,
					DialectMSSQL:   true,
					DialectMySQL:   true,
					DialectOracle:  true,
					DialectPSQL:    true,
				}

				for _, d := range AllDialects {
					t.Run(fmt.Sprintf("should identify for %s", d), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(d)}
						if supportedDialects[d] {
							assertIdentifyResults(t, query, options, []IdentifyResult{expectedResult}, "")
						} else {
							var expectedError string
							queryToTest := query
							switch d {
							case DialectBigQuery:
								expectedError = `Expected any of these tokens (type="keyword" value="DATABASE") or (type="keyword" value="SCHEMA") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") or (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="PROCEDURE"`
							case DialectSQLite:
								queryToTest = `DROP PROCEDURE mydataset.create_customer`
								expectedError = `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") instead of type="keyword" value="PROCEDURE" (currentStep=1)`
							default:
								expectedError = `Expected any of these tokens (type="keyword" value="DATABASE") or (type="keyword" value="SCHEMA") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") or (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="PROCEDURE"`
							}
							assertIdentifyResults(t, queryToTest, options, nil, expectedError)
						}
					})
				}
			})
		})

		t.Run("identify SHOW statements", func(t *testing.T) {
			showTestCases := []struct {
				stmtType string
				sql      string
			}{
				{"BINARY", "SHOW BINARY LOGS 'blerns';"},
				{"BINLOG", "SHOW BINLOG EVENTS 'blerns';"},
				{"CHARACTER", "SHOW CHARACTER SET 'blerns';"},
				{"COLLATION", "SHOW COLLATION 'blerns';"},
				{"COLUMNS", "SHOW COLUMNS 'blerns';"},
				{"CREATE", "SHOW CREATE DATABASE 'blerns';"},
				{"DATABASES", "SHOW DATABASES 'blerns';"},
				{"ENGINE", "SHOW ENGINE 'blerns';"},
				{"ENGINES", "SHOW ENGINES 'blerns';"},
				{"ERRORS", "SHOW ERRORS 'blerns';"},
				{"EVENTS", "SHOW EVENTS 'blerns';"},
				{"FUNCTION", "SHOW FUNCTION CODE 'blerns';"},
				{"GRANTS", "SHOW GRANTS 'blerns';"},
				{"INDEX", "SHOW INDEX 'blerns';"},
				{"MASTER", "SHOW MASTER STATUS 'blerns';"},
				{"OPEN", "SHOW OPEN TABLES 'blerns';"},
				{"PLUGINS", "SHOW PLUGINS 'blerns';"},
				{"PRIVILEGES", "SHOW PRIVILEGES 'blerns';"},
				{"PROCEDURE", "SHOW PROCEDURE CODE 'blerns';"},
				{"PROCESSLIST", "SHOW PROCESSLIST 'blerns';"},
				{"PROFILE", "SHOW PROFILE 'blerns';"},
				{"PROFILES", "SHOW PROFILES 'blerns';"},
				{"RELAYLOG", "SHOW RELAYLOG EVENTS 'blerns';"},
				{"REPLICAS", "SHOW REPLICAS 'blerns';"},
				{"SLAVE", "SHOW SLAVE HOSTS;"},
				{"REPLICA", "SHOW REPLICA STATUS 'blerns';"},
				{"STATUS", "SHOW STATUS 'blerns';"},
				{"TABLE", "SHOW TABLE STATUS 'blerns';"},
				{"TABLES", "SHOW TABLES 'blerns';"},
				{"TRIGGERS", "SHOW TRIGGERS 'blerns';"},
				{"VARIABLES", "SHOW VARIABLES 'blerns';"},
				{"WARNINGS", "SHOW WARNINGS 'blerns';"},
			}

			dialectsToTest := []Dialect{DialectMySQL, DialectGeneric, DialectMSSQL}

			for _, d := range dialectsToTest {
				currentDialect := d
				t.Run(fmt.Sprintf("for dialect %s", currentDialect), func(t *testing.T) {
					for _, tc := range showTestCases {
						currentType := tc.stmtType
						currentSQL := tc.sql

						testName := fmt.Sprintf("should identify SHOW %s statements", currentType)
						if currentDialect == DialectMSSQL {
							testName = fmt.Sprintf("should throw error for SHOW %s statements on MSSQL", currentType)
						}

						t.Run(testName, func(t *testing.T) {
							options := IdentifyOptions{Dialect: dialect(currentDialect)}

							if currentDialect == DialectMSSQL {
								expectedError := `Invalid statement parser "SHOW"`
								assertIdentifyResults(t, currentSQL, options, nil, expectedError)
								return
							}

							expected := []IdentifyResult{
								{
									Start:         0,
									End:           len(currentSQL) - 1,
									Text:          currentSQL,
									Type:          StatementType(fmt.Sprintf("SHOW_%s", currentType)),
									ExecutionType: ExecutionListing,
									Parameters:    []string{},
									Tables:        []string{},
								},
							}
							assertIdentifyResults(t, currentSQL, options, expected, "")
						})
					}
				})
			}
		})

		t.Run("identify CREATE FUNCTION statements", func(t *testing.T) {
			functionTestCases := []identifyTestCase{
				{
					name: "should identify postgres CREATE FUNCTION statement with LANGUAGE at end",
					query: `CREATE FUNCTION quarterly_summary_func(start_date date DEFAULT CURRENT_TIMESTAMP)
								RETURNS TABLE (staff_name text, staff_bonus int, quarter tsrange)
								As $$
								DECLARE
										employee RECORD;
										total_bonus int;
										sales_total int;
										end_date date := start_date + interval '3 months';
								BEGIN
										FOR employee IN SELECT staff_id FROM staff LOOP
												EXECUTE 'SELECT sum(staff_bonus), sum(sales_price) FROM sales WHERE staff_id = $1
												AND created_at >= $2 AND created_at < $3'
														INTO total_bonus, sales_total
														USING employee.staff_id, start_date, end_date;
												RAISE NOTICE 'total bonus is % and total sales is %', total_bonus, sales_total;
										EXECUTE 'INSERT INTO sales_summary (staff_id, bonus, total_sales, period) VALUES
																						($1, $2, $3, tsrange($4, $5))'
														USING employee.staff_id, total_bonus, sales_total, start_date, end_date;
										END LOOP;
										DELETE FROM sales WHERE created_at >= start_date
																AND created_at < end_date;
										RETURN QUERY SELECT name, bonus, period FROM sales_summary
																						LEFT JOIN staff on sales_summary.staff_id = staff.staff_id;
								RETURN;
								END;
								$$
								LANGUAGE plpgsql;`,
					options: IdentifyOptions{Dialect: dialect(DialectPSQL)},
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   1313,
							Text: `CREATE FUNCTION quarterly_summary_func(start_date date DEFAULT CURRENT_TIMESTAMP)
								RETURNS TABLE (staff_name text, staff_bonus int, quarter tsrange)
								As $$
								DECLARE
										employee RECORD;
										total_bonus int;
										sales_total int;
										end_date date := start_date + interval '3 months';
								BEGIN
										FOR employee IN SELECT staff_id FROM staff LOOP
												EXECUTE 'SELECT sum(staff_bonus), sum(sales_price) FROM sales WHERE staff_id = $1
												AND created_at >= $2 AND created_at < $3'
														INTO total_bonus, sales_total
														USING employee.staff_id, start_date, end_date;
												RAISE NOTICE 'total bonus is % and total sales is %', total_bonus, sales_total;
										EXECUTE 'INSERT INTO sales_summary (staff_id, bonus, total_sales, period) VALUES
																						($1, $2, $3, tsrange($4, $5))'
														USING employee.staff_id, total_bonus, sales_total, start_date, end_date;
										END LOOP;
										DELETE FROM sales WHERE created_at >= start_date
																AND created_at < end_date;
										RETURN QUERY SELECT name, bonus, period FROM sales_summary
																						LEFT JOIN staff on sales_summary.staff_id = staff.staff_id;
								RETURN;
								END;
								$$
								LANGUAGE plpgsql;`,
							Type:          StatementCreateFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify postgres CREATE FUNCTION statement with LANGUAGE at beginning",
					query: `CREATE OR REPLACE FUNCTION f_grp_prod(text)
										RETURNS TABLE (
												name text
										, result1 double precision
										, result2 double precision)
								LANGUAGE plpgsql STABLE
								AS
								$BODY$
								DECLARE
												r      mytable%ROWTYPE;
												_round integer;
								BEGIN
												-- init vars
												name    := $1;
												result2 := 1;       -- abuse result2 as temp var for convenience

								FOR r IN
												SELECT *
												FROM   mytable m
												WHERE  m.name = name
												ORDER  BY m.round
								LOOP
												IF r.round <> _round THEN   -- save result1 before 2nd round
																result1 := result2;
																result2 := 1;
												END IF;

												result2 := result2 * (1 - r.val/100);
												_round  := r.round;
								END LOOP;

								RETURN NEXT;

								END;
								$BODY$;`,
					options: IdentifyOptions{Dialect: dialect(DialectPSQL)},
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   902,
							Text: `CREATE OR REPLACE FUNCTION f_grp_prod(text)
										RETURNS TABLE (
												name text
										, result1 double precision
										, result2 double precision)
								LANGUAGE plpgsql STABLE
								AS
								$BODY$
								DECLARE
												r      mytable%ROWTYPE;
												_round integer;
								BEGIN
												-- init vars
												name    := $1;
												result2 := 1;       -- abuse result2 as temp var for convenience

								FOR r IN
												SELECT *
												FROM   mytable m
												WHERE  m.name = name
												ORDER  BY m.round
								LOOP
												IF r.round <> _round THEN   -- save result1 before 2nd round
																result1 := result2;
																result2 := 1;
												END IF;

												result2 := result2 * (1 - r.val/100);
												_round  := r.round;
								END LOOP;

								RETURN NEXT;

								END;
								$BODY$;`,
							Type:          StatementCreateFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify postgres CREATE FUNCTION statement with case",
					query: `CREATE OR REPLACE FUNCTION af_calculate_range(tt TEXT, tc INTEGER)
								RETURNS INTEGER IMMUTABLE AS $$
								BEGIN
												RETURN CASE tt WHEN 'day' THEN tc * 60 * 60
																										WHEN 'hour' THEN tc * 60
																		END;
								END;
								$$
								LANGUAGE PLPGSQL;`,
					options: IdentifyOptions{Dialect: dialect(DialectPSQL)},
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   299,
							Text: `CREATE OR REPLACE FUNCTION af_calculate_range(tt TEXT, tc INTEGER)
								RETURNS INTEGER IMMUTABLE AS $$
								BEGIN
												RETURN CASE tt WHEN 'day' THEN tc * 60 * 60
																										WHEN 'hour' THEN tc * 60
																		END;
								END;
								$$
								LANGUAGE PLPGSQL;`,
							Type:          StatementCreateFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "should identify mysql CREATE FUNCTION statement",
					query:   "CREATE FUNCTION hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!');",
					options: IdentifyOptions{Dialect: dialect(DialectMySQL)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           96,
							Text:          "CREATE FUNCTION hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!');",
							Type:          StatementCreateFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "should identify mysql CREATE FUNCTION statement with definer",
					query:   "CREATE DEFINER = 'admin'@'localhost' FUNCTION hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!');",
					options: IdentifyOptions{Dialect: dialect(DialectMySQL)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           126,
							Text:          "CREATE DEFINER = 'admin'@'localhost' FUNCTION hello (s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ',s,'!');",
							Type:          StatementCreateFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify sql server CREATE FUNCTION statement",
					query: `CREATE FUNCTION dbo.ISOweek (@DATE datetime)
								RETURNS int
								WITH EXECUTE AS CALLER
								AS
								BEGIN
												DECLARE @ISOweek int;
												SET @ISOweek= DATEPART(wk,@DATE)+1
																-DATEPART(wk,CAST(DATEPART(yy,@DATE) as CHAR(4))+'0104');
								--Special cases: Jan 1-3 may belong to the previous year
												IF (@ISOweek=0)
																SET @ISOweek=dbo.ISOweek(CAST(DATEPART(yy,@DATE)-1
																				AS CHAR(4))+'12'+ CAST(24+DATEPART(DAY,@DATE) AS CHAR(2)))+1;
								--Special case: Dec 29-31 may belong to the next year
												IF ((DATEPART(mm,@DATE)=12) AND
																((DATEPART(dd,@DATE)-DATEPART(dw,@DATE))>= 28))
												SET @ISOweek=1;
												RETURN(@ISOweek);
								END;`,
					options: IdentifyOptions{Dialect: dialect(DialectMSSQL)},
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   757,
							Text: `CREATE FUNCTION dbo.ISOweek (@DATE datetime)
								RETURNS int
								WITH EXECUTE AS CALLER
								AS
								BEGIN
												DECLARE @ISOweek int;
												SET @ISOweek= DATEPART(wk,@DATE)+1
																-DATEPART(wk,CAST(DATEPART(yy,@DATE) as CHAR(4))+'0104');
								--Special cases: Jan 1-3 may belong to the previous year
												IF (@ISOweek=0)
																SET @ISOweek=dbo.ISOweek(CAST(DATEPART(yy,@DATE)-1
																				AS CHAR(4))+'12'+ CAST(24+DATEPART(DAY,@DATE) AS CHAR(2)))+1;
								--Special case: Dec 29-31 may belong to the next year
												IF ((DATEPART(mm,@DATE)=12) AND
																((DATEPART(dd,@DATE)-DATEPART(dw,@DATE))>= 28))
												SET @ISOweek=1;
												RETURN(@ISOweek);
								END;`,
							Type:          StatementCreateFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range functionTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("identify CREATE INDEX statements", func(t *testing.T) {
			t.Run("Generic and Unique indexes", func(t *testing.T) {
				indexTestCases := []identifyTestCase{
					{
						name:    "should identify CREATE INDEX statement",
						query:   "CREATE INDEX foo ON bar (baz)",
						options: IdentifyOptions{Dialect: dialect(DialectMySQL)},
						expected: []IdentifyResult{
							{
								Start:         0,
								End:           28,
								Text:          "CREATE INDEX foo ON bar (baz)",
								Type:          StatementCreateIndex,
								ExecutionType: ExecutionModification,
								Parameters:    []string{},
								Tables:        []string{},
							},
						},
					},
					{
						name:    "should identify CREATE UNIQUE INDEX statement",
						query:   "CREATE UNIQUE INDEX foo ON bar (baz)",
						options: IdentifyOptions{Dialect: dialect(DialectMySQL)},
						expected: []IdentifyResult{
							{
								Start:         0,
								End:           35,
								Text:          "CREATE UNIQUE INDEX foo ON bar (baz)",
								Type:          StatementCreateIndex,
								ExecutionType: ExecutionModification,
								Parameters:    []string{},
								Tables:        []string{},
							},
						},
					},
				}
				for _, tc := range indexTestCases {
					tc := tc
					t.Run(tc.name, func(t *testing.T) {
						assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
					})
				}
			})

			t.Run("mysql specific index options", func(t *testing.T) {
				indexTypes := []string{"FULLTEXT", "SPATIAL"}
				for _, indexType := range indexTypes {
					t.Run(fmt.Sprintf("should identify CREATE %s INDEX statement", indexType), func(t *testing.T) {
						query := fmt.Sprintf("CREATE %s INDEX foo ON bar (baz)", indexType)
						expected := []IdentifyResult{
							{
								Start:         0,
								End:           29 + len(indexType),
								Text:          query,
								Type:          StatementCreateIndex,
								ExecutionType: ExecutionModification,
								Parameters:    []string{},
								Tables:        []string{},
							},
						}
						assertIdentifyResults(t, query, IdentifyOptions{Dialect: dialect(DialectMySQL)}, expected, "")
					})
				}
			})
			t.Run("mssql specific index options", func(t *testing.T) {
				indexTypes := []string{"CLUSTERED", "NONCLUSTERED"}
				for _, indexType := range indexTypes {
					t.Run(fmt.Sprintf("should identify CREATE %s INDEX statement", indexType), func(t *testing.T) {
						query := fmt.Sprintf("CREATE %s INDEX foo ON bar (baz)", indexType)
						expected := []IdentifyResult{
							{
								Start:         0,
								End:           29 + len(indexType),
								Text:          query,
								Type:          StatementCreateIndex,
								ExecutionType: ExecutionModification,
								Parameters:    []string{},
								Tables:        []string{},
							},
						}
						assertIdentifyResults(t, query, IdentifyOptions{Dialect: dialect(DialectMSSQL)}, expected, "")
					})
				}
			})
		})

		t.Run("identify DROP statements", func(t *testing.T) {
			for _, typeName := range []string{"DATABASE", "SCHEMA"} {
				t.Run(fmt.Sprintf(`identify "DROP %s" statements`, typeName), func(t *testing.T) {
					query := fmt.Sprintf("DROP %s Profile;", typeName)
					var expectedStatementType StatementType
					switch typeName {
					case "DATABASE":
						expectedStatementType = StatementDropDatabase
					case "SCHEMA":
						expectedStatementType = StatementDropSchema
					}

					expectedResult := IdentifyResult{
						Start:         0,
						End:           len(query) - 1,
						Text:          query,
						Type:          expectedStatementType,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					}

					t.Run("should identify statement", func(t *testing.T) {
						assertIdentifyResults(t, query, IdentifyOptions{}, []IdentifyResult{expectedResult}, "")
					})

					t.Run("should throw error for sqlite", func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(DialectSQLite)}
						expectedError := fmt.Sprintf(`Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") or (type="keyword" value="TRIGGER") or (type="keyword" value="FUNCTION") or (type="keyword" value="INDEX") instead of type="keyword" value="%s" (currentStep=1)`, typeName)
						assertIdentifyResults(t, query, options, nil, expectedError)
					})
				})
			}

			dropTestCases := []identifyTestCase{
				{
					name:  "should identify DROP TABLE statement",
					query: "DROP TABLE Persons;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           18,
							Text:          "DROP TABLE Persons;",
							Type:          StatementDropTable,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify DROP VIEW statement",
					query: "DROP VIEW kinds;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           15,
							Text:          "DROP VIEW kinds;",
							Type:          StatementDropView,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify DROP TRIGGER statement",
					query: "DROP TRIGGER delete_stu on student_mast;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           39,
							Text:          "DROP TRIGGER delete_stu on student_mast;",
							Type:          StatementDropTrigger,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify DROP FUNCTION statement",
					query: "DROP FUNCTION sqrt(integer);",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           27,
							Text:          "DROP FUNCTION sqrt(integer);",
							Type:          StatementDropFunction,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify DROP INDEX statement",
					query: "DROP INDEX foo;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           14,
							Text:          "DROP INDEX foo;",
							Type:          StatementDropIndex,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range dropTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("TRUNCATE, INSERT, UPDATE, DELETE statements", func(t *testing.T) {
			dmlTestCases := []identifyTestCase{
				{
					name:  "identify TRUNCATE TABLE statement",
					query: "TRUNCATE TABLE Persons;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           22,
							Text:          "TRUNCATE TABLE Persons;",
							Type:          StatementTruncate,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify INSERT statement",
					query: "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           55,
							Text:          "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');",
							Type:          StatementInsert,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify UPDATE statement",
					query: "UPDATE Persons SET Name = 'John' WHERE PersonID = 1;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           51,
							Text:          "UPDATE Persons SET Name = 'John' WHERE PersonID = 1;",
							Type:          StatementUpdate,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify more complex UPDATE statement with a weird string/keyword",
					query: "UPDATE customers SET a = 0, note = CONCAT(note, \"abc;def\") WHERE a = 10;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           71,
							Text:          "UPDATE customers SET a = 0, note = CONCAT(note, \"abc;def\") WHERE a = 10;",
							Type:          StatementUpdate,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify DELETE statement",
					query: "DELETE FROM Persons WHERE PersonID = 1;",
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           38,
							Text:          "DELETE FROM Persons WHERE PersonID = 1;",
							Type:          StatementDelete,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range dmlTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("identify ALTER statements", func(t *testing.T) {
			alterTestCases := []struct {
				stmtType StatementType
				query    string
			}{
				{StatementAlterDatabase, "ALTER DATABASE foo RENAME TO bar;"},
				{StatementAlterSchema, "ALTER SCHEMA foo RENAME to bar;"},
				{StatementAlterTable, "ALTER TABLE foo RENAME TO bar;"},
				{StatementAlterView, "ALTER VIEW foo RENAME TO bar;"},
				{StatementAlterTrigger, "ALTER TRIGGER foo ON bar RENAME TO baz;"},
				{StatementAlterFunction, "ALTER FUNCTION sqrt(integer) RENAME TO square_root;"},
				{StatementAlterIndex, "ALTER INDEX foo RENAME to bar;"},
			}

			for _, tc := range alterTestCases {
				tc := tc
				t.Run(fmt.Sprintf(`should identify "%s" statement`, tc.stmtType), func(t *testing.T) {
					expected := []IdentifyResult{
						{
							Start:         0,
							End:           len(tc.query) - 1,
							Text:          tc.query,
							Type:          tc.stmtType,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					}
					assertIdentifyResults(t, tc.query, IdentifyOptions{}, expected, "")
				})
			}

			t.Run("sqlite errors for unsupported ALTER types", func(t *testing.T) {
				sqliteErrorTestCases := []struct {
					stmtType    string
					query       string
					expectedErr string
				}{
					{"DATABASE", "ALTER DATABASE foo RENAME TO bar;", `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="DATABASE" (currentStep=1).`},
					{"SCHEMA", "ALTER SCHEMA foo RENAME to bar;", `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="SCHEMA" (currentStep=1).`},
					{"TRIGGER", "ALTER TRIGGER foo ON bar RENAME TO baz;", `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="TRIGGER" (currentStep=1).`},
					{"FUNCTION", "ALTER FUNCTION sqrt(integer) RENAME TO square_root;", `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="FUNCTION" (currentStep=1).`},
					{"INDEX", "ALTER INDEX foo RENAME to bar;", `Expected any of these tokens (type="keyword" value="TABLE") or (type="keyword" value="VIEW") instead of type="keyword" value="INDEX" (currentStep=1).`},
				}

				for _, tc := range sqliteErrorTestCases {
					tc := tc
					t.Run(fmt.Sprintf(`should throw error for "ALTER_%s" statement`, tc.stmtType), func(t *testing.T) {
						options := IdentifyOptions{Dialect: dialect(DialectSQLite)}
						assertIdentifyResults(t, tc.query, options, nil, tc.expectedErr)
					})
				}
			})
		})

		t.Run("Statements with comments", func(t *testing.T) {
			commentTestCases := []identifyTestCase{
				{
					name:  "should identify statement starting with inline comment",
					query: "\n        -- some comment\n        SELECT * FROM Persons\n      ",
					expected: []IdentifyResult{
						{
							Start:         33,
							End:           60,
							Text:          "SELECT * FROM Persons\n      ",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify statement starting with block comment",
					query: `
						/**
						  * some comment
						  */
						SELECT * FROM Persons
					`,
					expected: []IdentifyResult{
						{
							Start:         51,
							End:           77,
							Text:          "SELECT * FROM Persons\n\t\t\t\t\t",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify statement ending with block comment",
					query: `
						SELECT * FROM Persons
						/**
						  * some comment
						  */
					`,
					expected: []IdentifyResult{
						{
							Start:         7,
							End:           77,
							Text:          "SELECT * FROM Persons\n\t\t\t\t\t\t/**\n\t\t\t\t\t\t  * some comment\n\t\t\t\t\t\t  */\n\t\t\t\t\t",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify statement ending with inline comment",
					query: `
						SELECT * FROM Persons
						-- some comment
					`,
					expected: []IdentifyResult{
						{
							Start:         7,
							End:           55,
							Text:          "SELECT * FROM Persons\n\t\t\t\t\t\t-- some comment\n\t\t\t\t\t",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify statement with inline comment in the middle",
					query: "\n        SELECT *\n        -- some comment\n        FROM Persons\n      ",
					expected: []IdentifyResult{
						{
							Start:         9,
							End:           68,
							Text:          "SELECT *\n        -- some comment\n        FROM Persons\n      ",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:  "should identify statement with block comment in the middle",
					query: "\n        SELECT *\n        /**\n          * some comment\n          */\n        FROM Persons\n      ",
					expected: []IdentifyResult{
						{
							Start:         9,
							End:           94,
							Text:          "SELECT *\n        /**\n          * some comment\n          */\n        FROM Persons\n      ",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range commentTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("Edge cases and specific statements", func(t *testing.T) {
			edgeCaseTestCases := []identifyTestCase{
				{
					name:     "should identify empty statement",
					query:    "",
					expected: []IdentifyResult{},
				},
				{
					name:    "should be able to detect a statement even without knowing its type when strict is disabled - CREATE LOGFILE",
					query:   "CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' INITIAL_SIZE = 10M;",
					options: IdentifyOptions{Strict: strict(false)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           67,
							Text:          "CREATE LOGFILE GROUP lg1 ADD UNDOFILE 'undo.dat' INITIAL_SIZE = 10M;",
							Type:          "CREATE_LOGFILE",
							ExecutionType: ExecutionUnknown,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "Should identify declare statement as unknown for bigquery",
					query:   "DECLARE start_time TIMESTAMP DEFAULT '2022-08-08 13:05:00';",
					options: IdentifyOptions{Dialect: dialect(DialectBigQuery), Strict: strict(false)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           58,
							Text:          "DECLARE start_time TIMESTAMP DEFAULT '2022-08-08 13:05:00';",
							Type:          StatementUnknown,
							ExecutionType: ExecutionUnknown,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "Should parse lower case block opener in query correctly",
					query: `CREATE OR REPLACE PROCEDURE foo.bar (col string)
								BEGIN
								if foo is not null then
												SET foo = 'bar';
								end if;

								SELECT 1;
								END;`,
					options: IdentifyOptions{Dialect: dialect(DialectBigQuery), Strict: strict(false)},
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   170,
							Text: `CREATE OR REPLACE PROCEDURE foo.bar (col string)
								BEGIN
								if foo is not null then
												SET foo = 'bar';
								end if;

								SELECT 1;
								END;`,
							Type:          StatementCreateProcedure,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "Should identify a lone END",
					query:   "END;",
					options: IdentifyOptions{Strict: strict(false)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           3,
							Text:          "END;",
							Type:          StatementUnknown,
							ExecutionType: ExecutionUnknown,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range edgeCaseTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("identifying CTE statements", func(t *testing.T) {
			cteTestCases := []identifyTestCase{
				{
					name: "should identify statement using CTE with column list",
					query: `WITH cte_name (column1, column2) AS (
										SELECT * FROM table
								)
								SELECT * FROM cte_name;`,
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   108,
							Text: `WITH cte_name (column1, column2) AS (
										SELECT * FROM table
								)
								SELECT * FROM cte_name;`,
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify statement using lower case CTE with column list",
					query: `with cte_name (column1, column2) AS (
										SELECT * FROM table
								)
								SELECT * FROM cte_name;`,
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   108,
							Text: `with cte_name (column1, column2) AS (
										SELECT * FROM table
								)
								SELECT * FROM cte_name;`,
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify statement using multiple CTE and no column list",
					query: `WITH
								cte1 AS
								(
										SELECT 1 AS id
								),
								cte2 AS
								(
										SELECT 2 AS id
								),
								cte3 AS
								(
										SELECT 3 as id
								)
								SELECT  *
								FROM    cte1
								UNION ALL
								SELECT  *
								FROM    cte2
								UNION ALL
								SELECT  *
								FROM    cte3`,
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   341,
							Text: `WITH
								cte1 AS
								(
										SELECT 1 AS id
								),
								cte2 AS
								(
										SELECT 2 AS id
								),
								cte3 AS
								(
										SELECT 3 as id
								)
								SELECT  *
								FROM    cte1
								UNION ALL
								SELECT  *
								FROM    cte2
								UNION ALL
								SELECT  *
								FROM    cte3`,
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify statement with nested CTEs",
					query: `with temp as (
										with data as (
												select *
												from city
												limit 10
										)
										select name
										from data
								)
								select *
								from temp;`,
					expected: []IdentifyResult{
						{
							Start: 0,
							End:   202,
							Text: `with temp as (
										with data as (
												select *
												from city
												limit 10
										)
										select name
										from data
								)
								select *
								from temp;`,
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range cteTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})

		t.Run("Parameter extraction", func(t *testing.T) {
			parameterTestCases := []identifyTestCase{
				{
					name:    "Should extract positional Parameters",
					query:   "SELECT * FROM Persons where x = $1 and y = $2 and a = $1",
					options: IdentifyOptions{Dialect: dialect(DialectPSQL), Strict: strict(true)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           55,
							Text:          "SELECT * FROM Persons where x = $1 and y = $2 and a = $1",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{"$1", "$2"},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "Should extract positional Parameters with trailing commas",
					query:   "SELECT $1,$2 FROM foo WHERE foo.id in ($3, $4)",
					options: IdentifyOptions{Dialect: dialect(DialectPSQL), Strict: strict(true)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           45,
							Text:          "SELECT $1,$2 FROM foo WHERE foo.id in ($3, $4)",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{"$1", "$2", "$3", "$4"},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "Should extract named Parameters",
					query:   "SELECT * FROM Persons where x = :one and y = :two and a = :one",
					options: IdentifyOptions{Dialect: dialect(DialectMSSQL), Strict: strict(true)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           61,
							Text:          "SELECT * FROM Persons where x = :one and y = :two and a = :one",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{":one", ":two"},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "Should extract named Parameters with trailing commas",
					query:   "SELECT * FROM Persons where x in (:one, :two, :three)",
					options: IdentifyOptions{Dialect: dialect(DialectMSSQL), Strict: strict(true)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           52,
							Text:          "SELECT * FROM Persons where x in (:one, :two, :three)",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{":one", ":two", ":three"},
							Tables:        []string{},
						},
					},
				},
				{
					name:    "Should extract question mark Parameters",
					query:   "SELECT * FROM Persons where x = ? and y = ? and a = ?",
					options: IdentifyOptions{Dialect: dialect(DialectMySQL), Strict: strict(true)},
					expected: []IdentifyResult{
						{
							Start:         0,
							End:           52,
							Text:          "SELECT * FROM Persons where x = ? and y = ? and a = ?",
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{"?", "?", "?"},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range parameterTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})
	})

	t.Run("Multiple statements", func(t *testing.T) {
		multiStatementTestCases := []identifyTestCase{
			{
				name:  "should identify a query with different statements in a single line",
				query: "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');SELECT * FROM Persons",
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           55,
						Text:          "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         56,
						End:           76,
						Text:          "SELECT * FROM Persons",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name:  "should identify a query with different statements in multiple lines",
				query: "\n        INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');\n        SELECT * FROM Persons;\n      ",
				expected: []IdentifyResult{
					{
						Start:         9,
						End:           64,
						Text:          "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         74,
						End:           95,
						Text:          "SELECT * FROM Persons;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name: "should identify two queries with one using quoted identifier",
				query: `
								SELECT "foo'bar";
								SELECT * FROM table;
						`,
				options: IdentifyOptions{Dialect: dialect(DialectMySQL)},
				expected: []IdentifyResult{
					{
						Start:         9,
						End:           25,
						Text:          `SELECT "foo'bar";`,
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         35,
						End:           54,
						Text:          "SELECT * FROM table;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name:  "should be able to ignore empty statements (extra semicolons)",
				query: "\n        ;select 1;;select 2;;;\n        ;\n        select 3;\n      ",
				expected: []IdentifyResult{
					{
						Start:         10,
						End:           18,
						Text:          "select 1;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         20,
						End:           28,
						Text:          "select 2;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         50,
						End:           58,
						Text:          "select 3;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name: "should be able to detect queries with a CTE in middle query",
				query: `
					INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');

					WITH employee AS (SELECT * FROM Employees)
					SELECT * FROM employee WHERE ID < 20
					UNION ALL
					SELECT * FROM employee WHERE Sex = 'M';

					SELECT * FROM Persons;
				`,
				options: IdentifyOptions{Strict: strict(false)},
				expected: []IdentifyResult{
					{
						Start:         6,
						End:           61,
						Text:          "INSERT INTO Persons (PersonID, Name) VALUES (1, 'Jack');",
						Type:          StatementInsert,
						ExecutionType: ExecutionModification,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         69,
						End:           212,
						Text:          "WITH employee AS (SELECT * FROM Employees)\n\t\t\t\t\tSELECT * FROM employee WHERE ID < 20\n\t\t\t\t\tUNION ALL\n\t\t\t\t\tSELECT * FROM employee WHERE Sex = 'M';",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         220,
						End:           241,
						Text:          "SELECT * FROM Persons;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name: "should identify statements with semicolon following CTE",
				query: `with temp as (
					select * from foo
				);
				select * from foo;`,
				options: IdentifyOptions{Strict: strict(false)},
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           43,
						Text:          "with temp as (\n\t\t\t\t\tselect * from foo\n\t\t\t\t);",
						Type:          StatementUnknown,
						ExecutionType: ExecutionUnknown,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         49,
						End:           66,
						Text:          "select * from foo;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name: "should identify statements with semicolon following with keyword",
				query: `with;
					select * from foo;`,
				options: IdentifyOptions{Strict: strict(false)},
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           4,
						Text:          `with;`,
						Type:          StatementUnknown,
						ExecutionType: ExecutionUnknown,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         11,
						End:           28,
						Text:          "select * from foo;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
			{
				name: "should identify statements with semicolon inside CTE parens",
				query: `with temp as ( SELECT ;
					select * from foo`,
				options: IdentifyOptions{Strict: strict(false)},
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           22,
						Text:          `with temp as ( SELECT ;`,
						Type:          StatementUnknown,
						ExecutionType: ExecutionUnknown,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         29,
						End:           45,
						Text:          "select * from foo",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
		}

		for _, tc := range multiStatementTestCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
			})
		}

		t.Run("identify statements with anonymous blocks", func(t *testing.T) {
			anonymousBlockTestCases := []identifyTestCase{
				{
					name: "should work in strict mode",
					query: `
						DECLARE
							PK_NAME VARCHAR(200);

						BEGIN
							EXECUTE IMMEDIATE ('CREATE SEQUENCE "untitled_table8_seq"');

						SELECT
							cols.column_name INTO PK_NAME
						FROM
							all_constraints cons,
							all_cons_columns cols
						WHERE
							cons.constraint_type = 'P'
							AND cons.constraint_name = cols.constraint_name
							AND cons.owner = cols.owner
							AND cols.table_name = 'untitled_table8';

						execute immediate (
							'create or replace trigger "untitled_table8_autoinc_trg"  BEFORE INSERT on "untitled_table8"  for each row  declare  checking number := 1;  begin    if (:new."' || PK_NAME || '" is null) then      while checking >= 1 loop        select "untitled_table8_seq".nextval into :new."' || PK_NAME || '" from dual;        select count("' || PK_NAME || '") into checking from "untitled_table8"        where "' || PK_NAME || '" = :new."' || PK_NAME || '";      end loop;    end if;  end;'
						);

						END;
					`,
					options: IdentifyOptions{Dialect: dialect(DialectOracle), Strict: strict(true)},
					expected: []IdentifyResult{
						{
							Start:         7,
							End:           961,
							Text:          "DECLARE\n\t\t\t\t\t\t\tPK_NAME VARCHAR(200);\n\n\t\t\t\t\t\tBEGIN\n\t\t\t\t\t\t\tEXECUTE IMMEDIATE ('CREATE SEQUENCE \"untitled_table8_seq\"');\n\n\t\t\t\t\t\tSELECT\n\t\t\t\t\t\t\tcols.column_name INTO PK_NAME\n\t\t\t\t\t\tFROM\n\t\t\t\t\t\t\tall_constraints cons,\n\t\t\t\t\t\t\tall_cons_columns cols\n\t\t\t\t\t\tWHERE\n\t\t\t\t\t\t\tcons.constraint_type = 'P'\n\t\t\t\t\t\t\tAND cons.constraint_name = cols.constraint_name\n\t\t\t\t\t\t\tAND cons.owner = cols.owner\n\t\t\t\t\t\t\tAND cols.table_name = 'untitled_table8';\n\n\t\t\t\t\t\texecute immediate (\n\t\t\t\t\t\t\t'create or replace trigger \"untitled_table8_autoinc_trg\"  BEFORE INSERT on \"untitled_table8\"  for each row  declare  checking number := 1;  begin    if (:new.\"' || PK_NAME || '\" is null) then      while checking >= 1 loop        select \"untitled_table8_seq\".nextval into :new.\"' || PK_NAME || '\" from dual;        select count(\"' || PK_NAME || '\") into checking from \"untitled_table8\"        where \"' || PK_NAME || '\" = :new.\"' || PK_NAME || '\";      end loop;    end if;  end;'\n\t\t\t\t\t\t);\n\n\t\t\t\t\t\tEND;",
							Type:          StatementAnonBlock,
							ExecutionType: ExecutionAnonBlock,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
				{
					name: "should identify a create table then a block",
					query: `
					create table
						"untitled_table8" (
							"id" integer not null primary key,
							"created_at" varchar(255) not null
						);

					DECLARE
						PK_NAME VARCHAR(200);

					BEGIN
						EXECUTE IMMEDIATE ('CREATE SEQUENCE "untitled_table8_seq"');

					SELECT
						cols.column_name INTO PK_NAME
					FROM
						all_constraints cons,
						all_cons_columns cols
					WHERE
						cons.constraint_type = 'P'
						AND cons.constraint_name = cols.constraint_name
						AND cons.owner = cols.owner
						AND cols.table_name = 'untitled_table8';

					execute immediate (
						'create or replace trigger "untitled_table8_autoinc_trg"  BEFORE INSERT on "untitled_table8"  for each row  declare  checking number := 1;  begin    if (:new."` + "|| PK_NAME || `" + `" is null) then      while checking >= 1 loop        select "untitled_table8_seq".nextval into :new."' || PK_NAME || '" from dual;        select count("` + "|| PK_NAME || `" + `") into checking from "untitled_table8"        where "` + "|| PK_NAME || `" + `";      end loop;    end if;  end;'
					);

					END;
					`,
					options: IdentifyOptions{Dialect: dialect(DialectOracle), Strict: strict(false)},
					expected: []IdentifyResult{
						{
							Start:         6,
							End:           136,
							Text:          "create table\n\t\t\t\t\t\t\"untitled_table8\" (\n\t\t\t\t\t\t\t\"id\" integer not null primary key,\n\t\t\t\t\t\t\t\"created_at\" varchar(255) not null\n\t\t\t\t\t\t);",
							Type:          StatementCreateTable,
							ExecutionType: ExecutionModification,
							Parameters:    []string{},
							Tables:        []string{},
						},
						{
							Start:         144,
							End:           1048,
							Text:          "DECLARE\n\t\t\t\t\t\tPK_NAME VARCHAR(200);\n\n\t\t\t\t\tBEGIN\n\t\t\t\t\t\tEXECUTE IMMEDIATE ('CREATE SEQUENCE \"untitled_table8_seq\"');\n\n\t\t\t\t\tSELECT\n\t\t\t\t\t\tcols.column_name INTO PK_NAME\n\t\t\t\t\tFROM\n\t\t\t\t\t\tall_constraints cons,\n\t\t\t\t\t\tall_cons_columns cols\n\t\t\t\t\tWHERE\n\t\t\t\t\t\tcons.constraint_type = 'P'\n\t\t\t\t\t\tAND cons.constraint_name = cols.constraint_name\n\t\t\t\t\t\tAND cons.owner = cols.owner\n\t\t\t\t\t\tAND cols.table_name = 'untitled_table8';\n\n\t\t\t\t\texecute immediate (\n\t\t\t\t\t\t'create or replace trigger \"untitled_table8_autoinc_trg\"  BEFORE INSERT on \"untitled_table8\"  for each row  declare  checking number := 1;  begin    if (:new.\"|| PK_NAME || `\" is null) then      while checking >= 1 loop        select \"untitled_table8_seq\".nextval into :new.\"' || PK_NAME || '\" from dual;        select count(\"|| PK_NAME || `\") into checking from \"untitled_table8\"        where \"|| PK_NAME || `\";      end loop;    end if;  end;'\n\t\t\t\t\t);\n\n\t\t\t\t\tEND;",
							Type:          StatementAnonBlock,
							ExecutionType: ExecutionAnonBlock,
							Parameters:    []string{},
							Tables:        []string{},
						},
					},
				},
			}
			for _, tc := range anonymousBlockTestCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
				})
			}
		})
	})

	t.Run("identify transactions", func(t *testing.T) {
		transactionTestCases := []identifyTestCase{
			{
				name:    "should identify basic BEGIN TRANSACTION, SELECT, COMMIT",
				query:   "BEGIN TRANSACTION;\nSELECT 1;\nCOMMIT;",
				options: IdentifyOptions{Strict: strict(false)},
				expected: []IdentifyResult{
					{
						Start:         0,
						End:           17,
						Text:          "BEGIN TRANSACTION;",
						Type:          StatementUnknown,
						ExecutionType: ExecutionUnknown,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         19,
						End:           27,
						Text:          "SELECT 1;",
						Type:          StatementSelect,
						ExecutionType: ExecutionListing,
						Parameters:    []string{},
						Tables:        []string{},
					},
					{
						Start:         29,
						End:           35,
						Text:          "COMMIT;",
						Type:          StatementUnknown,
						ExecutionType: ExecutionUnknown,
						Parameters:    []string{},
						Tables:        []string{},
					},
				},
			},
		}

		for _, tc := range transactionTestCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				assertIdentifyResults(t, tc.query, tc.options, tc.expected, tc.expectedError)
			})
		}

		t.Run("identify keywords for sqlite transactions", func(t *testing.T) {
			for _, typeName := range []string{"DEFERRED", "IMMEDIATE", "EXCLUSIVE"} {
				t.Run(fmt.Sprintf("identifies BEGIN %s TRANSACTION", typeName), func(t *testing.T) {
					stmt0Text := fmt.Sprintf("BEGIN %s TRANSACTION;", typeName)
					stmt1Text := "SELECT 1;"
					stmt2Text := "COMMIT;"
					query := stmt0Text + "\n" + stmt1Text + "\n" + stmt2Text

					expected := []IdentifyResult{
						{
							Start:         0,
							End:           18 + len(typeName),
							Text:          stmt0Text,
							Type:          StatementUnknown,
							ExecutionType: ExecutionUnknown,
							Parameters:    []string{},
							Tables:        []string{},
						},
						{
							Start:         20 + len(typeName),
							End:           28 + len(typeName),
							Text:          stmt1Text,
							Type:          StatementSelect,
							ExecutionType: ExecutionListing,
							Parameters:    []string{},
							Tables:        []string{},
						},
						{
							Start:         30 + len(typeName),
							End:           36 + len(typeName),
							Text:          stmt2Text,
							Type:          StatementUnknown,
							ExecutionType: ExecutionUnknown,
							Parameters:    []string{},
							Tables:        []string{},
						},
					}
					assertIdentifyResults(t, query, IdentifyOptions{Dialect: dialect(DialectSQLite), Strict: strict(false)}, expected, "")
				})
			}
		})
	})
}

func TestGetExecutionType(t *testing.T) {
	t.Run("should return the correct execution type for a known command", func(t *testing.T) {
		actual := GetExecutionType(StatementSelect)
		expected := ExecutionListing
		if actual != expected {
			t.Errorf("Expected %s, but got %s", expected, actual)
		}
	})

	t.Run("should return UNKNOWN for an unknown command", func(t *testing.T) {
		actual := GetExecutionType(StatementType("UNKNOWN_COMMAND"))
		expected := ExecutionUnknown
		if actual != expected {
			t.Errorf("Expected %s, but got %s", expected, actual)
		}
	})
}
