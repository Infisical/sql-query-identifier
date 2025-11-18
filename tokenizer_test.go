package sqlqueryidentifier

import (
	"fmt"
	"reflect"
	"testing"
)

func TestScanToken(t *testing.T) {
	trueBool := true
	genericParamTypes := &ParamTypes{Positional: &trueBool}

	type testCase struct {
		name       string
		input      string
		dialect    Dialect
		paramTypes *ParamTypes
		expected   Token
	}

	testCases := []testCase{
		{
			name:       "scans inline comments",
			input:      "-- my comment",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenCommentInline, Value: "-- my comment", Start: 0, End: 12},
		},
		{
			name:       "scans block comments",
			input:      "/*\n * This is my comment block\n */",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenCommentBlock, Value: "/*\n * This is my comment block\n */", Start: 0, End: 33},
		},
		{
			name:       "scans white spaces",
			input:      "   \n\t\r  ",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenWhitespace, Value: "   \n\t\r  ", Start: 0, End: 7},
		},
		{
			name:       "scans SELECT keyword",
			input:      "SELECT",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenKeyword, Value: "SELECT", Start: 0, End: 5},
		},
		{
			name:       `scans quoted keyword`,
			input:      `"ta;'` + "`" + `ble"`,
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenKeyword, Value: `"ta;'` + "`" + `ble"`, Start: 0, End: 9},
		},
		{
			name:       "scans quoted string",
			input:      `'some string; I "love it"'`,
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenString, Value: `'some string; I "love it"'`, Start: 0, End: 25},
		},
		{
			name:       "scans quoted string with escaped quotes",
			input:      `'''foo'' bar'`,
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenString, Value: `'''foo'' bar'`, Start: 0, End: 12},
		},
		{
			name:       "skips unknown tokens",
			input:      "*",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenUnknown, Value: "*", Start: 0, End: 0},
		},
		{
			name:       "scans ; individual identifier",
			input:      ";",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenSemicolon, Value: ";", Start: 0, End: 0},
		},
		{
			name:       "scans string with underscore as one token",
			input:      "end_date",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenUnknown, Value: "end_date", Start: 0, End: 7},
		},
		{
			name:       "scans dollar quoted string",
			input:      "$$test$$",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenString, Value: "$$test$$", Start: 0, End: 7},
		},
		{
			name:       "scans dollar quoted string with label",
			input:      "$aaa$test$aaa$",
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenString, Value: "$aaa$test$aaa$", Start: 0, End: 13},
		},
	}

	keywords := []string{"INSERT", "DELETE", "UPDATE", "CREATE", "DROP", "TABLE", "VIEW", "DATABASE", "TRUNCATE", "ALTER"}
	for _, kw := range keywords {
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("scans %s keyword", kw),
			input:      kw,
			dialect:    DialectGeneric,
			paramTypes: genericParamTypes,
			expected:   Token{Type: TokenKeyword, Value: kw, Start: 0, End: len(kw) - 1},
		})
	}

	for _, chDialect := range []struct {
		ch      string
		dialect Dialect
	}{{"?", DialectGeneric}, {"?", DialectMySQL}, {"?", DialectSQLite}, {":", DialectMSSQL}} {
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("scans just %s as parameter for %s", chDialect.ch, chDialect.dialect),
			input:      chDialect.ch,
			dialect:    chDialect.dialect,
			paramTypes: DefaultParamTypesFor(chDialect.dialect),
			expected:   Token{Type: TokenParameter, Value: chDialect.ch, Start: 0, End: 0},
		})
	}
	testCases = append(testCases, testCase{
		name:       "does not scan just $ as parameter for psql",
		input:      "$",
		dialect:    DialectPSQL,
		paramTypes: DefaultParamTypesFor(DialectPSQL),
		expected:   Token{Type: TokenUnknown, Value: "$", Start: 0, End: 0},
	})

	for _, chDialect := range []struct {
		ch      string
		dialect Dialect
	}{{"?", DialectGeneric}, {"?", DialectMySQL}} {
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("should only scan %s from %s1 for %s", chDialect.ch, chDialect.ch, chDialect.dialect),
			input:      chDialect.ch + "1",
			dialect:    chDialect.dialect,
			paramTypes: DefaultParamTypesFor(chDialect.dialect),
			expected:   Token{Type: TokenParameter, Value: chDialect.ch, Start: 0, End: 0},
		})
	}

	for _, chDialect := range []struct {
		ch      string
		dialect Dialect
	}{{"$", DialectPSQL}, {":", DialectMSSQL}} {
		input := chDialect.ch + "1"
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("should scan %s1 for %s", chDialect.ch, chDialect.dialect),
			input:      input,
			dialect:    chDialect.dialect,
			paramTypes: DefaultParamTypesFor(chDialect.dialect),
			expected:   Token{Type: TokenParameter, Value: input, Start: 0, End: len(input) - 1},
		})
	}
	testCases = append(testCases, testCase{
		name:       "should not scan $a for psql",
		input:      "$a",
		dialect:    DialectPSQL,
		paramTypes: DefaultParamTypesFor(DialectPSQL),
		expected:   Token{Type: TokenUnknown, Value: "$", Start: 0, End: 0},
	})
	testCases = append(testCases, testCase{
		name:       "should not include trailing non-numbers for psql",
		input:      "$1,",
		dialect:    DialectPSQL,
		paramTypes: DefaultParamTypesFor(DialectPSQL),
		expected:   Token{Type: TokenParameter, Value: "$1", Start: 0, End: 1},
	})
	testCases = append(testCases, testCase{
		name:       "should not include trailing non-alphanumerics for mssql :one,",
		input:      ":one,",
		dialect:    DialectMSSQL,
		paramTypes: DefaultParamTypesFor(DialectMSSQL),
		expected:   Token{Type: TokenParameter, Value: ":one", Start: 0, End: 3},
	}, testCase{
		name:       "should not include trailing non-alphanumerics for mssql :two)",
		input:      ":two)",
		dialect:    DialectMSSQL,
		paramTypes: DefaultParamTypesFor(DialectMSSQL),
		expected:   Token{Type: TokenParameter, Value: ":two", Start: 0, End: 3},
	})

	allDialects := []Dialect{DialectMSSQL, DialectPSQL, DialectOracle, DialectBigQuery, DialectSQLite, DialectMySQL, DialectGeneric}

	for _, d := range allDialects {
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("should allow positional parameters for %s", d),
			input:      "?",
			dialect:    d,
			paramTypes: &ParamTypes{Positional: &trueBool},
			expected:   Token{Type: TokenParameter, Value: "?", Start: 0, End: 0},
		})
	}

	numberedParamTypes := &ParamTypes{Numbered: []rune{'$', '?', ':'}}
	for _, d := range allDialects {
		for _, p := range numberedParamTypes.Numbered {
			testCases = append(testCases, testCase{
				name:       fmt.Sprintf("should allow numeric parameters for %s - %c numeric", d, p),
				input:      string(p) + "1",
				dialect:    d,
				paramTypes: numberedParamTypes,
				expected:   Token{Type: TokenParameter, Value: string(p) + "1", Start: 0, End: 1},
			})
		}
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("numeric trailing alpha for %s", d),
			input:      "$123hello",
			dialect:    d,
			paramTypes: numberedParamTypes,
			expected:   Token{Type: TokenUnknown, Value: "$", Start: 0, End: 0},
		})
	}

	namedParamTypes := &ParamTypes{Named: []rune{'$', '@', ':'}}
	for _, d := range allDialects {
		for _, p := range namedParamTypes.Named {
			testCases = append(testCases, testCase{
				name:       fmt.Sprintf("should allow named parameters for %s - %c named", d, p),
				input:      string(p) + "namedParam",
				dialect:    d,
				paramTypes: namedParamTypes,
				expected:   Token{Type: TokenParameter, Value: string(p) + "namedParam", Start: 0, End: len(string(p)) + 9},
			})
		}
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("named starting with numbers for %s", d),
			input:      "$123hello",
			dialect:    d,
			paramTypes: namedParamTypes,
			expected:   Token{Type: TokenParameter, Value: "$123hello", Start: 0, End: 8},
		})
	}

	quotedParamTypes := &ParamTypes{Quoted: []rune{'$', '@', ':'}}
	dialectQuotes := map[Dialect][]string{
		DialectMSSQL:    {`""`, `[]`},
		DialectPSQL:     {`""`, "``"},
		DialectOracle:   {`""`, "``"},
		DialectBigQuery: {`""`, "``"},
		DialectSQLite:   {`""`, "``"},
		DialectMySQL:    {`""`, "``"},
		DialectGeneric:  {`""`, "``"},
	}
	for d, quotes := range dialectQuotes {
		for _, p := range quotedParamTypes.Quoted {
			for _, q := range quotes {
				val := fmt.Sprintf(`%c%cquoted param%c`, p, q[0], q[1])
				testCases = append(testCases, testCase{
					name:       fmt.Sprintf("should allow quoted parameters for %s - %c quoted with %s", d, p, q),
					input:      val,
					dialect:    d,
					paramTypes: quotedParamTypes,
					expected:   Token{Type: TokenParameter, Value: val, Start: 0, End: len(val) - 1},
				})
			}
		}
	}

	customParamTypes := &ParamTypes{Custom: []string{`\{[a-zA-Z0-9_]+\}`}}
	for _, d := range allDialects {
		testCases = append(testCases, testCase{
			name:       fmt.Sprintf("should allow custom parameters for %s", d),
			input:      "{namedParam}",
			dialect:    d,
			paramTypes: customParamTypes,
			expected:   Token{Type: TokenParameter, Value: "{namedParam}", Start: 0, End: 11},
		})
	}

	collisionParamTypes := &ParamTypes{
		Positional: &trueBool,
		Numbered:   []rune{':'},
		Named:      []rune{':'},
		Quoted:     []rune{':'},
		Custom:     []string{`\{[a-zA-Z0-9_]+\}`},
	}
	collisionTests := []testCase{
		{
			name:     "parameter types don't collide, finds positional",
			input:    "?",
			expected: Token{Type: TokenParameter, Value: "?", Start: 0, End: 0},
		},
		{
			name:     "parameter types don't collide, finds numeric",
			input:    ":123",
			expected: Token{Type: TokenParameter, Value: ":123", Start: 0, End: 3},
		},
		{
			name:     "parameter types don't collide, finds named",
			input:    ":123hello",
			expected: Token{Type: TokenParameter, Value: ":123hello", Start: 0, End: 8},
		},
		{
			name:     `parameter types don't collide, finds quoted`,
			input:    `:"named param"`,
			expected: Token{Type: TokenParameter, Value: `:"named param"`, Start: 0, End: 13},
		},
		{
			name:     "parameter types don't collide, finds custom",
			input:    "{namedParam}",
			expected: Token{Type: TokenParameter, Value: "{namedParam}", Start: 0, End: 11},
		},
	}
	for _, tt := range collisionTests {
		tt.dialect = DialectMSSQL
		tt.paramTypes = collisionParamTypes
		testCases = append(testCases, tt)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runes := []rune(tc.input)
			state := &State{
				Input:    runes,
				Position: -1,
				Start:    0,
				End:      len(runes) - 1,
			}

			token := ScanToken(state, tc.dialect, tc.paramTypes)

			if !reflect.DeepEqual(token, tc.expected) {
				t.Errorf("Expected token %+v, but got %+v", tc.expected, token)
			}
		})
	}
}
