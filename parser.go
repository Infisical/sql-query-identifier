package sqlqueryidentifier

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type StatementParser interface {
	AddToken(token Token, nextToken Token)
	GetStatement() *Statement
}

// maps statement types to their execution behavior
var ExecutionTypes = map[StatementType]ExecutionType{
	StatementSelect:          ExecutionListing,
	StatementInsert:          ExecutionModification,
	StatementDelete:          ExecutionModification,
	StatementUpdate:          ExecutionModification,
	StatementTruncate:        ExecutionModification,
	StatementCreateDatabase:  ExecutionModification,
	StatementCreateSchema:    ExecutionModification,
	StatementCreateTable:     ExecutionModification,
	StatementCreateView:      ExecutionModification,
	StatementCreateTrigger:   ExecutionModification,
	StatementCreateFunction:  ExecutionModification,
	StatementCreateIndex:     ExecutionModification,
	StatementCreateProcedure: ExecutionModification,
	StatementShowBinary:      ExecutionListing,
	StatementShowBinlog:      ExecutionListing,
	StatementShowCharacter:   ExecutionListing,
	StatementShowCollation:   ExecutionListing,
	StatementShowCreate:      ExecutionListing,
	StatementShowEngine:      ExecutionListing,
	StatementShowEngines:     ExecutionListing,
	StatementShowErrors:      ExecutionListing,
	StatementShowEvents:      ExecutionListing,
	StatementShowFunction:    ExecutionListing,
	StatementShowGrants:      ExecutionListing,
	StatementShowMaster:      ExecutionListing,
	StatementShowOpen:        ExecutionListing,
	StatementShowPlugins:     ExecutionListing,
	StatementShowPrivileges:  ExecutionListing,
	StatementShowProcedure:   ExecutionListing,
	StatementShowProcesslist: ExecutionListing,
	StatementShowProfile:     ExecutionListing,
	StatementShowProfiles:    ExecutionListing,
	StatementShowRelaylog:    ExecutionListing,
	StatementShowReplicas:    ExecutionListing,
	StatementShowSlave:       ExecutionListing,
	StatementShowReplica:     ExecutionListing,
	StatementShowStatus:      ExecutionListing,
	StatementShowTriggers:    ExecutionListing,
	StatementShowVariables:   ExecutionListing,
	StatementShowWarnings:    ExecutionListing,
	StatementShowDatabases:   ExecutionListing,
	StatementShowKeys:        ExecutionListing,
	StatementShowIndex:       ExecutionListing,
	StatementShowTable:       ExecutionListing,
	StatementShowTables:      ExecutionListing,
	StatementShowColumns:     ExecutionListing,
	StatementDropDatabase:    ExecutionModification,
	StatementDropSchema:      ExecutionModification,
	StatementDropTable:       ExecutionModification,
	StatementDropView:        ExecutionModification,
	StatementDropTrigger:     ExecutionModification,
	StatementDropFunction:    ExecutionModification,
	StatementDropIndex:       ExecutionModification,
	StatementDropProcedure:   ExecutionModification,
	StatementAlterDatabase:   ExecutionModification,
	StatementAlterSchema:     ExecutionModification,
	StatementAlterTable:      ExecutionModification,
	StatementAlterView:       ExecutionModification,
	StatementAlterTrigger:    ExecutionModification,
	StatementAlterFunction:   ExecutionModification,
	StatementAlterIndex:      ExecutionModification,
	StatementAlterProcedure:  ExecutionModification,
	StatementUnknown:         ExecutionUnknown,
	StatementAnonBlock:       ExecutionAnonBlock,
}

var statementsWithEnds = []StatementType{
	StatementCreateTrigger,
	StatementCreateFunction,
	StatementCreateProcedure,
	StatementAnonBlock,
	StatementUnknown,
}

var preTableKeywords = regexp.MustCompile(`(?i)^from$|^join$|^into$`)

var blockOpeners = map[Dialect][]string{
	DialectGeneric:  {"BEGIN", "CASE"},
	DialectPSQL:     {"BEGIN", "CASE", "LOOP", "IF"},
	DialectMySQL:    {"BEGIN", "CASE", "LOOP", "IF"},
	DialectMSSQL:    {"BEGIN", "CASE"},
	DialectSQLite:   {"BEGIN", "CASE"},
	DialectOracle:   {"DECLARE", "BEGIN", "CASE"},
	DialectBigQuery: {"BEGIN", "CASE", "IF", "LOOP", "REPEAT", "WHILE", "FOR"},
}

type ParseOptions struct {
	IsStrict       bool
	Dialect        Dialect
	IdentifyTables bool
	ParamTypes     *ParamTypes
}

type cteState struct {
	isCte        bool
	asSeen       bool
	statementEnd bool
	parens       int
	state        *State
	params       []string
}

func createInitialStatement() *Statement {
	return &Statement{
		Start:      -1,
		End:        0,
		Parameters: []string{},
		Tables:     []string{},
	}
}

func initState(input []rune, prevState *State) *State {
	if prevState != nil {
		return &State{
			Input:    prevState.Input,
			Position: prevState.Position,
			Start:    prevState.Position + 1,
			End:      len(prevState.Input) - 1,
		}
	}
	return &State{
		Input:    input,
		Position: -1,
		Start:    0,
		End:      len(input) - 1,
	}
}

func nextNonWhitespaceToken(state *State, dialect Dialect, paramTypes *ParamTypes) Token {
	var token Token
	for {
		s := initState(nil, state)
		token = ScanToken(s, dialect, paramTypes)
		state = s
		if token.Type != TokenWhitespace {
			break
		}
	}
	return token
}

func Parse(input string, isStrict bool, dialect Dialect, identifyTables bool, paramTypes *ParamTypes) *ParseResult {
	inputRunes := []rune(input)
	topLevelState := initState(inputRunes, nil)
	topLevelResult := &ParseResult{
		Type:   "QUERY",
		Start:  0,
		End:    len(inputRunes) - 1,
		Body:   []ConcreteStatement{},
		Tokens: []Token{},
	}

	prevState := topLevelState
	var statementParser StatementParser

	cte := &cteState{
		state: topLevelState,
	}

	ignoreOutsideBlankTokens := []TokenType{
		TokenWhitespace,
		TokenCommentInline,
		TokenCommentBlock,
		TokenSemicolon,
	}

	for prevState.Position < topLevelState.End {
		tokenState := initState(nil, prevState)
		token := ScanToken(tokenState, dialect, paramTypes)
		nextToken := nextNonWhitespaceToken(tokenState, dialect, paramTypes)

		if statementParser == nil {
			// ignore blank tokens before the start of a CTE / not part of a statement
			if !cte.isCte && slices.Contains(ignoreOutsideBlankTokens, token.Type) {
				topLevelResult.Tokens = append(topLevelResult.Tokens, token)
				prevState = tokenState
			} else if !cte.isCte && token.Type == TokenKeyword && strings.ToUpper(token.Value) == "WITH" {
				cte.isCte = true
				topLevelResult.Tokens = append(topLevelResult.Tokens, token)
				cte.state = tokenState
				prevState = tokenState

				// if a semicolon is encountered while parsing a CTE definition, treat it as a premature
				// termination of the CTE block
			} else if cte.isCte && token.Type == TokenSemicolon {
				topLevelResult.Tokens = append(topLevelResult.Tokens, token)
				prevState = tokenState
				topLevelResult.Body = append(topLevelResult.Body, ConcreteStatement{
					Start:         cte.state.Start,
					End:           token.End,
					Type:          StatementUnknown,
					ExecutionType: ExecutionUnknown,
					Parameters:    []string{},
					Tables:        []string{},
				})
				cte.isCte = false
				cte.asSeen = false
				cte.statementEnd = false
				cte.parens = 0
			} else if cte.isCte && !cte.statementEnd {
				if cte.asSeen {
					switch token.Value {
					case "(":
						cte.parens++
					case ")":
						cte.parens--
						if cte.parens == 0 {
							cte.statementEnd = true
						}
					}
				} else if strings.ToUpper(token.Value) == "AS" {
					cte.asSeen = true
				}
				topLevelResult.Tokens = append(topLevelResult.Tokens, token)
				prevState = tokenState
			} else if cte.isCte && cte.statementEnd && token.Value == "," {
				cte.asSeen = false
				cte.statementEnd = false
				topLevelResult.Tokens = append(topLevelResult.Tokens, token)
				prevState = tokenState
			} else if cte.isCte && cte.statementEnd && slices.Contains(ignoreOutsideBlankTokens, token.Type) {
				topLevelResult.Tokens = append(topLevelResult.Tokens, token)
				prevState = tokenState
			} else {
				statementParser = createStatementParserByToken(token, nextToken, ParseOptions{
					IsStrict:       isStrict,
					Dialect:        dialect,
					IdentifyTables: identifyTables,
					ParamTypes:     paramTypes,
				})
				if cte.isCte {
					stmt := statementParser.GetStatement()
					stmt.Start = cte.state.Start
					isCte := true
					stmt.IsCte = &isCte
					stmt.Parameters = append(stmt.Parameters, cte.params...)
					cte.params = []string{}
					cte.isCte = false
					cte.asSeen = false
					cte.statementEnd = false
				}
			}

			if cte.isCte && token.Type == TokenParameter {
				cte.params = append(cte.params, token.Value)
			}
		} else {
			statementParser.AddToken(token, nextToken)
			topLevelResult.Tokens = append(topLevelResult.Tokens, token)
			prevState = tokenState

			statement := statementParser.GetStatement()
			if statement.EndStatement != nil {
				statement.End = token.End
				topLevelResult.Body = append(topLevelResult.Body, statement.ToConcrete())
				statementParser = nil
			}
		}
	}

	// last statement without ending key
	if statementParser != nil {
		statement := statementParser.GetStatement()
		if statement.EndStatement == nil {
			statement.End = topLevelResult.End
			topLevelResult.Body = append(topLevelResult.Body, statement.ToConcrete())
		}
	}

	return topLevelResult
}

func createStatementParserByToken(token Token, nextToken Token, options ParseOptions) StatementParser {
	if token.Type == TokenKeyword {
		switch strings.ToUpper(token.Value) {
		case "SELECT":
			return createSelectStatementParser(options)
		case "CREATE":
			return createCreateStatementParser(options)
		case "SHOW":
			if options.Dialect == DialectMySQL || options.Dialect == DialectGeneric {
				return createShowStatementParser(options)
			}
		case "DROP":
			return createDropStatementParser(options)
		case "ALTER":
			return createAlterStatementParser(options)
		case "INSERT":
			return createInsertStatementParser(options)
		case "UPDATE":
			return createUpdateStatementParser(options)
		case "DELETE":
			return createDeleteStatementParser(options)
		case "TRUNCATE":
			return createTruncateStatementParser(options)
		case "BEGIN":
			if (options.Dialect == DialectBigQuery || options.Dialect == DialectOracle) && strings.ToUpper(nextToken.Value) != "TRANSACTION" {
				return createBlockStatementParser(options)
			}
		case "DECLARE":
			if options.Dialect == DialectOracle {
				return createBlockStatementParser(options)
			}
		}
	}

	if !options.IsStrict {
		return createUnknownStatementParser(options)
	}

	panic(fmt.Sprintf("Invalid statement parser \"%s\"", token.Value))
}

func createSelectStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "SELECT"}},
			},
			Add: func(token Token) {
				statementType := StatementSelect
				statement.Type = &statementType
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createBlockStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	statementType := StatementAnonBlock
	statement.Type = &statementType

	acceptTokens := []AcceptToken{}
	if options.Dialect == DialectOracle {
		acceptTokens = append(acceptTokens, AcceptToken{Type: "keyword", Value: "DECLARE"})
	}
	acceptTokens = append(acceptTokens, AcceptToken{Type: "keyword", Value: "BEGIN"})

	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: acceptTokens,
			},
			Add: func(token Token) {
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createInsertStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "INSERT"}},
			},
			Add: func(token Token) {
				statementType := StatementInsert
				statement.Type = &statementType
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createUpdateStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "UPDATE"}},
			},
			Add: func(token Token) {
				statementType := StatementUpdate
				statement.Type = &statementType
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createDeleteStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "DELETE"}},
			},
			Add: func(token Token) {
				statementType := StatementDelete
				statement.Type = &statementType
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createCreateStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	var acceptTokens []AcceptToken
	if options.Dialect != DialectSQLite {
		acceptTokens = append(acceptTokens,
			AcceptToken{Type: "keyword", Value: "DATABASE"},
			AcceptToken{Type: "keyword", Value: "SCHEMA"},
			AcceptToken{Type: "keyword", Value: "PROCEDURE"},
		)
	}
	acceptTokens = append(acceptTokens,
		AcceptToken{Type: "keyword", Value: "TABLE"},
		AcceptToken{Type: "keyword", Value: "VIEW"},
		AcceptToken{Type: "keyword", Value: "TRIGGER"},
		AcceptToken{Type: "keyword", Value: "FUNCTION"},
		AcceptToken{Type: "keyword", Value: "INDEX"},
	)

	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "CREATE"}},
			},
			Add: func(token Token) {
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				RequireBefore: []string{string(TokenWhitespace)},
				AcceptTokens:  acceptTokens,
			},
			Add: func(token Token) {
				statementType := StatementType("CREATE_" + strings.ToUpper(token.Value))
				statement.Type = &statementType
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createDropStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	var acceptTokens []AcceptToken
	if options.Dialect != DialectSQLite {
		acceptTokens = append(acceptTokens,
			AcceptToken{Type: "keyword", Value: "DATABASE"},
			AcceptToken{Type: "keyword", Value: "SCHEMA"},
			AcceptToken{Type: "keyword", Value: "PROCEDURE"},
		)
	}
	acceptTokens = append(acceptTokens,
		AcceptToken{Type: "keyword", Value: "TABLE"},
		AcceptToken{Type: "keyword", Value: "VIEW"},
		AcceptToken{Type: "keyword", Value: "TRIGGER"},
		AcceptToken{Type: "keyword", Value: "FUNCTION"},
		AcceptToken{Type: "keyword", Value: "INDEX"},
	)

	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "DROP"}},
			},
			Add: func(token Token) {
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				RequireBefore: []string{string(TokenWhitespace)},
				AcceptTokens:  acceptTokens,
			},
			Add: func(token Token) {
				statementType := StatementType("DROP_" + strings.ToUpper(token.Value))
				statement.Type = &statementType
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createAlterStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	var acceptTokens []AcceptToken
	if options.Dialect != DialectSQLite {
		acceptTokens = append(acceptTokens,
			AcceptToken{Type: "keyword", Value: "DATABASE"},
			AcceptToken{Type: "keyword", Value: "SCHEMA"},
			AcceptToken{Type: "keyword", Value: "TRIGGER"},
			AcceptToken{Type: "keyword", Value: "FUNCTION"},
			AcceptToken{Type: "keyword", Value: "INDEX"},
		)
		if options.Dialect != DialectBigQuery {
			acceptTokens = append(acceptTokens, AcceptToken{Type: "keyword", Value: "PROCEDURE"})
		}
	}
	acceptTokens = append(acceptTokens,
		AcceptToken{Type: "keyword", Value: "TABLE"},
		AcceptToken{Type: "keyword", Value: "VIEW"},
	)

	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "ALTER"}},
			},
			Add: func(token Token) {
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				RequireBefore: []string{string(TokenWhitespace)},
				AcceptTokens:  acceptTokens,
			},
			Add: func(token Token) {
				statementType := StatementType("ALTER_" + strings.ToUpper(token.Value))
				statement.Type = &statementType
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createTruncateStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "TRUNCATE"}},
			},
			Add: func(token Token) {
				statementType := StatementTruncate
				statement.Type = &statementType
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createShowStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				AcceptTokens: []AcceptToken{{Type: "keyword", Value: "SHOW"}},
			},
			Add: func(token Token) {
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Validation: &StepValidation{
				RequireBefore: []string{string(TokenWhitespace)},
				AcceptTokens: []AcceptToken{
					{Type: "keyword", Value: "DATABASES"}, {Type: "keyword", Value: "DATABASE"},
					{Type: "keyword", Value: "KEYS"}, {Type: "keyword", Value: "INDEX"},
					{Type: "keyword", Value: "COLUMNS"}, {Type: "keyword", Value: "TABLES"},
					{Type: "keyword", Value: "TABLE"}, {Type: "keyword", Value: "BINARY"},
					{Type: "keyword", Value: "BINLOG"}, {Type: "keyword", Value: "CHARACTER"},
					{Type: "keyword", Value: "COLLATION"}, {Type: "keyword", Value: "CREATE"},
					{Type: "keyword", Value: "ENGINE"}, {Type: "keyword", Value: "ENGINES"},
					{Type: "keyword", Value: "ERRORS"}, {Type: "keyword", Value: "EVENTS"},
					{Type: "keyword", Value: "FUNCTION"}, {Type: "keyword", Value: "GRANTS"},
					{Type: "keyword", Value: "MASTER"}, {Type: "keyword", Value: "OPEN"},
					{Type: "keyword", Value: "PLUGINS"}, {Type: "keyword", Value: "PRIVILEGES"},
					{Type: "keyword", Value: "PROCEDURE"}, {Type: "keyword", Value: "PROCESSLIST"},
					{Type: "keyword", Value: "PROFILE"}, {Type: "keyword", Value: "PROFILES"},
					{Type: "keyword", Value: "RELAYLOG"}, {Type: "keyword", Value: "REPLICAS"},
					{Type: "keyword", Value: "REPLICA"}, {Type: "keyword", Value: "SLAVE"},
					{Type: "keyword", Value: "STATUS"}, {Type: "keyword", Value: "TRIGGERS"},
					{Type: "keyword", Value: "VARIABLES"}, {Type: "keyword", Value: "WARNINGS"},
				},
			},
			Add: func(token Token) {
				statementType := StatementType("SHOW_" + strings.ToUpper(strings.ReplaceAll(token.Value, " ", "_")))
				statement.Type = &statementType
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

func createUnknownStatementParser(options ParseOptions) StatementParser {
	statement := createInitialStatement()
	steps := []Step{
		{
			PreCanGoToNext: func(token *Token) bool { return false },
			Add: func(token Token) {
				statementType := StatementUnknown
				statement.Type = &statementType
				if statement.Start < 0 {
					statement.Start = token.Start
				}
			},
			PostCanGoToNext: func(token *Token) bool { return true },
		},
	}
	return stateMachineStatementParser(statement, steps, options)
}

type stateMachineParser struct {
	statement              *Statement
	steps                  []Step
	options                ParseOptions
	currentStepIndex       int
	prevToken              *Token
	prevNonWhitespaceToken *Token
	lastBlockOpener        *Token
	anonBlockStarted       bool
	openBlocks             int
}

func (p *stateMachineParser) GetStatement() *Statement {
	return p.statement
}

func (p *stateMachineParser) setPrevToken(token Token) {
	p.prevToken = &token
	if token.Type != TokenWhitespace {
		p.prevNonWhitespaceToken = &token
	}
}

func (p *stateMachineParser) isValidToken(step Step, token Token) bool {
	if step.Validation == nil {
		return true
	}
	for _, accept := range step.Validation.AcceptTokens {
		isValidType := token.Type == TokenType(accept.Type)
		isValidValue := accept.Value == "" || strings.ToUpper(token.Value) == accept.Value
		if isValidType && isValidValue {
			return true
		}
	}
	return false
}

func (p *stateMachineParser) AddToken(token Token, nextToken Token) {
	if p.statement.EndStatement != nil {
		panic("This statement has already got to the end.")
	}

	statementTypeEnds := false
	if p.statement.Type != nil {
		if slices.Contains(statementsWithEnds, *p.statement.Type) {
			statementTypeEnds = true
		}
	}

	if token.Type == TokenSemicolon &&
		(!statementTypeEnds || (p.openBlocks == 0 && (*p.statement.Type == StatementUnknown || (p.statement.CanEnd != nil && *p.statement.CanEnd)))) {
		end := ";"
		p.statement.EndStatement = &end
		return
	}

	if p.openBlocks > 0 && strings.ToUpper(token.Value) == "END" {
		p.openBlocks--
		if p.openBlocks == 0 {
			canEnd := true
			p.statement.CanEnd = &canEnd
		}
		p.setPrevToken(token)
		return
	}

	if token.Type == TokenWhitespace {
		p.setPrevToken(token)
		return
	}

	if token.Type == TokenKeyword {
		upperVal := strings.ToUpper(token.Value)
		isBlockOpener := slices.Contains(blockOpeners[p.options.Dialect], upperVal)
		if isBlockOpener && (p.prevNonWhitespaceToken == nil || strings.ToUpper(p.prevNonWhitespaceToken.Value) != "END") {
			canOpenBlock := false
			if upperVal != "BEGIN" {
				canOpenBlock = true
			} else {
				if strings.ToUpper(nextToken.Value) != "TRANSACTION" {
					if p.options.Dialect != DialectSQLite {
						canOpenBlock = true
					} else {
						if !(strings.ToUpper(nextToken.Value) == "DEFERRED" || strings.ToUpper(nextToken.Value) == "IMMEDIATE" || strings.ToUpper(nextToken.Value) == "EXCLUSIVE") {
							canOpenBlock = true
						}
					}
				}
			}

			if canOpenBlock {
				if p.options.Dialect == DialectOracle && p.lastBlockOpener != nil && p.lastBlockOpener.Value == "DECLARE" && upperVal == "BEGIN" {
					p.setPrevToken(token)
					p.lastBlockOpener = &token
					return
				}
				p.openBlocks++
				p.lastBlockOpener = &token
				p.setPrevToken(token)
				if p.statement.Type != nil && *p.statement.Type == StatementAnonBlock && !p.anonBlockStarted {
					p.anonBlockStarted = true
				} else if p.statement.Type != nil {
					return
				}
			}
		}
	}

	if p.options.IdentifyTables && preTableKeywords.MatchString(token.Value) && (p.statement.IsCte == nil || !*p.statement.IsCte) {
		if p.statement.Type != nil && (*p.statement.Type == StatementSelect || *p.statement.Type == StatementInsert) {
			tableValue := nextToken.Value
			if !slices.Contains(p.statement.Tables, tableValue) {
				p.statement.Tables = append(p.statement.Tables, tableValue)
			}
		}
	}

	if token.Type == TokenParameter {
		if token.Value == "?" || !slices.Contains(p.statement.Parameters, token.Value) {
			p.statement.Parameters = append(p.statement.Parameters, token.Value)
		}
	}

	if p.statement.Type != nil && p.statement.Start >= 0 {
		p.setPrevToken(token)
		return
	}

	upperValue := strings.ToUpper(token.Value)
	if upperValue == "UNIQUE" ||
		(p.options.Dialect == DialectMySQL && (upperValue == "FULLTEXT" || upperValue == "SPATIAL")) ||
		(p.options.Dialect == DialectMSSQL && (upperValue == "CLUSTERED" || upperValue == "NONCLUSTERED")) {
		p.setPrevToken(token)
		return
	}

	if (p.options.Dialect == DialectPSQL || p.options.Dialect == DialectMSSQL || p.options.Dialect == DialectBigQuery) && upperValue == "MATERIALIZED" {
		p.setPrevToken(token)
		return
	}

	if p.options.Dialect != DialectSQLite {
		prevIsOr := p.prevNonWhitespaceToken != nil && strings.ToUpper(p.prevNonWhitespaceToken.Value) == "OR"
		isAlterOrReplace := false
		if p.options.Dialect == DialectMSSQL {
			isAlterOrReplace = upperValue == "ALTER"
		} else {
			isAlterOrReplace = upperValue == "REPLACE"
		}

		if upperValue == "OR" || (prevIsOr && isAlterOrReplace) {
			p.setPrevToken(token)
			return
		}
	}

	if (p.options.Dialect == DialectPSQL && (upperValue == "TEMP" || upperValue == "TEMPORARY")) ||
		(p.options.Dialect == DialectSQLite && (upperValue == "TEMP" || upperValue == "TEMPORARY" || upperValue == "VIRTUAL")) {
		p.setPrevToken(token)
		return
	}

	if p.options.Dialect == DialectMySQL && upperValue == "DEFINER" {
		definer := 0
		p.statement.Definer = &definer
		p.setPrevToken(token)
		return
	}

	if p.statement.Definer != nil && *p.statement.Definer == 0 && token.Value == "=" {
		*p.statement.Definer++
		p.setPrevToken(token)
		return
	}

	if p.statement.Definer != nil && *p.statement.Definer > 0 {
		if *p.statement.Definer == 1 && p.prevToken != nil && p.prevToken.Type == TokenWhitespace {
			*p.statement.Definer++
			p.setPrevToken(token)
			return
		}
		if *p.statement.Definer > 1 && p.prevToken != nil && p.prevToken.Type != TokenWhitespace {
			p.setPrevToken(token)
			return
		}
		p.statement.Definer = nil
	}

	if p.options.Dialect == DialectMySQL && upperValue == "ALGORITHM" {
		algorithm := 0
		p.statement.Algorithm = &algorithm
		p.setPrevToken(token)
		return
	}

	if p.statement.Algorithm != nil && *p.statement.Algorithm == 0 && token.Value == "=" {
		*p.statement.Algorithm++
		p.setPrevToken(token)
		return
	}

	if p.statement.Algorithm != nil && *p.statement.Algorithm > 0 {
		if *p.statement.Algorithm == 1 && p.prevToken != nil && p.prevToken.Type == TokenWhitespace {
			*p.statement.Algorithm++
			p.setPrevToken(token)
			return
		}
		if p.statement.Algorithm != nil && *p.statement.Algorithm > 1 && p.prevToken != nil {
			if slices.Contains([]string{"UNDEFINED", "MERGE", "TEMPTABLE"}, strings.ToUpper(p.prevToken.Value)) {
				p.setPrevToken(token)
				return
			}
		}
		p.statement.Algorithm = nil
	}

	if p.options.Dialect == DialectMySQL && upperValue == "SQL" {
		sqlSecurity := 0
		p.statement.SQLSecurity = &sqlSecurity
		p.setPrevToken(token)
		return
	}

	if p.statement.SQLSecurity != nil {
		if (*p.statement.SQLSecurity == 0 && upperValue == "SECURITY") ||
			(*p.statement.SQLSecurity == 1 && (upperValue == "DEFINER" || upperValue == "INVOKER")) {
			*p.statement.SQLSecurity++
			p.setPrevToken(token)
			return
		} else if *p.statement.SQLSecurity == 2 {
			p.statement.SQLSecurity = nil
		}
	}

	currentStep := p.steps[p.currentStepIndex]
	if currentStep.PreCanGoToNext(&token) {
		p.currentStepIndex++
		currentStep = p.steps[p.currentStepIndex]
	}

	if p.prevToken != nil && currentStep.Validation != nil && len(currentStep.Validation.RequireBefore) > 0 {
		if !slices.Contains(currentStep.Validation.RequireBefore, string(p.prevToken.Type)) {
			requiredTokenTypes := strings.Join(currentStep.Validation.RequireBefore, " or ")
			panic(fmt.Sprintf("Expected any of these tokens %s before \"%s\" (currentStep=%d).", requiredTokenTypes, token.Value, p.currentStepIndex))
		}
	}

	if !p.isValidToken(currentStep, token) && p.options.IsStrict {
		var expectedTokenStrings []string
		if currentStep.Validation != nil {
			for _, accept := range currentStep.Validation.AcceptTokens {
				expectedTokenStrings = append(expectedTokenStrings, fmt.Sprintf("(type=\"%s\" value=\"%s\")", accept.Type, accept.Value))
			}
		}
		panic(fmt.Sprintf("Expected any of these tokens %s instead of type=\"%s\" value=\"%s\" (currentStep=%d).", strings.Join(expectedTokenStrings, " or "), token.Type, token.Value, p.currentStepIndex))
	}

	currentStep.Add(token)

	if p.statement.Type != nil {
		execType, ok := ExecutionTypes[*p.statement.Type]
		if ok {
			p.statement.ExecutionType = &execType
		} else {
			unknownExecType := ExecutionUnknown
			p.statement.ExecutionType = &unknownExecType
		}
	} else {
		unknown := ExecutionUnknown
		p.statement.ExecutionType = &unknown
	}

	if currentStep.PostCanGoToNext(&token) {
		p.currentStepIndex++
	}

	p.setPrevToken(token)
}

func stateMachineStatementParser(statement *Statement, steps []Step, options ParseOptions) StatementParser {
	return &stateMachineParser{
		statement: statement,
		steps:     steps,
		options:   options,
	}
}

// returns the default parameter types for a given SQL dialect
func DefaultParamTypesFor(dialect Dialect) *ParamTypes {
	switch dialect {
	case DialectPSQL:
		return &ParamTypes{Numbered: []rune{'$'}}
	case DialectMSSQL:
		return &ParamTypes{Named: []rune{':'}}
	case DialectBigQuery:
		positional := true
		return &ParamTypes{Positional: &positional, Named: []rune{'@'}, Quoted: []rune{'@'}}
	case DialectSQLite:
		positional := true
		return &ParamTypes{Positional: &positional, Numbered: []rune{'?'}, Named: []rune{':', '@'}}
	default:
		positional := true
		return &ParamTypes{Positional: &positional}
	}
}
