package sqlqueryidentifier

// represents a specific SQL dialect
type Dialect string

const (
	DialectMSSQL    Dialect = "mssql"
	DialectSQLite   Dialect = "sqlite"
	DialectMySQL    Dialect = "mysql"
	DialectOracle   Dialect = "oracle"
	DialectPSQL     Dialect = "psql"
	DialectBigQuery Dialect = "bigquery"
	DialectGeneric  Dialect = "generic"
)

var DIALECTS = []Dialect{
	DialectMSSQL,
	DialectSQLite,
	DialectMySQL,
	DialectOracle,
	DialectPSQL,
	DialectBigQuery,
	DialectGeneric,
}

// represents the type of a SQL statement (e.g., SELECT, INSERT)
type StatementType string

const (
	StatementInsert          StatementType = "INSERT"
	StatementUpdate          StatementType = "UPDATE"
	StatementDelete          StatementType = "DELETE"
	StatementSelect          StatementType = "SELECT"
	StatementTruncate        StatementType = "TRUNCATE"
	StatementCreateDatabase  StatementType = "CREATE_DATABASE"
	StatementCreateSchema    StatementType = "CREATE_SCHEMA"
	StatementCreateTable     StatementType = "CREATE_TABLE"
	StatementCreateView      StatementType = "CREATE_VIEW"
	StatementCreateTrigger   StatementType = "CREATE_TRIGGER"
	StatementCreateFunction  StatementType = "CREATE_FUNCTION"
	StatementCreateIndex     StatementType = "CREATE_INDEX"
	StatementCreateProcedure StatementType = "CREATE_PROCEDURE"
	StatementShowBinary      StatementType = "SHOW_BINARY"
	StatementShowBinlog      StatementType = "SHOW_BINLOG"
	StatementShowCharacter   StatementType = "SHOW_CHARACTER"
	StatementShowCollation   StatementType = "SHOW_COLLATION"
	StatementShowCreate      StatementType = "SHOW_CREATE"
	StatementShowEngine      StatementType = "SHOW_ENGINE"
	StatementShowEngines     StatementType = "SHOW_ENGINES"
	StatementShowErrors      StatementType = "SHOW_ERRORS"
	StatementShowEvents      StatementType = "SHOW_EVENTS"
	StatementShowFunction    StatementType = "SHOW_FUNCTION"
	StatementShowGrants      StatementType = "SHOW_GRANTS"
	StatementShowMaster      StatementType = "SHOW_MASTER"
	StatementShowOpen        StatementType = "SHOW_OPEN"
	StatementShowPlugins     StatementType = "SHOW_PLUGINS"
	StatementShowPrivileges  StatementType = "SHOW_PRIVILEGES"
	StatementShowProcedure   StatementType = "SHOW_PROCEDURE"
	StatementShowProcesslist StatementType = "SHOW_PROCESSLIST"
	StatementShowProfile     StatementType = "SHOW_PROFILE"
	StatementShowProfiles    StatementType = "SHOW_PROFILES"
	StatementShowRelaylog    StatementType = "SHOW_RELAYLOG"
	StatementShowReplicas    StatementType = "SHOW_REPLICAS"
	StatementShowSlave       StatementType = "SHOW_SLAVE"
	StatementShowReplica     StatementType = "SHOW_REPLICA"
	StatementShowStatus      StatementType = "SHOW_STATUS"
	StatementShowTriggers    StatementType = "SHOW_TRIGGERS"
	StatementShowVariables   StatementType = "SHOW_VARIABLES"
	StatementShowWarnings    StatementType = "SHOW_WARNINGS"
	StatementShowDatabases   StatementType = "SHOW_DATABASES"
	StatementShowKeys        StatementType = "SHOW_KEYS"
	StatementShowIndex       StatementType = "SHOW_INDEX"
	StatementShowTable       StatementType = "SHOW_TABLE"
	StatementShowTables      StatementType = "SHOW_TABLES"
	StatementShowColumns     StatementType = "SHOW_COLUMNS"
	StatementDropDatabase    StatementType = "DROP_DATABASE"
	StatementDropSchema      StatementType = "DROP_SCHEMA"
	StatementDropTable       StatementType = "DROP_TABLE"
	StatementDropView        StatementType = "DROP_VIEW"
	StatementDropTrigger     StatementType = "DROP_TRIGGER"
	StatementDropFunction    StatementType = "DROP_FUNCTION"
	StatementDropIndex       StatementType = "DROP_INDEX"
	StatementDropProcedure   StatementType = "DROP_PROCEDURE"
	StatementAlterDatabase   StatementType = "ALTER_DATABASE"
	StatementAlterSchema     StatementType = "ALTER_SCHEMA"
	StatementAlterTable      StatementType = "ALTER_TABLE"
	StatementAlterView       StatementType = "ALTER_VIEW"
	StatementAlterTrigger    StatementType = "ALTER_TRIGGER"
	StatementAlterFunction   StatementType = "ALTER_FUNCTION"
	StatementAlterIndex      StatementType = "ALTER_INDEX"
	StatementAlterProcedure  StatementType = "ALTER_PROCEDURE"
	StatementAnonBlock       StatementType = "ANON_BLOCK"
	StatementUnknown         StatementType = "UNKNOWN"
)

// represents the behavior of a statement (e.g., LISTING, MODIFICATION)
type ExecutionType string

const (
	ExecutionListing      ExecutionType = "LISTING"
	ExecutionModification ExecutionType = "MODIFICATION"
	ExecutionInformation  ExecutionType = "INFORMATION"
	ExecutionAnonBlock    ExecutionType = "ANON_BLOCK"
	ExecutionUnknown      ExecutionType = "UNKNOWN"
)

type ParamTypes struct {
	Positional *bool
	Numbered   []rune // '?' | ':' | '$'
	Named      []rune // ':' | '@' | '$'
	Quoted     []rune // ':' | '@' | '$'
	Custom     []string
}

// provides configuration for the Identify function
type IdentifyOptions struct {
	Strict         *bool
	Dialect        *Dialect
	IdentifyTables *bool
	ParamTypes     *ParamTypes
}

// represents a single parsed SQL statement
type IdentifyResult struct {
	Start         int           `json:"start"`
	End           int           `json:"end"`
	Text          string        `json:"text"`
	Type          StatementType `json:"type"`
	ExecutionType ExecutionType `json:"executionType"`
	Parameters    []string      `json:"parameters"`
	Tables        []string      `json:"tables"`
}

type Statement struct {
	Start         int
	End           int
	Type          *StatementType
	ExecutionType *ExecutionType
	EndStatement  *string
	CanEnd        *bool
	Definer       *int
	Algorithm     *int
	SQLSecurity   *int
	Parameters    []string
	Tables        []string
	IsCte         *bool
}

func (s *Statement) ToConcrete() ConcreteStatement {
	cs := ConcreteStatement{
		Start:        s.Start,
		End:          s.End,
		EndStatement: s.EndStatement,
		CanEnd:       s.CanEnd,
		Definer:      s.Definer,
		Algorithm:    s.Algorithm,
		SQLSecurity:  s.SQLSecurity,
		Parameters:   s.Parameters,
		Tables:       s.Tables,
		IsCte:        s.IsCte,
	}
	if s.Type != nil {
		cs.Type = *s.Type
	} else {
		cs.Type = StatementUnknown
	}
	if s.ExecutionType != nil {
		cs.ExecutionType = *s.ExecutionType
	} else {
		cs.ExecutionType = ExecutionUnknown
	}
	return cs
}

type ConcreteStatement struct {
	Start         int
	End           int
	Type          StatementType
	ExecutionType ExecutionType
	EndStatement  *string
	CanEnd        *bool
	Definer       *int
	Algorithm     *int
	SQLSecurity   *int
	Parameters    []string
	Tables        []string
	IsCte         *bool
}

type State struct {
	Start    int
	End      int
	Position int
	Input    []rune
}

type TokenType string

const (
	TokenWhitespace    TokenType = "whitespace"
	TokenCommentInline TokenType = "comment-inline"
	TokenCommentBlock  TokenType = "comment-block"
	TokenString        TokenType = "string"
	TokenSemicolon     TokenType = "semicolon"
	TokenKeyword       TokenType = "keyword"
	TokenParameter     TokenType = "parameter"
	TokenTable         TokenType = "table"
	TokenUnknown       TokenType = "unknown"
)

type Token struct {
	Type  TokenType `json:"type"`
	Value string    `json:"value"`
	Start int       `json:"start"`
	End   int       `json:"end"`
}

type ParseResult struct {
	Type   string              `json:"type"`
	Start  int                 `json:"start"`
	End    int                 `json:"end"`
	Body   []ConcreteStatement `json:"body"`
	Tokens []Token             `json:"tokens"`
}

type StepValidation struct {
	RequireBefore []string
	AcceptTokens  []AcceptToken
}

type AcceptToken struct {
	Type  string
	Value string
}

type Step struct {
	PreCanGoToNext  func(token *Token) bool
	Validation      *StepValidation
	Add             func(token Token)
	PostCanGoToNext func(token *Token) bool
}
