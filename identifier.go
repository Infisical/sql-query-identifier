package sqlqueryidentifier

import (
	"fmt"
	"slices"
	"sort"
)

func Identify(query string, options IdentifyOptions) (results []IdentifyResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	isStrict := true
	if options.Strict != nil {
		isStrict = *options.Strict
	}

	dialect := DialectGeneric
	if options.Dialect != nil {
		dialect = *options.Dialect
	}

	isValidDialect := slices.Contains(DIALECTS, dialect)

	if !isValidDialect {
		return nil, fmt.Errorf("Unknown dialect. Allowed values: %v", DIALECTS)
	}

	paramTypes := options.ParamTypes
	if paramTypes == nil {
		paramTypes = DefaultParamTypesFor(dialect)
	}

	identifyTables := false
	if options.IdentifyTables != nil {
		identifyTables = *options.IdentifyTables
	}

	result := Parse(query, isStrict, dialect, identifyTables, paramTypes)
	sortParams := dialect == DialectPSQL && options.ParamTypes == nil

	identifyResults := make([]IdentifyResult, len(result.Body))
	for i, statement := range result.Body {
		// sorting the postgres params: $1 $2 $3, regardless of the order they appear
		parameters := statement.Parameters
		if sortParams {
			sort.Strings(parameters)
		}

		identifyResults[i] = IdentifyResult{
			Start:         statement.Start,
			End:           statement.End,
			Text:          query[statement.Start:min(statement.End+1, len(query))],
			Type:          statement.Type,
			ExecutionType: statement.ExecutionType,
			Parameters:    parameters,
			Tables:        statement.Tables,
		}
	}

	return identifyResults, nil
}

func GetExecutionType(command StatementType) ExecutionType {
	executionType, ok := ExecutionTypes[command]
	if !ok {
		return ExecutionUnknown
	}
	return executionType
}
