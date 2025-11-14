package sqlqueryidentifier

import (
	"slices"
	"strings"
	"unicode"

	regexp "github.com/wasilibs/go-re2"
)

const eof = rune(-1)

var keywords = make(map[string]bool)

func init() {
	kwList := []string{
		"SELECT", "INSERT", "DELETE", "UPDATE", "CREATE", "DROP", "DATABASE",
		"SCHEMA", "TABLE", "VIEW", "TRIGGER", "FUNCTION", "INDEX", "ALTER",
		"TRUNCATE", "WITH", "AS", "MATERIALIZED", "BEGIN", "DECLARE", "CASE",
		"LOOP", "IF", "REPEAT", "WHILE", "FOR", "PROCEDURE", "SHOW", "DATABASES",
		"KEYS", "TABLES", "COLUMNS", "STATUS", "BINARY", "BINLOG", "CHARACTER",
		"COLLATION", "ENGINE", "ENGINES", "ERRORS", "EVENTS", "GRANTS", "MASTER",
		"OPEN", "PLUGINS", "PRIVILEGES", "PROCESSLIST", "PROFILE", "PROFILES",
		"RELAYLOG", "REPLICAS", "SLAVE", "REPLICA", "TRIGGERS", "VARIABLES", "WARNINGS",
	}
	for _, kw := range kwList {
		keywords[kw] = true
	}
}

var individuals = map[string]TokenType{
	";": TokenSemicolon,
}

var endTokens = map[rune]rune{
	'"':  '"',
	'\'': '\'',
	'`':  '`',
	'[':  ']',
}

func ScanToken(state *State, dialect Dialect, paramTypes *ParamTypes) Token {
	ch := read(state, 0)

	if isWhitespace(ch) {
		return scanWhitespace(state)
	}

	if isCommentInline(ch, state) {
		return scanCommentInline(state)
	}

	if isCommentBlock(ch, state) {
		return scanCommentBlock(state)
	}

	if isString(ch, dialect) {
		endToken, ok := endTokens[ch]
		if ok {
			return scanString(state, endToken)
		}
	}

	if isParameter(ch, state, paramTypes) {
		return scanParameter(state, dialect, paramTypes)
	}

	if isDollarQuotedString(state) {
		return scanDollarQuotedString(state)
	}

	if isQuotedIdentifier(ch, dialect) {
		endToken, ok := endTokens[ch]
		if ok {
			return scanQuotedIdentifier(state, endToken)
		}
	}

	if isLetter(ch) {
		return scanWord(state)
	}

	if individual := scanIndividualCharacter(state); individual != nil {
		return *individual
	}

	return skipChar(state)
}

func read(state *State, skip int) rune {
	if state.Position+skip >= len(state.Input)-1 {
		return eof
	}
	state.Position += 1 + skip
	return state.Input[state.Position]
}

func unread(state *State) {
	if state.Position <= state.Start {
		return
	}
	state.Position--
}

func peekBack(state *State) rune {
	if state.Position == 0 {
		return eof
	}
	return state.Input[state.Position-1]
}

func peek(state *State) rune {
	if state.Position >= len(state.Input)-1 {
		return eof
	}
	return state.Input[state.Position+1]
}

func isKeyword(word string) bool {
	_, ok := keywords[strings.ToUpper(word)]
	return ok
}

func resolveIndividualTokenType(ch string) (TokenType, bool) {
	tokenType, ok := individuals[ch]
	return tokenType, ok
}

func scanWhitespace(state *State) Token {
	var nextChar rune
	for {
		nextChar = read(state, 0)
		if !isWhitespace(nextChar) {
			break
		}
	}

	if nextChar != eof {
		unread(state)
	}

	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenWhitespace,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func scanCommentInline(state *State) Token {
	var nextChar rune
	for {
		nextChar = read(state, 0)
		if nextChar == '\n' || nextChar == eof {
			break
		}
	}

	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenCommentInline,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

var dollarQuotedStringOpenerRegex = regexp.MustCompile(`^(\$[a-zA-Z0-9_]*\$)`)

func scanDollarQuotedString(state *State) Token {
	remainingInput := string(state.Input[state.Start:])
	match := dollarQuotedStringOpenerRegex.FindStringSubmatch(remainingInput)
	if match == nil {
		panic("Could not find dollar quoted string opener")
	}
	label := match[1]
	labelRunes := []rune(label)

	for i := 0; i < len(labelRunes)-1; i++ {
		read(state, 0)
	}

	for {
		if state.Position+1+len(labelRunes) > len(state.Input) {
			for read(state, 0) != eof {
			}
			break
		}
		peekedSlice := state.Input[state.Position+1 : state.Position+1+len(labelRunes)]
		if string(peekedSlice) == label {
			for i := 0; i < len(labelRunes); i++ {
				read(state, 0)
			}
			break
		}
		if read(state, 0) == eof {
			break
		}
	}

	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenString,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func scanString(state *State, endToken rune) Token {
	var nextChar rune
	for {
		nextChar = read(state, 0)
		if nextChar == endToken {
			if peek(state) == endToken {
				read(state, 0)
			} else {
				break
			}
		}
		if nextChar == eof {
			break
		}
	}

	if nextChar == eof {
		unread(state)
	}

	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenString,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func getCustomParam(state *State, paramTypes *ParamTypes) string {
	if len(paramTypes.Custom) == 0 {
		return ""
	}
	remainingInput := string(state.Input[state.Start:])
	for _, r := range paramTypes.Custom {
		re := regexp.MustCompile("^(?:" + r + ")")
		match := re.FindString(remainingInput)
		if match != "" {
			return match
		}
	}
	return ""
}

func scanParameter(state *State, dialect Dialect, paramTypes *ParamTypes) Token {
	curCh := state.Input[state.Start]
	nextCh := peek(state)
	matched := false

	if len(paramTypes.Numbered) > 0 && slices.Contains(paramTypes.Numbered, curCh) {
		if nextCh != eof && unicode.IsDigit(nextCh) {
			var potentialNumberStr string
			if state.Start+1 < len(state.Input) {
				subSlice := state.Input[state.Start+1:]
				endIndex := slices.IndexFunc(subSlice, func(r rune) bool {
					return !(isLetter(r) || unicode.IsDigit(r))
				})
				if endIndex == -1 {
					potentialNumberStr = string(subSlice)
				} else {
					potentialNumberStr = string(subSlice[:endIndex])
				}
			}

			if len(potentialNumberStr) > 0 {
				isAllDigits := true
				for _, r := range potentialNumberStr {
					if !unicode.IsDigit(r) {
						isAllDigits = false
						break
					}
				}

				if isAllDigits {
					for i := 0; i < len([]rune(potentialNumberStr)); i++ {
						read(state, 0)
					}
					matched = true
				}
			}
		}
	}

	if !matched && len(paramTypes.Named) > 0 && slices.Contains(paramTypes.Named, curCh) {
		if !isQuotedIdentifier(nextCh, dialect) {
			for isAlphaNumeric(peek(state)) {
				read(state, 0)
			}
			matched = true
		}
	}

	if !matched && len(paramTypes.Quoted) > 0 && slices.Contains(paramTypes.Quoted, curCh) {
		if isQuotedIdentifier(nextCh, dialect) {
			quoteChar := read(state, 0)
			endQuote := endTokens[quoteChar]
			for (isAlphaNumeric(peek(state)) || peek(state) == ' ') && peek(state) != endQuote {
				read(state, 0)
			}
			read(state, 0)
			matched = true
		}
	}

	if !matched && len(paramTypes.Custom) > 0 {
		custom := getCustomParam(state, paramTypes)
		if custom != "" {
			read(state, len(custom)-2)
			matched = true
		}
	}

	value := string(state.Input[state.Start : state.Position+1])

	isPositional := paramTypes.Positional != nil && *paramTypes.Positional
	if !matched && !isPositional && curCh != '?' {
		return Token{
			Type:  TokenUnknown,
			Value: value,
			Start: state.Start,
			End:   state.Start + len(value) - 1,
		}
	}

	return Token{
		Type:  TokenParameter,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func scanCommentBlock(state *State) Token {
	var nextChar, prevChar rune
	for {
		prevChar = nextChar
		nextChar = read(state, 0)
		if (prevChar != eof && nextChar != eof && string([]rune{prevChar, nextChar}) == "*/") || nextChar == eof {
			break
		}
	}

	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenCommentBlock,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func scanQuotedIdentifier(state *State, endToken rune) Token {
	var nextChar rune
	for {
		nextChar = read(state, 0)
		if nextChar == endToken || nextChar == eof {
			break
		}
	}

	if nextChar == eof {
		unread(state)
	}

	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenKeyword,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func scanWord(state *State) Token {
	var nextChar rune
	for {
		nextChar = read(state, 0)
		if !isLetter(nextChar) {
			break
		}
	}

	if nextChar != eof {
		unread(state)
	}

	value := string(state.Input[state.Start : state.Position+1])
	if !isKeyword(value) {
		return skipWord(state, value)
	}

	return Token{
		Type:  TokenKeyword,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func scanIndividualCharacter(state *State) *Token {
	value := string(state.Input[state.Start : state.Position+1])
	tokenType, ok := resolveIndividualTokenType(value)
	if !ok {
		return nil
	}

	return &Token{
		Type:  tokenType,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func skipChar(state *State) Token {
	value := string(state.Input[state.Start : state.Position+1])
	return Token{
		Type:  TokenUnknown,
		Value: value,
		Start: state.Start,
		End:   state.Start,
	}
}

func skipWord(state *State, value string) Token {
	return Token{
		Type:  TokenUnknown,
		Value: value,
		Start: state.Start,
		End:   state.Start + len(value) - 1,
	}
}

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isAlphaNumeric(ch rune) bool {
	return ch != eof && (isLetter(ch) || (ch >= '0' && ch <= '9'))
}

func isString(ch rune, dialect Dialect) bool {
	stringStart := []rune{'\''}
	if dialect == DialectMySQL {
		stringStart = append(stringStart, '"')
	}
	return slices.Contains(stringStart, ch)
}

func isCustomParam(state *State, paramTypes *ParamTypes) bool {
	remainingInput := string(state.Input[state.Start:])
	for _, r := range paramTypes.Custom {
		re := regexp.MustCompile("^(?:" + r + ")")
		if re.MatchString(remainingInput) {
			return true
		}
	}
	return false
}

func isParameter(ch rune, state *State, paramTypes *ParamTypes) bool {
	if ch == eof {
		return false
	}
	nextChar := peek(state)
	prevChar := peekBack(state)

	if ch == ':' && (prevChar == ':' || nextChar == ':') {
		return false
	}

	if paramTypes.Positional != nil && *paramTypes.Positional && ch == '?' {
		return true
	}

	if len(paramTypes.Numbered) > 0 {
		if slices.Contains(paramTypes.Numbered, ch) {
			if nextChar != eof && unicode.IsDigit(nextChar) {
				return true
			}
		}
	}

	if len(paramTypes.Named) > 0 {
		if slices.Contains(paramTypes.Named, ch) {
			return true
		}
	}
	if len(paramTypes.Quoted) > 0 {
		if slices.Contains(paramTypes.Quoted, ch) {
			return true
		}
	}

	if len(paramTypes.Custom) > 0 && isCustomParam(state, paramTypes) {
		return true
	}

	return false
}

var isDollarQuotedStringRegex = regexp.MustCompile(`^\$\w*\$`)

func isDollarQuotedString(state *State) bool {
	remainingInput := string(state.Input[state.Start:])
	return isDollarQuotedStringRegex.MatchString(remainingInput)
}

func isQuotedIdentifier(ch rune, dialect Dialect) bool {
	startQuoteChars := []rune{'"', '`'}
	if dialect == DialectMSSQL {
		startQuoteChars = []rune{'"', '['}
	}
	return slices.Contains(startQuoteChars, ch)
}

func isCommentInline(ch rune, state *State) bool {
	if ch != '-' {
		return false
	}
	nextChar := read(state, 0)
	isComment := nextChar == '-'
	if !isComment {
		unread(state)
	}
	return isComment
}

func isCommentBlock(ch rune, state *State) bool {
	if ch != '/' {
		return false
	}
	nextChar := read(state, 0)
	isComment := nextChar == '*'
	if !isComment {
		unread(state)
	}
	return isComment
}

func isLetter(ch rune) bool {
	return ch != eof && ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_')
}
