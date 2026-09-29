// Package sqlsplit splits SQL text into individual statements.
//
// It handles PostgreSQL syntax including dollar-quoted strings, nested block
// comments, and quoted identifiers. Top-level comments are emitted as
// separate [Stmt] values so they can carry directives.
package sqlsplit

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// StmtType represents the type of a statement.
type StmtType string

const (
	StmtTypeQuery   StmtType = "query"
	StmtTypeComment StmtType = "comment"
)

type state int

const (
	stateStmt state = iota
	stateLineComment
	stateBlockComment
	stateString
	stateDollarString
	stateIdent
)

const maxHighlightLines = 5

// Split splits a SQL file into individual statements.
func Split(sql []byte) ([]Stmt, error) {
	s := newScanner(sql)
	if err := s.scan(); err != nil {
		return nil, err
	}

	return s.stmts, nil
}

// Stmt represents a single SQL statement with its position in the original file.
type Stmt struct {
	Content string
	Type    StmtType
	Start   Location
	End     Location
}

// Location tracks position, line number, and column in the input.
type Location struct {
	Pos  int
	Line int
	Col  int
}

func (l Location) String() string {
	return fmt.Sprintf("%d:%d", l.Line, l.Col)
}

// String formats the statement with line numbers for display.
//
// If the statement spans more than 5 lines, the middle is collapsed.
func (s Stmt) String() string {
	lines := strings.Split(s.Content, "\n")
	gutter := len(strconv.Itoa(s.End.Line))

	var b strings.Builder

	if len(lines) <= maxHighlightLines {
		for i, line := range lines {
			fmt.Fprintf(&b, "%*d | %s\n", gutter, s.Start.Line+i, line)
		}
	} else {
		headOffset := 2
		tailOffset := 2

		for i := range headOffset {
			fmt.Fprintf(&b, "%*d | %s\n", gutter, s.Start.Line+i, lines[i])
		}

		fmt.Fprintf(&b, "%*s | ...\n", gutter, "")

		tailStart := len(lines) - tailOffset
		for i := range tailOffset {
			fmt.Fprintf(&b, "%*d | %s\n", gutter, s.End.Line-tailOffset+1+i, lines[tailStart+i])
		}
	}

	return b.String()
}

type scanner struct {
	dollarTag string
	buf       strings.Builder
	data      []byte
	stmts     []Stmt

	// pos is the current byte offset into data.
	pos int

	currentLoc Location

	// startLoc is current statement start location
	startLoc Location

	// stateLoc is current state start location (e.g., can be a string within statement).
	stateLoc        Location
	state           state
	commentDepth    int
	topLevelComment bool
}

func newScanner(sql []byte) *scanner {
	//nolint:exhaustruct_v5
	return &scanner{
		data:       sql,
		currentLoc: Location{Pos: 0, Line: 1, Col: 1},
	}
}

// peek0 decodes the current rune and its byte size.
func (s *scanner) peek0() (rune, int) {
	if s.pos >= len(s.data) {
		return 0, 0
	}

	return utf8.DecodeRune(s.data[s.pos:])
}

// peek1 decodes the next rune after the current one, given the current rune's byte size.
func (s *scanner) peek1(size0 int) rune {
	next := s.pos + size0
	if next >= len(s.data) {
		return 0
	}

	r, _ := utf8.DecodeRune(s.data[next:])

	return r
}

func (s *scanner) advance(r rune, size int) {
	if r == '\n' {
		s.currentLoc.Line++
		s.currentLoc.Col = 1
	} else {
		s.currentLoc.Col++
	}

	s.currentLoc.Pos++
	s.pos += size
}

func (s *scanner) consume1() {
	r, size := s.peek0()
	s.buf.Write(s.data[s.pos : s.pos+size])
	s.advance(r, size)
}

func (s *scanner) consume2() {
	s.consume1()
	s.consume1()
}

func (s *scanner) emitStmt() {
	content := s.buf.String()

	typ := StmtTypeQuery
	if s.topLevelComment {
		typ = StmtTypeComment
		s.topLevelComment = false
	}

	if stmt := newStmt(content, s.startLoc, s.currentLoc, typ); stmt != nil {
		s.stmts = append(s.stmts, *stmt)
	}

	s.buf.Reset()
}

func (s *scanner) scan() error {
	for s.pos < len(s.data) {
		switch s.state {
		case stateStmt:
			s.scanStmt()
		case stateLineComment:
			s.scanLineComment()
		case stateBlockComment:
			s.scanBlockComment()
		case stateString:
			s.scanString()
		case stateDollarString:
			s.scanDollarString()
		case stateIdent:
			s.scanIdent()
		}
	}

	if err := s.reportUnclosed(); err != nil {
		return err
	}

	s.emitStmt()

	return nil
}

func (s *scanner) reportUnclosed() error {
	switch s.state {
	case stateBlockComment:
		return fmt.Errorf("unclosed block comment starting at %s", s.stateLoc.String())
	case stateString:
		return fmt.Errorf("unclosed string starting at %s", s.stateLoc.String())
	case stateDollarString:
		if s.dollarTag == "" {
			return fmt.Errorf("unclosed dollar-quoted string starting at %s", s.stateLoc.String())
		}

		return fmt.Errorf("unclosed dollar-quoted string $%s$ starting at %s",
			s.dollarTag, s.stateLoc.String())
	case stateIdent:
		return fmt.Errorf("unclosed quoted identifier starting at %s", s.stateLoc.String())

	case stateStmt:
	case stateLineComment:
		// noop, all good
	}

	return nil
}

func (s *scanner) scanStmt() {
	r, size := s.peek0()

	// Skip leading whitespace between statements
	if s.buf.Len() == 0 && unicode.IsSpace(r) {
		s.advance(r, size)
		return
	}

	// Track where actual statement content begins
	if s.buf.Len() == 0 {
		s.startLoc = s.currentLoc
	}

	switch {
	case r == '-' && s.peek1(size) == '-':
		s.topLevelComment = s.buf.Len() == 0
		s.state = stateLineComment
		s.consume2() // --

	case r == '/' && s.peek1(size) == '*':
		s.topLevelComment = s.buf.Len() == 0
		s.stateLoc = s.currentLoc
		s.state = stateBlockComment
		s.commentDepth = 1
		s.consume2() // /*

	case r == '$':
		if tag, endBytePos, ok := parseDollarQuoteTag(s.data, s.pos); ok {
			s.stateLoc = s.currentLoc
			s.state = stateDollarString
			s.dollarTag = tag
			s.consumeBytes(endBytePos - s.pos)
		} else {
			s.consume1()
		}

	case r == '\'':
		s.stateLoc = s.currentLoc
		s.state = stateString
		s.consume1()

	case r == '"':
		s.stateLoc = s.currentLoc
		s.state = stateIdent
		s.consume1()

	case r == ';':
		s.consume1()
		s.emitStmt()

	default:
		s.consume1()
	}
}

func (s *scanner) scanLineComment() {
	r, size := s.peek0()

	if r == '\n' {
		if s.topLevelComment {
			s.emitStmt()
			s.advance(r, size) // consume the newline
			s.state = stateStmt

			return
		}

		s.state = stateStmt
	}

	s.consume1()
}

func (s *scanner) scanBlockComment() {
	r, size := s.peek0()

	switch {
	case r == '/' && s.peek1(size) == '*':
		s.commentDepth++
		s.consume2() // /*

	case r == '*' && s.peek1(size) == '/':
		s.commentDepth--
		s.consume2() // */

		if s.commentDepth == 0 {
			if s.topLevelComment {
				s.emitStmt()
			}

			s.state = stateStmt
		}

	default:
		s.consume1()
	}
}

func (s *scanner) scanString() {
	r, size := s.peek0()

	switch {
	case r == '\\' && s.pos+size < len(s.data):
		s.consume2() // escape sequence

	case r == '\'' && s.peek1(size) == '\'':
		s.consume2() // ''

	case r == '\'':
		s.state = stateStmt
		s.consume1()

	default:
		s.consume1()
	}
}

func (s *scanner) scanDollarString() {
	r, _ := s.peek0()

	if r == '$' {
		if closeTag, closeEnd, ok := parseDollarQuoteTag(s.data, s.pos); ok &&
			closeTag == s.dollarTag {
			s.state = stateStmt
			s.consumeBytes(closeEnd - s.pos)

			return
		}
	}

	s.consume1()
}

func (s *scanner) scanIdent() {
	r, size := s.peek0()

	switch {
	case r == '"' && s.peek1(size) == '"':
		s.consume2() // ""

	case r == '"':
		s.state = stateStmt
		s.consume1()

	default:
		s.consume1()
	}
}

// consumeBytes consumes exactly n bytes worth of runes from the input.
func (s *scanner) consumeBytes(n int) {
	end := s.pos + n
	for s.pos < end {
		s.consume1()
	}
}

func newStmt(content string, start, end Location, typ StmtType) *Stmt {
	content = strings.TrimSpace(content)
	if content == "" || content == ";" {
		return nil
	}

	return &Stmt{
		Content: content,
		Start:   start,
		End:     end,
		Type:    typ,
	}
}

// parseDollarQuoteTag attempts to parse a dollar-quote tag at the given byte position.
// Returns (tag, endBytePos, ok) where tag is the identifier between $...$,
// endBytePos is the byte position after the closing $, and ok indicates if parsing succeeded.
// For $$, returns ("", pos+2, true). For $tag$, returns ("tag", pos+5, true).
func parseDollarQuoteTag(data []byte, pos int) (string, int, bool) {
	if pos >= len(data) || data[pos] != '$' {
		return "", pos, false
	}

	start := pos
	pos++

	// Empty tag: $$
	if pos < len(data) && data[pos] == '$' {
		return "", pos + 1, true
	}

	if pos >= len(data) {
		return "", start, false
	}

	tagStart := pos

	r, size := utf8.DecodeRune(data[pos:])
	if !unicode.IsLetter(r) && r != '_' {
		return "", start, false
	}

	pos += size

	for pos < len(data) {
		r, size = utf8.DecodeRune(data[pos:])
		if r == '$' {
			return string(data[tagStart:pos]), pos + 1, true
		}

		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", start, false
		}

		pos += size
	}

	return "", start, false
}
