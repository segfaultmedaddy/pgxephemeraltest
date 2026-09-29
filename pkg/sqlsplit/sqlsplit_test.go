package sqlsplit

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/require"

	"go.segfaultmedaddy.com/pgxephemeraltest/v2/internal/sliceutil"
)

func TestSplit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "simple statements",
			input: "SELECT 1; SELECT 2;",
		},
		{
			name:  "single-line comments",
			input: "SELECT 1; -- comment\nSELECT 2;",
		},
		{
			name: "multi-line comments",
			input: `SELECT 1; /* comment
spanning lines */ SELECT 2;`,
		},
		{
			name:  "nested multi-line comments",
			input: "SELECT 1; /* outer /* inner */ still outer */ SELECT 2;",
		},
		{
			name:  "single-quoted strings",
			input: "SELECT 'test'; SELECT 'it''s';",
		},
		{
			name:  "semicolon in string",
			input: "SELECT 'test; value'; SELECT 2;",
		},
		{
			name:  "dollar-quoted strings",
			input: "SELECT $$test$$; SELECT $$it's$$;",
		},
		{
			name:  "dollar-quoted with tag",
			input: "SELECT $tag$test$tag$; SELECT $foo$it's$foo$;",
		},
		{
			name:  "dollar-quoted with semicolon",
			input: "SELECT $$test; value$$; SELECT 2;",
		},
		{
			name:  "dollar-quoted with nested dollars",
			input: "SELECT $outer$test $$ value$outer$; SELECT 2;",
		},
		{
			name:  "quoted identifiers",
			input: `SELECT "table"; SELECT "col;umn";`,
		},
		{
			name:  "escaped quote in identifier",
			input: `SELECT "test""name"; SELECT 2;`,
		},
		{
			name: "CREATE FUNCTION with dollar quoting",
			input: `CREATE FUNCTION test() RETURNS integer AS $$
BEGIN
    RETURN 1;
END;
$$ LANGUAGE plpgsql;
SELECT 2;`,
		},
		{
			name: "complex nested quoting",
			input: `CREATE FUNCTION test() RETURNS text AS $func$
BEGIN
    RETURN $inner$It's a "test"; value$inner$;
END;
$func$ LANGUAGE plpgsql;`,
		},
		{
			name:  "escape string syntax",
			input: `SELECT E'test\nvalue'; SELECT 'normal';`,
		},
		{
			name:  "empty statements",
			input: ";;; SELECT 1; ;;",
		},
		{
			name:  "trailing statement without semicolon",
			input: "SELECT 1; SELECT 2",
		},

		// Realistic PL/pgSQL functions
		{
			name: "plpgsql function with exception handling",
			input: `CREATE FUNCTION safe_divide(a int, b int) RETURNS int AS $$
BEGIN
    RETURN a / b;
EXCEPTION
    WHEN division_by_zero THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;`,
		},
		{
			name: "plpgsql function with dynamic SQL",
			input: `CREATE FUNCTION dynamic_query(tbl text) RETURNS SETOF record AS $$
BEGIN
    RETURN QUERY EXECUTE 'SELECT * FROM ' || quote_ident(tbl) || ' WHERE id > $1' USING 10;
END;
$$ LANGUAGE plpgsql;`,
		},
		{
			name: "plpgsql function with nested dollar quotes and comments",
			input: `CREATE FUNCTION complex_func() RETURNS text AS $body$
BEGIN
    -- This is a line comment inside the function
    /* This is a block comment
       spanning multiple lines */
    RETURN $inner$It's a nested "string" with; semicolons$inner$;
END;
$body$ LANGUAGE plpgsql;`,
		},

		// Complex comment scenarios
		{
			name:  "block comment containing string-like content",
			input: `SELECT /* it's got 'quotes' and "identifiers" */ 1;`,
		},
		{
			name:  "block comment containing dollar signs",
			input: `SELECT /* $$ not a string $$ */ 1; SELECT /* $tag$ also not $tag$ */ 2;`,
		},
		{
			name:  "deeply nested block comments",
			input: `SELECT /* level1 /* level2 /* level3 */ back to 2 */ back to 1 */ 1;`,
		},
		{
			name: "mixed line and block comments",
			input: `SELECT 1; -- line comment /* not a block
SELECT /* block comment -- not a line */ 2;`,
		},

		// Complex string scenarios
		{
			name:  "string containing comment-like content",
			input: `SELECT '/* not a comment */'; SELECT '-- also not a comment';`,
		},
		{
			name:  "string containing dollar signs",
			input: `SELECT 'price is $5 or $$10';`,
		},
		{
			name:  "multiple escape sequences",
			input: `SELECT E'line1\nline2\ttab\\backslash\'quote';`,
		},
		{
			name:  "adjacent strings",
			input: `SELECT 'first' 'second'; SELECT 'it''s escaped';`,
		},

		// Complex dollar quoting
		{
			name:  "dollar tag resembling keyword",
			input: `SELECT $SELECT$content with; semicolon$SELECT$;`,
		},
		{
			name:  "dollar string containing its own fake closer",
			input: `SELECT $a$text $a not closed yet$a$;`,
		},
		{
			name:  "empty dollar string",
			input: `SELECT $$$$; SELECT $tag$$tag$;`,
		},
		{
			name:  "multiple different dollar tags in sequence",
			input: `SELECT $a$one$a$, $b$two$b$, $$three$$;`,
		},

		// Complex identifier scenarios
		{
			name:  "identifier containing all special chars",
			input: `SELECT "table;name--with/*special*/chars" FROM "test";`,
		},
		{
			name:  "identifier resembling keyword",
			input: `SELECT "SELECT", "FROM", "WHERE" FROM "table";`,
		},

		// Real-world migration patterns
		{
			name: "CREATE TYPE with multiline",
			input: `CREATE TYPE mood AS ENUM (
    'sad',
    'ok',
    'happy'
);
CREATE TYPE address AS (
    street text,
    city text,
    zip text
);`,
		},
		{
			name: "CREATE TRIGGER with function reference",
			input: `CREATE FUNCTION update_timestamp() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER set_timestamp
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();`,
		},
		{
			name:  "ALTER TABLE with multiple operations",
			input: `ALTER TABLE users ADD COLUMN email text, ADD COLUMN created_at timestamp DEFAULT NOW(), DROP COLUMN old_field;`,
		},
		{
			name: "DO block with anonymous function",
			input: `DO $$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT * FROM users WHERE active = false
    LOOP
        RAISE NOTICE 'Inactive user: %', r.name;
    END LOOP;
END;
$$;`,
		},

		{
			name: "CREATE TABLE with comments and quoted identifier",
			input: `-- Create a table for user data
/* This table contains all user-related data
   for the application */
CREATE TABLE "user-data" (
    id serial PRIMARY KEY,
    "full-name" text NOT NULL
);`,
		},

		// Edge cases
		{
			name:  "unicode in strings and identifiers",
			input: `SELECT 'émoji 🎉 café'; SELECT "tëst_tàble" FROM "schëma"."tãble";`,
		},

		// Top-level comments
		{
			name:  "top-level line comment standalone",
			input: "-- this is a top-level comment",
		},
		{
			name: "top-level line comment before statement",
			input: `-- top-level comment
SELECT 1;`,
		},
		{
			name: "top-level block comment before statement",
			input: `/* top-level block comment */
SELECT 1;`,
		},
		{
			name:  "top-level block comment standalone",
			input: `/* top-level block comment */`,
		},
		{
			name: "multiple top-level comments",
			input: `-- first comment
-- second comment
SELECT 1;`,
		},
		{
			name: "top-level comment between statements",
			input: `SELECT 1;
-- middle comment
SELECT 2;`,
		},
		{
			name:  "mid-statement comment stays in query",
			input: `SELECT /* inline */ 1;`,
		},
		{
			name:  "mid-statement line comment stays in query",
			input: "SELECT 1 -- inline\n, 2;",
		},
		{
			name: "top-level directive-style comments",
			input: `---- enable-tx ----
CREATE TABLE users (id int);
---- create above / drop below ----
DROP TABLE users;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := Split([]byte(tt.input))
			require.NoError(t, err)
			snaps.MatchSnapshot(t, stmts)
		})
	}
}

func TestLocationTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected []struct{ start, end Location }
	}{
		{
			name:  "single line multiple statements",
			input: "SELECT 1; SELECT 2;",
			expected: []struct{ start, end Location }{
				{Location{Pos: 0, Line: 1, Col: 1}, Location{Pos: 9, Line: 1, Col: 10}},
				{Location{Pos: 10, Line: 1, Col: 11}, Location{Pos: 19, Line: 1, Col: 20}},
			},
		},
		{
			name:  "statements on separate lines",
			input: "SELECT 1;\nSELECT 2;",
			expected: []struct{ start, end Location }{
				{Location{Pos: 0, Line: 1, Col: 1}, Location{Pos: 9, Line: 1, Col: 10}},
				{Location{Pos: 10, Line: 2, Col: 1}, Location{Pos: 19, Line: 2, Col: 10}},
			},
		},
		{
			name:  "indented statements",
			input: "  SELECT 1;\n    SELECT 2;",
			expected: []struct{ start, end Location }{
				{Location{Pos: 2, Line: 1, Col: 3}, Location{Pos: 11, Line: 1, Col: 12}},
				{Location{Pos: 16, Line: 2, Col: 5}, Location{Pos: 25, Line: 2, Col: 14}},
			},
		},
		{
			name:  "multiline statement",
			input: "SELECT\n  1;",
			expected: []struct{ start, end Location }{
				{Location{Pos: 0, Line: 1, Col: 1}, Location{Pos: 11, Line: 2, Col: 5}},
			},
		},
		{
			name:  "statement after multiline comment",
			input: "/* comment\n   */ SELECT 1;",
			expected: []struct{ start, end Location }{
				{Location{Pos: 0, Line: 1, Col: 1}, Location{Pos: 16, Line: 2, Col: 6}},
				{Location{Pos: 17, Line: 2, Col: 7}, Location{Pos: 26, Line: 2, Col: 16}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := Split([]byte(tt.input))
			require.NoError(t, err)
			require.Len(t, stmts, len(tt.expected))

			for i, stmt := range stmts {
				exp := tt.expected[i]
				require.Equal(t, exp.start, stmt.Start, "stmt %d: start", i)
				require.Equal(t, exp.end, stmt.End, "stmt %d: end", i)
			}
		})
	}
}

func TestUnclosedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       string
		errContains string
	}{
		{
			name:        "unclosed string",
			input:       "SELECT 'unclosed",
			errContains: "unclosed string starting at 1:8",
		},
		{
			name:        "unclosed string on line 2",
			input:       "SELECT 1;\nSELECT 'unclosed",
			errContains: "unclosed string starting at 2:8",
		},
		{
			name:        "unclosed block comment",
			input:       "SELECT /* unclosed",
			errContains: "unclosed block comment starting at 1:8",
		},
		{
			name:        "unclosed nested block comment",
			input:       "SELECT /* outer /* inner */",
			errContains: "unclosed block comment starting at 1:8",
		},
		{
			name:        "unclosed dollar string",
			input:       "SELECT $$unclosed",
			errContains: "unclosed dollar-quoted string starting at 1:8",
		},
		{
			name:        "unclosed dollar string with tag",
			input:       "SELECT $func$unclosed",
			errContains: "unclosed dollar-quoted string $func$ starting at 1:8",
		},
		{
			name:        "unclosed quoted identifier",
			input:       `SELECT "unclosed`,
			errContains: "unclosed quoted identifier starting at 1:8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Split([]byte(tt.input))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errContains,
				"expected error containing %q, got %q", tt.errContains, err.Error())
		})
	}
}

func TestStmtHighlight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "single statement single line",
			input: "SELECT 1;",
		},
		{
			name: "two single-line statements",
			input: `SELECT 1;
SELECT 2;`,
		},
		{
			name: "multiline statement followed by single line",
			input: `CREATE TABLE users (
  id int,
  name text
);
SELECT 1;`,
		},
		{
			name: "comment then multiline statement",
			input: `---- enable-tx ----
CREATE TABLE users (
  id int,
  name text
);`,
		},
		{
			name: "long function collapses middle",
			input: `CREATE FUNCTION test() RETURNS text AS $body$
BEGIN
  x := 1;
  y := 2;
  z := 3;
  RETURN x;
END;
$body$ LANGUAGE plpgsql;
SELECT 1;`,
		},
		{
			name: "multiple statements with blank lines",
			input: `SELECT 1;

SELECT 2;

CREATE TABLE t (
  id int
);`,
		},
		{
			name: "many short statements",
			input: `SELECT 1;
SELECT 2;
SELECT 3;
SELECT 4;
SELECT 5;
SELECT 6;
SELECT 7;
SELECT 8;
SELECT 9;
SELECT 10;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stmts, err := Split([]byte(tt.input))
			require.NoError(t, err)

			highlights := make([]string, len(stmts))
			for i, stmt := range stmts {
				highlights[i] = stmt.String()
			}

			snaps.MatchSnapshot(
				t,
				sliceutil.Map(highlights, func(h string) any { return any(h) })...,
			)
		})
	}
}
