package opengauss

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// VectorSearchResult is the outcome of one query in a multi-vector search.
// Results are returned in the order of the argument sets.
type VectorSearchResult struct {
	// Index is the position of the argument set that produced this result.
	Index int
	// Rows holds one map per returned row, keyed by column name. A duplicate
	// column name keeps the last value, and a value is the driver's decoded
	// value: text columns are strings, bytea columns are []byte and NULL is nil.
	Rows []map[string]any
	// Err reports why this query failed. The other results are unaffected.
	Err error
}

// vectorOperators are the operators a vector query uses for distance and
// scoring on openGauss vector columns.
var vectorOperators = []string{"<->", "<=>", "<#>", "<+>", "<~>", "<%>"}

// restoreTimeout bounds restoring session parameters on a connection whose work
// is finished, so a slow server cannot delay the caller indefinitely.
const restoreTimeout = 5 * time.Second

const (
	readSettingSQL  = "SELECT pg_catalog.current_setting($1)"
	writeSettingSQL = "SELECT pg_catalog.set_config($1, $2, false)"
)

// ExecuteMultiSearch runs query once for every argument set in args on at most
// threadCount sessions taken from db, with the scan parameters in scanParams
// applied to each of those sessions. It is the concurrent multi-vector search
// helper: a server that supports openGauss multi-vector retrieval evaluates the
// searches in parallel, and this function also pipelines the separate requests.
//
// Unlike the reference helper, which opens its own pools from a connection
// string, this one reuses the caller's pool through db.Conn, so pool size,
// TLS policy and authentication configuration stay in one place. Each worker
// holds one session for all of its queries, because scan parameters such as
// hnsw_ef_search or nprobes belong to a session; they are restored to their
// previous values before the session returns to the pool. A failure is
// reported per argument set in its VectorSearchResult, never as a log.Fatal,
// and results keep the input order. The returned error is reserved for invalid
// arguments, including a query that is not a single SELECT using a vector
// operator and an empty argument set: every set is sent through the extended
// protocol, so the server rejects any statement beyond the first. Canceling ctx
// stops the outstanding work, marks the affected results with the context error
// and releases the pinned sessions.
//
// Every worker takes one session from db for its whole chunk, so a thread count
// above the pool's limit, or above the connections other callers hold, waits
// for a session to become free. Pass a context with a deadline when the caller
// itself can hold every connection.
func ExecuteMultiSearch(ctx context.Context, db *sql.DB, query string, args [][]any, scanParams map[string]any, threadCount int) ([]VectorSearchResult, error) {
	if db == nil {
		return nil, errors.New("opengauss: ExecuteMultiSearch needs a *sql.DB")
	}
	if ctx == nil {
		return nil, errors.New("opengauss: ExecuteMultiSearch needs a non-nil context")
	}
	if err := validateVectorQuery(query); err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, errors.New("opengauss: ExecuteMultiSearch needs at least one argument set")
	}
	for i, set := range args {
		if len(set) == 0 {
			return nil, fmt.Errorf("opengauss: argument set %d is empty: every query is sent with parameters so the server sees one statement", i)
		}
	}
	if threadCount <= 0 {
		return nil, errors.New("opengauss: ExecuteMultiSearch needs a positive thread count")
	}
	settings, err := prepareScanSettings(scanParams)
	if err != nil {
		return nil, err
	}

	results := make([]VectorSearchResult, len(args))
	for i := range results {
		results[i].Index = i
	}
	workers := threadCount
	if workers > len(args) {
		workers = len(args)
	}
	if poolLimit := db.Stats().MaxOpenConnections; poolLimit > 0 && workers > poolLimit {
		workers = poolLimit
	}
	chunk := (len(args) + workers - 1) / workers
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		start := worker * chunk
		end := start + chunk
		if end > len(args) {
			end = len(args)
		}
		if start >= end {
			break
		}
		wait.Add(1)
		go func(start, end int) {
			defer wait.Done()
			searchVectorChunk(ctx, db, query, args, settings, start, end, results)
		}(start, end)
	}
	wait.Wait()
	return results, nil
}

// searchVectorChunk runs the assigned argument sets on one pinned session.
func searchVectorChunk(ctx context.Context, db *sql.DB, query string, args [][]any, settings []scanSetting, start, end int, results []VectorSearchResult) {
	failChunk := func(err error) {
		for i := start; i < end; i++ {
			results[i].Err = err
		}
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		failChunk(err)
		return
	}
	defer func() { _ = conn.Close() }()

	applied, err := applyScanSettings(ctx, conn, settings)
	if err != nil {
		failChunk(err)
		return
	}
	if len(applied) > 0 {
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), restoreTimeout)
		defer cancel()
		// Restore before the deferred Close returns the session to the pool. A
		// session whose parameters cannot be restored is discarded instead of
		// being pooled with changed settings.
		defer func() {
			if !restoreScanSettings(restoreCtx, conn, applied) {
				discardPooledSession(conn)
			}
		}()
	}

	for i := start; i < end; i++ {
		if err := ctx.Err(); err != nil {
			results[i].Err = err
			continue
		}
		rows, err := searchVectorRows(ctx, conn, query, args[i])
		if err != nil {
			results[i].Err = err
			continue
		}
		results[i].Rows = rows
	}
}

// searchVectorRows runs one search and collects its rows as column-name maps.
func searchVectorRows(ctx context.Context, conn *sql.Conn, query string, args []any) ([]map[string]any, error) {
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	collected := make([]map[string]any, 0, 8)
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = values[i]
		}
		collected = append(collected, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return collected, nil
}

// scanSetting is one session parameter with the value to apply.
type scanSetting struct {
	name  string
	value string
}

// prepareScanSettings validates the requested parameters and converts their
// values to text. The order is deterministic so tests and server logs are.
func prepareScanSettings(params map[string]any) ([]scanSetting, error) {
	if len(params) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	settings := make([]scanSetting, 0, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return nil, errors.New("opengauss: a scan parameter name is empty")
		}
		value, err := scanSettingValue(name, params[name])
		if err != nil {
			return nil, err
		}
		settings = append(settings, scanSetting{name: name, value: value})
	}
	return settings, nil
}

func scanSettingValue(name string, value any) (string, error) {
	if value == nil {
		return "", fmt.Errorf("opengauss: scan parameter %q has no value", name)
	}
	converted, err := driver.DefaultParameterConverter.ConvertValue(value)
	if err != nil {
		return "", fmt.Errorf("opengauss: scan parameter %q: %w", name, err)
	}
	text, err := encodeTextValue(converted)
	if err != nil {
		return "", fmt.Errorf("opengauss: scan parameter %q: %w", name, err)
	}
	return string(text), nil
}

// applyScanSettings reads every parameter's previous value and applies the
// requested one, so a failed application can restore what was already changed.
func applyScanSettings(ctx context.Context, conn *sql.Conn, settings []scanSetting) ([]scanSetting, error) {
	applied := make([]scanSetting, 0, len(settings))
	for _, setting := range settings {
		var previous string
		if err := conn.QueryRowContext(ctx, readSettingSQL, setting.name).Scan(&previous); err != nil {
			restoreAfterFailure(ctx, conn, applied)
			return nil, fmt.Errorf("opengauss: read scan parameter %q: %w", setting.name, err)
		}
		if _, err := conn.ExecContext(ctx, writeSettingSQL, setting.name, setting.value); err != nil {
			restoreAfterFailure(ctx, conn, applied)
			return nil, fmt.Errorf("opengauss: set scan parameter %q: %w", setting.name, err)
		}
		applied = append(applied, scanSetting{name: setting.name, value: previous})
	}
	return applied, nil
}

// restoreAfterFailure restores what was already applied when the next parameter
// cannot be read or set.
func restoreAfterFailure(ctx context.Context, conn *sql.Conn, applied []scanSetting) {
	if len(applied) == 0 {
		return
	}
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), restoreTimeout)
	defer cancel()
	if !restoreScanSettings(restoreCtx, conn, applied) {
		discardPooledSession(conn)
	}
}

// restoreScanSettings puts the previous values back in reverse order and
// reports whether every value was restored.
func restoreScanSettings(ctx context.Context, conn *sql.Conn, applied []scanSetting) bool {
	restored := true
	for i := len(applied) - 1; i >= 0; i-- {
		if _, err := conn.ExecContext(ctx, writeSettingSQL, applied[i].name, applied[i].value); err != nil {
			restored = false
		}
	}
	return restored
}

// discardPooledSession drops the physical connection, so closing the pinned
// session cannot return changed session parameters to the pool. Returning
// driver.ErrBadConn makes database/sql close it during the release instead of
// deferring the drop to the next borrow.
func discardPooledSession(pinned *sql.Conn) {
	_ = pinned.Raw(func(driverConn any) error {
		if physical, ok := driverConn.(*conn); ok {
			physical.bad = true
		}
		return driver.ErrBadConn
	})
}

// validateVectorQuery checks that query is one SELECT statement using a vector
// operator. The inspection understands SQL quoting, line comments and block
// comments; dollar-quoted bodies are not special-cased, which is a documented
// limit of this helper.
func validateVectorQuery(query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return errors.New("opengauss: the multi-vector query is empty")
	}
	statements, hasOperator := inspectVectorQuery(trimmed)
	if statements != 1 {
		return fmt.Errorf("opengauss: the multi-vector query must contain exactly one statement, found %d", statements)
	}
	if !hasLeadingSelect(trimmed) {
		return errors.New("opengauss: the multi-vector query must start with SELECT")
	}
	if !hasOperator {
		return errors.New("opengauss: the multi-vector query must use a vector operator: <->, <=>, <#>, <+>, <~> or <%>")
	}
	return nil
}

// hasLeadingSelect accepts leading whitespace and comments before the keyword
// and rejects a keyword that runs into an identifier byte.
func hasLeadingSelect(query string) bool {
	rest := query[skipLeadingComments(query):]
	const keyword = "SELECT"
	if len(rest) < len(keyword) || !strings.EqualFold(rest[:len(keyword)], keyword) {
		return false
	}
	if len(rest) == len(keyword) {
		return true
	}
	switch next := rest[len(keyword)]; {
	case next >= 'a' && next <= 'z', next >= 'A' && next <= 'Z',
		next >= '0' && next <= '9', next == '_', next >= 0x80:
		return false
	}
	return true
}

// skipLeadingComments returns the offset of the first token of the statement.
func skipLeadingComments(query string) int {
	i := 0
	for i < len(query) {
		switch {
		case isSpace(query[i]):
			i++
		case strings.HasPrefix(query[i:], "--"):
			newline := strings.IndexAny(query[i:], "\n\r")
			if newline < 0 {
				return len(query)
			}
			i += newline + 1
		case strings.HasPrefix(query[i:], "/*"):
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return len(query)
			}
			i += 2 + end + 2
		default:
			return i
		}
	}
	return i
}

// inspectVectorQuery counts the top-level statements and reports whether a
// vector operator appears outside string literals and comments.
func inspectVectorQuery(query string) (statements int, hasOperator bool) {
	started := false
	i, n := 0, len(query)
	for i < n {
		switch c := query[i]; {
		case c == '\'':
			i = skipQuoted(query, i, '\'')
		case c == '"':
			i = skipQuoted(query, i, '"')
		case c == '-' && i+1 < n && query[i+1] == '-':
			for i < n && query[i] != '\n' && query[i] != '\r' {
				i++
			}
		case c == '/' && i+1 < n && query[i+1] == '*':
			i = skipBlockComment(query, i+2)
		case isSpace(c):
			i++
		case c == ';':
			started = false
			i++
		default:
			if !started {
				statements++
				started = true
			}
			if !hasOperator {
				for _, operator := range vectorOperators {
					if strings.HasPrefix(query[i:], operator) {
						hasOperator = true
						break
					}
				}
			}
			i++
		}
	}
	return statements, hasOperator
}

// skipQuoted returns the offset after a quoted run, honouring doubled quotes.
func skipQuoted(query string, i int, quote byte) int {
	i++
	for i < len(query) {
		if query[i] == quote {
			if i+1 < len(query) && query[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(query)
}

// skipBlockComment returns the offset after the closing marker.
func skipBlockComment(query string, i int) int {
	for i+1 < len(query) {
		if query[i] == '*' && query[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(query)
}
