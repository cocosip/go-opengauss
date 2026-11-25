package opengauss

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/cocosip/go-opengauss/internal/proto"
)

// sessionState is what target_session_attrs needs to know about a server. Both
// facts come from one query, so a checked connection costs one round trip.
type sessionState struct {
	inRecovery bool
	readOnly   bool
}

func (s sessionState) String() string {
	role := "primary"
	if s.inRecovery {
		role = "standby"
	}
	access := "read-write"
	if s.readOnly {
		access = "read-only"
	}
	return role + " " + access
}

// satisfiedBy reports whether the server state matches the requested policy.
// prefer-standby is handled by the caller, which keeps a fallback connection.
func (a TargetSessionAttrs) satisfiedBy(state sessionState) bool {
	switch a {
	case TargetSessionAttrsReadWrite:
		return !state.readOnly
	case TargetSessionAttrsReadOnly:
		return state.readOnly
	case TargetSessionAttrsPrimary:
		return !state.inRecovery
	case TargetSessionAttrsStandby:
		return state.inRecovery
	case TargetSessionAttrsPreferStandby:
		// Only a standby satisfies the preference; the caller keeps a
		// non-standby connection as the fallback.
		return state.inRecovery
	default:
		return true
	}
}

// sessionStateQuery reads both facts in one round trip.
const sessionStateQuery = "SELECT pg_catalog.pg_is_in_recovery(), pg_catalog.current_setting('transaction_read_only')"

// detectSessionState asks the server whether it is in recovery and read-only.
// The connection is already authenticated, so this runs as an ordinary query.
func (c *conn) detectSessionState(ctx context.Context) (sessionState, error) {
	query := proto.NewMessageWriter(proto.MsgQuery)
	query.String(sessionStateQuery)
	result, err := c.queryFramesOperation(ctx, "query", [][]byte{query.Finish()})
	if err != nil {
		return sessionState{}, err
	}
	defer func() { _ = result.Close() }()

	dest := make([]driver.Value, 2)
	if err := result.Next(dest); err != nil {
		return sessionState{}, fmt.Errorf("opengauss: read the server state: %w", err)
	}
	inRecovery, err := parseServerBool(dest[0])
	if err != nil {
		return sessionState{}, fmt.Errorf("opengauss: pg_is_in_recovery(): %w", err)
	}
	readOnly, err := parseServerBool(dest[1])
	if err != nil {
		return sessionState{}, fmt.Errorf("opengauss: transaction_read_only: %w", err)
	}
	return sessionState{inRecovery: inRecovery, readOnly: readOnly}, nil
}

// parseServerBool accepts the boolean spellings a server can produce, whether
// the value arrived as a decoded bool or as text.
func parseServerBool(value driver.Value) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		switch v {
		case "t", "true", "on", "1":
			return true, nil
		case "f", "false", "off", "0":
			return false, nil
		}
		return false, fmt.Errorf("unexpected boolean value %q", v)
	case []byte:
		return parseServerBool(string(v))
	default:
		return false, fmt.Errorf("unexpected boolean value of type %T", value)
	}
}
