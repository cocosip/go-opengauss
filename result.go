package opengauss

import (
	"errors"
	"strconv"
	"strings"
)

// result implements driver.Result for one executed command.
type result struct {
	tag string
}

var errLastInsertID = errors.New("opengauss: LastInsertId is not supported; use INSERT ... RETURNING")

func (r result) LastInsertId() (int64, error) { return 0, errLastInsertID }

func (r result) RowsAffected() (int64, error) { return rowsAffected(r.tag), nil }

// rowsAffected reads the count from a command tag such as "INSERT 0 3" or
// "UPDATE 2". SELECT and transaction tags report no affected rows.
func rowsAffected(tag string) int64 {
	fields := strings.Fields(tag)
	if len(fields) < 2 {
		return 0
	}
	switch fields[0] {
	case "INSERT", "UPDATE", "DELETE", "MERGE", "MOVE", "FETCH", "COPY":
		n, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}
