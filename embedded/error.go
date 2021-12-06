package embedded

import "errors"

var ErrQueriedNonSelectStatement = errors.New("tried to query a statement which is not a SELECT")
var ErrQueriedMultipleStatements = errors.New("only a single statement may be present in a query")
