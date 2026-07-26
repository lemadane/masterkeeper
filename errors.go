package masterkeeper

import "errors"

var (
	ErrTransactionNotActive          = errors.New("transaction is not active")
	ErrNestedTransactionNotSupported = errors.New("nested write transactions are not supported")
	ErrClosed                        = errors.New("database is closed")
	ErrRollbackOnly                  = errors.New("transaction is marked as rollback-only")
)

type ErrDuplicateIndex struct {
	TableName string
	IndexName string
	Value     any
	Message   string
}

func (e *ErrDuplicateIndex) Error() string {
	return e.Message
}
