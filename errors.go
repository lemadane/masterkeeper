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

func (duplicateIndexError *ErrDuplicateIndex) Error() string {
	return duplicateIndexError.Message
}

var ErrInvalidTableName = errors.New("invalid table name")

func isValidTableName(tableName string) bool {
	if len(tableName) == 0 {
		return false
	}
	for _, char := range tableName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

