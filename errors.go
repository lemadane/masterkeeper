package masterkeeper

import (
	"context"
	"errors"
)

type transactionWaitTimeoutError struct {
	wrappedError error
}

func (timeoutError *transactionWaitTimeoutError) Error() string {
	return "timed out waiting for transaction writer lock"
}

func (timeoutError *transactionWaitTimeoutError) Unwrap() error {
	return timeoutError.wrappedError
}

func (timeoutError *transactionWaitTimeoutError) Is(target error) bool {
	return target == TransactionWaitTimeoutError || target == context.DeadlineExceeded
}

var TransactionWaitTimeoutError error = &transactionWaitTimeoutError{wrappedError: context.DeadlineExceeded}

var (
	NotActiveTransactionError          = errors.New("transaction is not active")
	NestedTransactionNotSupportedError = errors.New("nested write transactions are not supported")
	DatabaseClosedError                = errors.New("database is closed")
	RollbackOnlyTransactionError       = errors.New("transaction is marked as rollback-only")
	InvalidTransactionWaitTimeoutError = errors.New("invalid transaction wait timeout")
	InvalidTransactionContextError      = errors.New("invalid transaction context")
)

type DuplicateIndexError struct {
	TableName string
	IndexName string
	Value     any
	Message   string
}

func (duplicateIndexError *DuplicateIndexError) Error() string {
	return duplicateIndexError.Message
}

var InvalidTableNameError = errors.New("invalid table name")
var IncompatibleTypesError = errors.New("incompatible table schema types")

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

