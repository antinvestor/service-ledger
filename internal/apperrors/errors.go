package apperrors

import (
	"fmt"
	"strings"
)

// Error code constants for different categories.
const (
	// System error codes (1-10).
	ErrorCodeSystemFailure        = 1
	ErrorCodeUnspecifiedID        = 2
	ErrorCodeUnspecifiedReference = 3
	ErrorCodeBadDataSupplied      = 4

	// Ledger error codes (11-20).
	ErrorCodeLedgerNotFound = 11

	// Account error codes (21-30).
	ErrorCodeAccountNotFound            = 21
	ErrorCodeAccountsNotFound           = 22
	ErrorCodeAccountsCurrencyUnknown    = 23
	ErrorCodeAccountWithReferenceExists = 24

	// Transaction error codes (31-60).
	ErrorCodeTransactionNotFound               = 31
	ErrorCodeTransactionEntriesNotFound        = 32
	ErrorCodeTransactionEntryHasZeroAmount     = 33
	ErrorCodeTransactionAccountsDifferCurrency = 34
	ErrorCodeTransactionAlreadyExists          = 35
	ErrorCodeTransactionHasNonZeroSum          = 36
	ErrorCodeTransactionHasInvalidDrCrEntry    = 37
	ErrorCodeTransactionIsConflicting          = 38
	ErrorCodeTransactionTypeNotReversible      = 39

	// Search error codes (61-70).
	ErrorCodeSearchNamespaceUnknown       = 61
	ErrorCodeSearchQueryHasInvalidFormat  = 62
	ErrorCodeSearchQueryHasInvalidKeys    = 63
	ErrorCodeSearchQueryResultsNotCasting = 64
)

type ApplicationLedgerError interface {
	error
	ErrorCode() int32
	String() string
	Extend(message string) ApplicationLedgerError
	Override(errs ...error) ApplicationLedgerError
}

type applicationLedgerError struct {
	Code         int32
	CodeOffset   int32
	Message      string
	ExtraMessage string
}

func NewApplicationError(code int32, message string) ApplicationLedgerError {
	return &applicationLedgerError{code, 200, message, ""}
}

func (e applicationLedgerError) Error() string {
	if e.ExtraMessage != "" {
		return fmt.Sprintf("%d  : - %s  \n extra info : %s", e.ErrorCode(), e.Message, e.ExtraMessage)
	}
	return fmt.Sprintf("%d  : - %s  ", e.Code, e.Message)
}

// ErrorCode returns the unique Code of the error.
func (e applicationLedgerError) ErrorCode() int32 {
	return e.CodeOffset + e.Code
}

// String implementation supports logging.
func (e applicationLedgerError) String() string {
	return e.Error()
}

// Extend default Message.
func (e applicationLedgerError) Extend(message string) ApplicationLedgerError {
	return &applicationLedgerError{e.Code, e.CodeOffset, e.Message, message}
}

// Override default Message.
func (e applicationLedgerError) Override(errs ...error) ApplicationLedgerError {
	errorStrings := make([]string, len(errs))

	for i, err := range errs {
		errorStrings[i] = err.Error()
	}
	return &applicationLedgerError{e.Code, e.CodeOffset, e.Message, strings.Join(errorStrings, "\n")}
}

var (
	ErrSystemFailure        = NewApplicationError(ErrorCodeSystemFailure, "Internal System failure")
	ErrUnspecifiedID        = NewApplicationError(ErrorCodeUnspecifiedID, "No ID was supplied")
	ErrUnspecifiedReference = NewApplicationError(ErrorCodeUnspecifiedReference, "No reference was supplied")
	ErrBadDataSupplied      = NewApplicationError(ErrorCodeBadDataSupplied, "Invalid data format was supplied")

	ErrLedgerNotFound = NewApplicationError(ErrorCodeLedgerNotFound, "Ledger with reference/id not found")

	ErrAccountNotFound  = NewApplicationError(ErrorCodeAccountNotFound, "Account with reference/id not found")
	ErrAccountsNotFound = NewApplicationError(
		ErrorCodeAccountsNotFound,
		"Accounts with references/ids were not found",
	)
	ErrAccountsCurrencyUnknown = NewApplicationError(
		ErrorCodeAccountsCurrencyUnknown,
		"Supplied account currency is unknown",
	)
	ErrAccountWithReferenceExists = NewApplicationError(
		ErrorCodeAccountWithReferenceExists,
		"An account with the given reference exists",
	)

	ErrTransactionNotFound = NewApplicationError(
		ErrorCodeTransactionNotFound,
		"Transaction with reference/id not found",
	)
	ErrTransactionEntriesNotFound = NewApplicationError(
		ErrorCodeTransactionEntriesNotFound,
		"Transaction no entries found",
	)
	ErrTransactionEntryHasZeroAmount = NewApplicationError(
		ErrorCodeTransactionEntryHasZeroAmount,
		"Transaction entry has zero amount",
	)
	ErrTransactionAccountsDifferCurrency = NewApplicationError(
		ErrorCodeTransactionAccountsDifferCurrency,
		"Transaction accounts have different currencies",
	)
	ErrTransactionAlreadyExists = NewApplicationError(
		ErrorCodeTransactionAlreadyExists,
		"Transaction with reference/id already exists",
	)
	ErrTransactionHasNonZeroSum = NewApplicationError(
		ErrorCodeTransactionHasNonZeroSum,
		"Transaction has a non zero sum",
	)
	ErrTransactionHasInvalidDrCrEntry = NewApplicationError(
		ErrorCodeTransactionHasInvalidDrCrEntry,
		"Transaction has a invalid count of dr/cr entries",
	)
	ErrTransactionIsConfilicting = NewApplicationError(
		ErrorCodeTransactionIsConflicting,
		"Transaction is conflicting",
	)
	ErrTransactionTypeNotReversible = NewApplicationError(
		ErrorCodeTransactionTypeNotReversible,
		"Transaction type is not reversible",
	)

	ErrSearchNamespaceUnknown = NewApplicationError(
		ErrorCodeSearchNamespaceUnknown,
		"Search namespace provided is unknown",
	)
	ErrSearchQueryHasInvalidFormart = NewApplicationError(
		ErrorCodeSearchQueryHasInvalidFormat,
		"Search query has invalid format",
	)
	ErrSearchQueryHasInvalidKeys = NewApplicationError(
		ErrorCodeSearchQueryHasInvalidKeys,
		"Search query has invalid keys",
	)
	ErrSearchQueryResultsNotCasting = NewApplicationError(
		ErrorCodeSearchQueryResultsNotCasting,
		"Search query results not casting",
	)
)
