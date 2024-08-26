package utility

import (
	"fmt"
	"strings"
)

type ApplicationLedgerError interface {
	error
	ErrorCode() int32
	String() string
	Extend(Message string) ApplicationLedgerError
	Override(errs ...error) ApplicationLedgerError
}

type applicationLedgerError struct {
	Code         int32
	CodeOffset   int32
	Message      string
	ExtraMessage string
}

func NewApplicationError(Code int32, Message string) ApplicationLedgerError {
	return &applicationLedgerError{Code, 200, Message, ""}
}

func (e applicationLedgerError) Error() string {

	if e.ExtraMessage != "" {
		return fmt.Sprintf("%d  : - %s  \n extra info : %s", e.ErrorCode(), e.Message, e.ExtraMessage)
	} else {
		return fmt.Sprintf("%d  : - %s  ", e.Code, e.Message)
	}
}

// ErrorCode returns the unique Code of the error
func (e applicationLedgerError) ErrorCode() int32 {
	return e.CodeOffset + e.Code
}

// String implementation supports logging
func (e applicationLedgerError) String() string {
	return e.Error()
}

// Extend default Message
func (e applicationLedgerError) Extend(Message string) ApplicationLedgerError {
	return &applicationLedgerError{e.Code, e.CodeOffset, e.Message, Message}
}

// Override default Message
func (e applicationLedgerError) Override(errs ...error) ApplicationLedgerError {

	errorStrings := make([]string, len(errs))

	for i, err := range errs {
		errorStrings[i] = err.Error()
	}
	return &applicationLedgerError{e.Code, e.CodeOffset, e.Message, strings.Join(errorStrings, "\n")}
}

var (
	ErrorSystemFailure        = NewApplicationError(1, "Internal System failure")
	ErrorUnspecifiedID        = NewApplicationError(2, "No ID was supplied")
	ErrorUnspecifiedReference = NewApplicationError(3, "No reference was supplied")
	ErrorBadDataSupplied      = NewApplicationError(4, "Invalid data format was supplied")

	ErrorLedgerNotFound = NewApplicationError(11, "Ledger with reference/id not found")

	ErrorAccountNotFound            = NewApplicationError(21, "Account with reference/id not found")
	ErrorAccountsNotFound           = NewApplicationError(22, "Accounts with references/ids were not found")
	ErrorAccountsCurrencyUnknown    = NewApplicationError(22, "Supplied account currency is unknown")
	ErrorAccountWithReferenceExists = NewApplicationError(23, "An account with the given reference exists")

	ErrorTransactionNotFound               = NewApplicationError(31, "Transaction with reference/id not found")
	ErrorTransactionEntriesNotFound        = NewApplicationError(32, "Transaction no entries found")
	ErrorTransactionEntryHasZeroAmount     = NewApplicationError(33, "Transaction entry has zero amount")
	ErrorTransactionAccountsDifferCurrency = NewApplicationError(34, "Transaction accounts have different currencies")
	ErrorTransactionAlreadyExists          = NewApplicationError(35, "Transaction with reference/id already exists")
	ErrorTransactionHasNonZeroSum          = NewApplicationError(36, "Transaction has a non zero sum")
	ErrorTransactionHasInvalidDrCrEntry    = NewApplicationError(37, "Transaction has a invalid count of dr/cr entries")
	ErrorTransactionIsConfilicting         = NewApplicationError(38, "Transaction is conflicting")
	ErrorTransactionTypeNotReversible      = NewApplicationError(39, "Transaction type is not reversible")

	ErrorSearchNamespaceUnknown       = NewApplicationError(61, "Search namespace provided is unknown")
	ErrorSearchQueryHasInvalidFormart = NewApplicationError(62, "Search query has invalid format")
	ErrorSearchQueryHasInvalidKeys    = NewApplicationError(63, "Search query has invalid keys")
	ErrorSearchQueryResultsNotCasting = NewApplicationError(64, "Search query results not casting")
)
