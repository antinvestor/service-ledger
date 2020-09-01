package ledger

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

func New(Code int32, Message string) ApplicationLedgerError {
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

// Extend default Message
func (e applicationLedgerError) Override(errs ...error) ApplicationLedgerError {

	errorStrings := make([]string, len(errs))

	for i, err := range errs {
		errorStrings[i] = err.Error()
	}
	return &applicationLedgerError{e.Code, e.CodeOffset, e.Message, strings.Join(errorStrings, "\n")}
}

var (
	ErrorSystemFailure        = New(1, "System failure")
	ErrorUnspecifiedID        = New(2, "No ID was supplied")
	ErrorUnspecifiedReference = New(3, "No reference was supplied")

	ErrorLedgerNotFound = New(11, "Ledger with reference/id not found")

	ErrorAccountNotFound            = New(21, "Account with reference/id not found")
	ErrorAccountsNotFound           = New(22, "Accounts with references/ids were not found")
	ErrorAccountsCurrencyUnknown    = New(22, "Supplied account currency is unknown")
	ErrorAccountWithReferenceExists = New(23, "An account with the given reference exists")

	ErrorTransactionNotFound               = New(31, "Transaction with reference/id not found")
	ErrorTransactionEntriesNotFound        = New(32, "Transaction with reference/id has no entries not found")
	ErrorTransactionEntryHasZeroAmount     = New(33, "Transaction entry has zero amount")
	ErrorTransactionAccountsDifferCurrency = New(34, "Transaction accounts have different currencies")
	ErrorTransactionAlreadyExists          = New(35, "Transaction with reference/id already exists")
	ErrorTransactionHasNonZeroSum          = New(36, "Transaction has a non zero sum")
	ErrorTransactionIsConfilicting         = New(37, "Transaction is conflicting")

	ErrorSearchNamespaceUnknown       = New(41, "Search namespace provided is unknown")
	ErrorSearchQueryHasInvalidFormart = New(42, "Search query has invalid format")
	ErrorSearchQueryHasInvalidKeys    = New(43, "Search query has invalid keys")
	ErrorSearchQueryResultsNotCasting = New(44, "Search query results not casting")
)
