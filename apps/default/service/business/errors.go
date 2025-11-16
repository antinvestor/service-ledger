package business

import "errors"

// Business validation errors
var (
	// Ledger errors
	ErrLedgerReferenceRequired = errors.New("ledger reference is required")
	ErrLedgerIDRequired        = errors.New("ledger ID is required")
	ErrInvalidLedgerType       = errors.New("invalid ledger type returned from repository")

	// Account errors
	ErrAccountReferenceRequired = errors.New("account reference is required")
	ErrAccountIDRequired        = errors.New("account ID is required")
	ErrAccountLedgerIDRequired  = errors.New("account ledger ID is required")
	ErrInvalidAccountType       = errors.New("invalid account type returned from repository")

	// Transaction errors
	ErrTransactionReferenceRequired = errors.New("transaction reference is required")
	ErrTransactionIDRequired        = errors.New("transaction ID is required")
	ErrTransactionAccountIDRequired = errors.New("transaction account ID is required")
	ErrInvalidTransactionType       = errors.New("invalid transaction type returned from repository")

	// General errors
	ErrInvalidSearchResult = errors.New("invalid search result type from repository")
)
