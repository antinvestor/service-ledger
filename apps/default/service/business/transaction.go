package business

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/workerpool"
	"github.com/shopspring/decimal"
)

// TransactionBusiness defines the business interface for transaction operations.
type TransactionBusiness interface {
	CreateTransaction(ctx context.Context, req *ledgerv1.CreateTransactionRequest) (*ledgerv1.Transaction, error)
	SearchTransactions(ctx context.Context, req *commonv1.SearchRequest,
		consumer func(ctx context.Context, batch []*ledgerv1.Transaction) error) error
	GetTransaction(ctx context.Context, id string) (*ledgerv1.Transaction, error)
	UpdateTransaction(ctx context.Context, req *ledgerv1.UpdateTransactionRequest) (*ledgerv1.Transaction, error)
	ReverseTransaction(ctx context.Context, req *ledgerv1.ReverseTransactionRequest) (*ledgerv1.Transaction, error)
	DeleteTransaction(ctx context.Context, id string) error
	SearchEntries(ctx context.Context, req *commonv1.SearchRequest, consumer func(ctx context.Context, batch []*ledgerv1.TransactionEntry) error) error

	IsConflict(
		ctx context.Context, transaction2 *models.Transaction) (bool, error)
	Transact(
		ctx context.Context, transaction *models.Transaction) (*models.Transaction, error)
}

// transactionBusiness implements the TransactionBusiness interface.
type transactionBusiness struct {
	workMan         workerpool.Manager
	transactionRepo repository.TransactionRepository
	accountRepo     repository.AccountRepository
}

// NewTransactionBusiness creates a new transaction business instance.
func NewTransactionBusiness(
	workMan workerpool.Manager,
	accountRepo repository.AccountRepository,
	transactionRepo repository.TransactionRepository,
) TransactionBusiness {
	return &transactionBusiness{
		workMan:         workMan,
		transactionRepo: transactionRepo,
		accountRepo:     accountRepo,
	}
}

// CreateTransaction creates a new transaction with business validation.
func (b *transactionBusiness) CreateTransaction(
	ctx context.Context,
	req *ledgerv1.CreateTransactionRequest,
) (*ledgerv1.Transaction, error) {
	// Business logic validation
	if req.GetId() == "" {
		return nil, ErrTransactionReferenceRequired
	}

	if req.GetCurrency() == "" {
		return nil, ErrTransactionAccountIDRequired
	}

	// Convert API request to model
	transactionModel := models.TransactionFromAPI(ctx, &ledgerv1.Transaction{
		Id:           req.GetId(),
		CurrencyCode: req.GetCurrency(),
		TransactedAt: req.GetTransactedAt(),
		Data:         req.GetData(),
		Entries:      req.GetEntries(),
		Cleared:      req.GetCleared(),
		Type:         req.GetType(),
	})

	// Perform business validation
	err := b.validateTransaction(ctx, transactionModel)
	if err != nil {
		return nil, err
	}

	// Get accounts for entry validation
	// Extract account IDs from transaction entries
	accountIDs := make([]string, 0, len(transactionModel.Entries))
	for _, entry := range transactionModel.Entries {
		accountIDs = append(accountIDs, entry.AccountID)
	}

	// Try to get accounts one by one as a workaround
	accountsMap := make(map[string]*models.Account)
	for _, accountID := range accountIDs {
		account, err := b.accountRepo.GetByID(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("failed to get account %s: %w", accountID, err)
		}
		if account == nil {
			return nil, fmt.Errorf("account %s not found", accountID)
		}
		accountsMap[accountID] = account
	}

	// Validate transaction entries against accounts
	err = b.validateTransactionEntries(transactionModel, accountsMap)
	if err != nil {
		return nil, err
	}

	// Process transaction entries (apply business logic for balances and signage)
	b.processTransactionEntries(transactionModel, accountsMap)

	// Create the transaction through repository
	result, err := b.Transact(ctx, transactionModel)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	// Convert back to API type
	return result.ToAPI(), nil
}

// validateTransaction performs business validation for a transaction
func (b *transactionBusiness) validateTransaction(ctx context.Context, txn *models.Transaction) error {
	if ledgerv1.TransactionType_NORMAL.String() == txn.TransactionType ||
		ledgerv1.TransactionType_REVERSAL.String() == txn.TransactionType {
		// Skip if the transaction is invalid
		// by validating the amount values
		if !txn.IsZeroSum() {
			return ErrTransactionNonZeroSum
		}

		if !txn.IsTrueDrCr() {
			return ErrTransactionInvalidDrCrEntry
		}
	} else if ledgerv1.TransactionType_RESERVATION.String() == txn.TransactionType {
		if len(txn.Entries) != 1 {
			return ErrTransactionInvalidDrCrEntry
		}
	}

	if len(txn.Entries) == 0 {
		return ErrTransactionEntriesNotFound
	}

	return nil
}

// validateTransactionEntries validates transaction entries against accounts
func (b *transactionBusiness) validateTransactionEntries(txn *models.Transaction, accountsMap map[string]*models.Account) error {
	for _, entry := range txn.Entries {
		if entry.Amount.Decimal.IsZero() {
			return fmt.Errorf("%w: entry [id=%s, account_id=%s] amount is zero", ErrTransactionEntryZeroAmount, entry.ID, entry.AccountID)
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			// Accounts have to be predefined hence check all references exist.
			return fmt.Errorf("%w: Account %s was not found in the system", ErrTransactionAccountNotFound, entry.AccountID)
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			return fmt.Errorf("%w: entry [id=%s, account_id=%s] currency [%s] != [%s]",
				ErrTransactionAccountsDifferCurrency, entry.ID, entry.AccountID, account.Currency, txn.Currency)
		}
	}
	return nil
}

// processTransactionEntries applies business logic to transaction entries
func (b *transactionBusiness) processTransactionEntries(txn *models.Transaction, accountsMap map[string]*models.Account) {
	typedLedgerMap := make(map[string][]string)
	typedLedgerMap[models.LedgerTypeAsset] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeExpense] = []string{"DR", "CR"}
	typedLedgerMap[models.LedgerTypeLiability] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeIncome] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeCapital] = []string{"CR", "DR"}

	// Add transaction Entries in one go to succeed or fail all
	for _, line := range txn.Entries {
		account := accountsMap[line.AccountID]

		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)

		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit &&
			(account.LedgerType == models.LedgerTypeAsset || account.LedgerType == models.LedgerTypeExpense) ||
			!line.Credit &&
				(account.LedgerType == models.LedgerTypeLiability || account.LedgerType == models.LedgerTypeIncome || account.LedgerType == models.LedgerTypeCapital) {
			line.Amount = decimal.NewNullDecimal(line.Amount.Decimal.Neg())
		}
	}
}

// SearchTransactions searches for transactions based on query.
func (b *transactionBusiness) SearchTransactions(ctx context.Context, req *commonv1.SearchRequest,
	consumer func(ctx context.Context, batch []*ledgerv1.Transaction) error) error {
	// Business logic for search validation
	query := req.GetQuery()
	if query == "" {
		query = "{}" // Default empty query
	}

	// Search through repository
	result, err := b.transactionRepo.SearchAsESQ(ctx, query)
	if err != nil {
		return err
	}

	for {
		res, ok := result.ReadResult(ctx)
		if !ok {
			return nil
		}

		if res.IsError() {
			return res.Error()
		}

		var apiResults []*ledgerv1.Transaction
		for _, transaction := range res.Item() {
			apiResults = append(apiResults, transaction.ToAPI())
		}

		jobErr := consumer(ctx, apiResults)
		if jobErr != nil {
			return jobErr
		}
	}

}

// GetTransaction retrieves a transaction by ID.
func (b *transactionBusiness) GetTransaction(ctx context.Context, id string) (*ledgerv1.Transaction, error) {
	if id == "" {
		return nil, ErrTransactionIDRequired
	}

	transaction, err := b.transactionRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return transaction.ToAPI(), nil
}

// UpdateTransaction updates an existing transaction.
func (b *transactionBusiness) UpdateTransaction(
	ctx context.Context,
	req *ledgerv1.UpdateTransactionRequest,
) (*ledgerv1.Transaction, error) {
	// Business logic validation
	if req.GetId() == "" {
		return nil, ErrTransactionIDRequired
	}

	// Convert API request to model - need to get existing transaction first
	existingTransaction, err := b.transactionRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Update fields from request
	if req.GetData() != nil {
		dataMap := data.JSONMap{}
		dataMap = dataMap.FromProtoStruct(req.GetData())

		for key, value := range dataMap {
			if value != "" && value != existingTransaction.Data[key] {
				existingTransaction.Data[key] = value
			}
		}
	}

	if existingTransaction.ClearedAt.IsZero() {

		if req.ClearedAt != "" {

			clearanceTime, parseErr := time.Parse(DefaultTimestamLayout, req.ClearedAt)
			if parseErr != nil {
				return nil, parseErr
			}

			accountsMap, validationErr := b.Validate(ctx, existingTransaction)
			if validationErr != nil {
				return nil, validationErr
			}

			for _, line := range existingTransaction.Entries {
				account := accountsMap[line.AccountID]
				line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)
			}
			existingTransaction.ClearedAt = clearanceTime
		}
	}

	// Update through repository
	_, err = b.transactionRepo.Update(ctx, existingTransaction)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return existingTransaction.ToAPI(), nil
}

// ReverseTransaction reverses a transaction by creating offsetting entries.
func (b *transactionBusiness) ReverseTransaction(
	ctx context.Context,
	req *ledgerv1.ReverseTransactionRequest,
) (*ledgerv1.Transaction, error) {
	// Business logic validation
	if req.GetId() == "" {
		return nil, ErrTransactionIDRequired
	}

	// Get the original transaction to reverse
	originalTxn, err := b.transactionRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err)
	}

	if originalTxn.TransactionType != ledgerv1.TransactionType_NORMAL.String() {
		return nil, apperrors.ErrTransactionTypeNotReversible.Extend(
			fmt.Sprintf("transaction (type=%s) is not reversible", originalTxn.TransactionType),
		)
	}

	for _, entry := range originalTxn.Entries {
		entry.ID = fmt.Sprintf("%s_REVERSAL", entry.ID)
		entry.Credit = !entry.Credit
	}

	originalTxn.ID = fmt.Sprintf("%s_REVERSAL", originalTxn.ID)
	originalTxn.TransactionType = ledgerv1.TransactionType_REVERSAL.String()

	timeNow := time.Now()
	originalTxn.TransactedAt = timeNow
	originalTxn.CreatedAt = timeNow
	originalTxn.ModifiedAt = timeNow

	reversedTxn, err := b.Transact(ctx, originalTxn)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return reversedTxn.ToAPI(), nil
}

// DeleteTransaction deletes a transaction by ID.
func (b *transactionBusiness) DeleteTransaction(ctx context.Context, id string) error {
	if id == "" {
		return ErrTransactionIDRequired
	}

	// Delete through repository
	return nil // Implementation depends on repository interface
}

// SearchEntries searches for transaction entries based on query.
func (b *transactionBusiness) SearchEntries(ctx context.Context, req *commonv1.SearchRequest, consumer func(ctx context.Context, batch []*ledgerv1.TransactionEntry) error) error {
	// Business logic for search validation
	query := req.GetQuery()
	if query == "" {
		query = "{}" // Default empty query
	}

	// Search through repository
	result, err := b.transactionRepo.SearchEntries(ctx, query)
	if err != nil {
		return err
	}

	for {
		res, ok := result.ReadResult(ctx)
		if !ok {
			return nil
		}

		if res.IsError() {
			return res.Error()
		}

		var apiResults []*ledgerv1.TransactionEntry
		for _, txEntry := range res.Item() {
			apiResults = append(apiResults, txEntry.ToAPI())
		}

		jobErr := consumer(ctx, apiResults)
		if jobErr != nil {
			return jobErr
		}
	}
}

// Validate checks all issues around transaction are satisfied.
func (t *transactionBusiness) Validate(
	ctx context.Context,
	txn *models.Transaction,
) (map[string]*models.Account, error) {
	if ledgerv1.TransactionType_NORMAL.String() == txn.TransactionType ||
		ledgerv1.TransactionType_REVERSAL.String() == txn.TransactionType {
		// Skip if the transaction is invalid
		// by validating the amount values
		if !txn.IsZeroSum() {
			return nil, apperrors.ErrTransactionHasNonZeroSum
		}

		if !txn.IsTrueDrCr() {
			return nil, apperrors.ErrTransactionHasInvalidDrCrEntry
		}
	} else if ledgerv1.TransactionType_RESERVATION.String() == txn.TransactionType {
		if len(txn.Entries) != 1 {
			return nil, apperrors.ErrTransactionHasInvalidDrCrEntry
		}
	}

	if len(txn.Entries) == 0 {
		return nil, apperrors.ErrTransactionEntriesNotFound
	}

	accountIDSet := map[string]bool{}
	for _, entry := range txn.Entries {
		accountIDSet[entry.AccountID] = true
	}

	accountIDs := make([]string, 0, len(accountIDSet))
	for accountID := range accountIDSet {
		accountIDs = append(accountIDs, accountID)
	}

	// Retrieve accounts from database
	accountsMap, errAcc := t.accountRepo.ListByID(ctx, accountIDs...)
	if errAcc != nil {
		return nil, errAcc
	}

	for _, entry := range txn.Entries {
		if entry.Amount.Decimal.IsZero() {
			return nil, apperrors.ErrTransactionEntryHasZeroAmount.Extend(
				fmt.Sprintf("entry [id=%s, account_id=%s] amount is zero", entry.ID, entry.AccountID),
			)
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			// // Accounts have to be predefined hence check all references exist.
			return nil, apperrors.ErrAccountNotFound.Extend(
				fmt.Sprintf("Account %s was not found in the system", entry.AccountID),
			)
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			return nil, apperrors.ErrTransactionAccountsDifferCurrency.Extend(
				fmt.Sprintf(
					"entry [id=%s, account_id=%s] currency [%s] != [%s]",
					entry.ID,
					entry.AccountID,
					account.Currency,
					txn.Currency,
				),
			)
		}
	}

	return accountsMap, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction.
func (b *transactionBusiness) IsConflict(
	ctx context.Context, transaction2 *models.Transaction) (bool, error) {
	transaction1, err := b.transactionRepo.GetByID(ctx, transaction2.ID)
	if err != nil {
		return false, apperrors.ErrSystemFailure.Override(err)
	}

	// CompareMoney new and existing transaction Entries
	return !containsSameElements(transaction1.Entries, transaction2.Entries), nil
}

// Transact creates the input transaction in the DB.
func (b *transactionBusiness) Transact(
	ctx context.Context, transaction *models.Transaction,
) (*models.Transaction, error) {
	// Check if a transaction with Reference already exists
	existingTransaction, aerr := b.transactionRepo.GetByID(ctx, transaction.GetID())
	if aerr != nil {

		if !data.ErrorIsNoRows(aerr) {
			return nil, apperrors.ErrSystemFailure.Override(aerr)
		}
	}

	if existingTransaction != nil {
		var conflictErr error
		isConflict, conflictErr := b.IsConflict(ctx, transaction)
		if conflictErr != nil {
			return nil, conflictErr
		}
		if isConflict {
			return nil, apperrors.ErrTransactionIsConfilicting
		}
		return existingTransaction, nil
	}

	accountsMap, aerr := b.Validate(ctx, transaction)
	if aerr != nil {
		var appErr apperrors.ApplicationError
		if errors.As(aerr, &appErr) {
			return nil, appErr
		}
		return nil, apperrors.ErrSystemFailure.Override(aerr)
	}

	typedLedgerMap := make(map[string][]string)
	typedLedgerMap[models.LedgerTypeAsset] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeExpense] = []string{"DR", "CR"}
	typedLedgerMap[models.LedgerTypeLiability] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeIncome] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeCapital] = []string{"CR", "DR"}

	// Add transaction Entries in one go to succeed or fail all
	for _, line := range transaction.Entries {
		account := accountsMap[line.AccountID]

		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)

		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit &&
			(account.LedgerType == models.LedgerTypeAsset || account.LedgerType == models.LedgerTypeExpense) ||
			!line.Credit &&
				(account.LedgerType == models.LedgerTypeLiability || account.LedgerType == models.LedgerTypeIncome || account.LedgerType == models.LedgerTypeCapital) {
			line.Amount = decimal.NewNullDecimal(line.Amount.Decimal.Neg())
		}
	}

	// Create the transaction and its entries
	err := b.transactionRepo.Create(ctx, transaction)
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err)
	}

	timeNow := time.Now()
	if transaction.TransactedAt.IsZero() {
		transaction.TransactedAt = timeNow
	}
	// Only set ClearedAt if it was explicitly provided and is not zero
	// Don't automatically clear transactions that should remain uncleared

	result, err := b.transactionRepo.GetByID(ctx, transaction.GetID())
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err)
	}
	return result, nil
}
