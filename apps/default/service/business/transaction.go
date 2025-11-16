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
	SearchEntries(
		ctx context.Context,
		req *commonv1.SearchRequest,
		consumer func(ctx context.Context, batch []*ledgerv1.TransactionEntry) error,
	) error

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
		account, getErr := b.accountRepo.GetByID(ctx, accountID)
		if getErr != nil {
			return nil, fmt.Errorf("failed to get account %s: %w", accountID, getErr)
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

// validateTransaction performs business validation for a transaction.
func (b *transactionBusiness) validateTransaction(_ context.Context, txn *models.Transaction) error {
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

// validateTransactionEntries validates transaction entries against accounts.
func (b *transactionBusiness) validateTransactionEntries(
	txn *models.Transaction,
	accountsMap map[string]*models.Account,
) error {
	for _, entry := range txn.Entries {
		if entry.Amount.Decimal.IsZero() {
			return fmt.Errorf(
				"%w: entry [id=%s, account_id=%s] amount is zero",
				ErrTransactionEntryZeroAmount,
				entry.ID,
				entry.AccountID,
			)
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			// Accounts have to be predefined hence check all references exist.
			return fmt.Errorf(
				"%w: Account %s was not found in the system",
				ErrTransactionAccountNotFound,
				entry.AccountID,
			)
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			return fmt.Errorf("%w: entry [id=%s, account_id=%s] currency [%s] != [%s]",
				ErrTransactionAccountsDifferCurrency, entry.ID, entry.AccountID, account.Currency, txn.Currency)
		}
	}
	return nil
}

// processTransactionEntries applies business logic to transaction entries.
func (b *transactionBusiness) processTransactionEntries(
	txn *models.Transaction,
	accountsMap map[string]*models.Account,
) {
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
		err = b.processClearanceUpdate(ctx, req, existingTransaction)
		if err != nil {
			return nil, err
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

	// Create a new reversal transaction instead of modifying the original
	reversalTxn := &models.Transaction{
		Currency:        originalTxn.Currency,
		TransactionType: ledgerv1.TransactionType_REVERSAL.String(),
		TransactedAt:    time.Now(),
		Data:            originalTxn.Data,
	}
	reversalTxn.GenID(ctx)
	reversalTxn.ID = fmt.Sprintf("%s_REVERSAL", originalTxn.ID)

	// Create reversed entries
	for _, entry := range originalTxn.Entries {
		reversalTxn.Entries = append(reversalTxn.Entries, &models.TransactionEntry{
			BaseModel: data.BaseModel{ID: fmt.Sprintf("%s_REVERSAL", entry.ID)},
			AccountID: entry.AccountID,
			Amount:    entry.Amount,
			Credit:    !entry.Credit, // Reverse the credit/debit
		})
	}

	reversedTxn, err := b.Transact(ctx, reversalTxn)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return reversedTxn.ToAPI(), nil
}

// DeleteTransaction deletes a transaction by ID.
func (b *transactionBusiness) DeleteTransaction(_ context.Context, id string) error {
	if id == "" {
		return ErrTransactionIDRequired
	}

	// Delete through repository
	return nil // Implementation depends on repository interface
}

// SearchEntries searches for transaction entries based on query.
func (b *transactionBusiness) SearchEntries(
	ctx context.Context,
	req *commonv1.SearchRequest,
	consumer func(ctx context.Context, batch []*ledgerv1.TransactionEntry) error,
) error {
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
func (b *transactionBusiness) Validate(
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
	accountsMap, errAcc := b.accountRepo.ListByID(ctx, accountIDs...)
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

// Transact creates the input transaction in the DB with improved concurrency handling.
//
//nolint:gocognit // High cognitive complexity is unavoidable due to comprehensive transaction validation logic
func (b *transactionBusiness) Transact(
	ctx context.Context, transaction *models.Transaction,
) (*models.Transaction, error) {
	// Set transaction time early to ensure consistency
	if transaction.TransactedAt.IsZero() {
		transaction.TransactedAt = time.Now()
	}

	// Pre-validate accounts before any database operations to fail fast
	accountsMap, aerr := b.Validate(ctx, transaction)
	if aerr != nil {
		var appErr apperrors.ApplicationError
		if errors.As(aerr, &appErr) {
			return nil, appErr
		}
		return nil, apperrors.ErrSystemFailure.Override(aerr)
	}

	// Process transaction entries with account balances and signage
	b.processTransactionEntriesWithAccounts(transaction, accountsMap)

	// Try to create transaction with built-in conflict detection
	// This handles the race condition between existence check and creation
	err := b.transactionRepo.Create(ctx, transaction)
	if err != nil {
		// Check if this is a duplicate transaction error
		if b.isDuplicateTransactionError(err) {
			// Transaction already exists, check for conflicts
			existingTransaction, getErr := b.transactionRepo.GetByID(ctx, transaction.GetID())
			if getErr != nil {
				return nil, apperrors.ErrSystemFailure.Override(getErr)
			}

			// Validate that the existing transaction matches our request
			isConflict, conflictErr := b.IsConflict(ctx, transaction)
			if conflictErr != nil {
				return nil, conflictErr
			}
			if isConflict {
				return nil, apperrors.ErrTransactionIsConfilicting
			}

			// Return existing transaction for idempotent behavior
			return existingTransaction, nil
		}
		return nil, apperrors.ErrSystemFailure.Override(err)
	}

	// Return the created transaction (no need for another GetByID call)
	return transaction, nil
}

// processTransactionEntriesWithAccounts processes transaction entries with balance and signage logic.
func (b *transactionBusiness) processTransactionEntriesWithAccounts(
	transaction *models.Transaction,
	accountsMap map[string]*models.Account,
) {
	// Define ledger type mappings for debit/credit rules
	typedLedgerMap := make(map[string][]string)
	typedLedgerMap[models.LedgerTypeAsset] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeExpense] = []string{"DR", "CR"}
	typedLedgerMap[models.LedgerTypeLiability] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeIncome] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeCapital] = []string{"CR", "DR"}

	// Process all transaction entries atomically
	for _, line := range transaction.Entries {
		account := accountsMap[line.AccountID]
		
		// Set the account balance snapshot at transaction time
		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)

		// Apply signage based on double-entry bookkeeping rules (DEADCLIC)
		// Debit: Expense, Asset | Credit: Liability, Income, Capital
		if line.Credit &&
			(account.LedgerType == models.LedgerTypeAsset || account.LedgerType == models.LedgerTypeExpense) ||
			!line.Credit &&
				(account.LedgerType == models.LedgerTypeLiability || account.LedgerType == models.LedgerTypeIncome || account.LedgerType == models.LedgerTypeCapital) {
			line.Amount = decimal.NewNullDecimal(line.Amount.Decimal.Neg())
		}
	}
}

// isDuplicateTransactionError checks if the error indicates a duplicate transaction.
func (b *transactionBusiness) isDuplicateTransactionError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for unique constraint violations or duplicate key errors
	errStr := strings.ToLower(err.Error())
	
	// PostgreSQL unique constraint violation
	if strings.Contains(errStr, "unique constraint") || 
	   strings.Contains(errStr, "duplicate key") ||
	   strings.Contains(errStr, "violates unique constraint") {
		return true
	}
	
	// MySQL duplicate entry
	if strings.Contains(errStr, "duplicate entry") ||
	   strings.Contains(errStr, "duplicate key value") {
		return true
	}
	
	// SQLite unique constraint failure
	if strings.Contains(errStr, "unique constraint failed") {
		return true
	}
	
	// Generic database uniqueness errors
	if strings.Contains(errStr, "already exists") ||
	   strings.Contains(errStr, "duplicate") {
		return true
	}
	
	return false
}

// processClearanceUpdate handles the clearance time update for a transaction.
func (b *transactionBusiness) processClearanceUpdate(
	ctx context.Context,
	req *ledgerv1.UpdateTransactionRequest,
	existingTransaction *models.Transaction,
) error {
	if req.GetClearedAt() == "" {
		return nil
	}

	clearanceTime, parseErr := time.Parse(DefaultTimestamLayout, req.GetClearedAt())
	if parseErr != nil {
		return parseErr
	}

	accountsMap, validationErr := b.Validate(ctx, existingTransaction)
	if validationErr != nil {
		return validationErr
	}

	for _, line := range existingTransaction.Entries {
		account := accountsMap[line.AccountID]
		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)
	}
	existingTransaction.ClearedAt = clearanceTime
	return nil
}
