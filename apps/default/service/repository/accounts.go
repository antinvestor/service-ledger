package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/datastore"
	"github.com/pitabwire/frame/datastore/pool"
	"github.com/pitabwire/frame/workerpool"
	"github.com/pitabwire/util"
	"golang.org/x/text/currency"
)

const constAccountQuery = `WITH current_balance_summary AS (
    SELECT 
        e.account_id, 
        t.currency,
        COALESCE(SUM(CASE WHEN t.transaction_type IN ('NORMAL', 'REVERSAL') AND t.cleared_at IS NOT NULL AND t.cleared_at != '0001-01-01 00:00:00' THEN e.amount ELSE 0 END), 0) AS balance,
        COALESCE(SUM(CASE WHEN t.transaction_type IN ('NORMAL', 'REVERSAL') AND (t.cleared_at IS NULL OR t.cleared_at = '0001-01-01 00:00:00') THEN e.amount ELSE 0 END), 0) AS uncleared_balance,
        COALESCE(SUM(CASE WHEN t.transaction_type = 'RESERVATION' THEN e.amount ELSE 0 END), 0) AS reserved_balance
    FROM transaction_entries e 
    LEFT JOIN transactions t ON e.transaction_id = t.id
    GROUP BY e.account_id, t.currency
)
SELECT 
    a.id,
    a.currency,
    a.data,
    COALESCE(bs.balance, 0) AS total_balance,
    COALESCE(bs.uncleared_balance, 0) AS total_uncleared_balance,
    COALESCE(bs.reserved_balance, 0) AS total_reserved_balance,
    a.ledger_id,
    a.ledger_type,
    a.created_at,
    a.modified_at,
    a.version,
    a.tenant_id,
    a.partition_id,
    a.access_id,
    a.deleted_at
FROM accounts a
LEFT JOIN current_balance_summary bs ON a.id = bs.account_id AND a.currency = bs.currency `

type AccountRepository interface {
	datastore.BaseRepository[*models.Account]
	SearchAsESQ(ctx context.Context, query string) (workerpool.JobResultPipe[[]*models.Account], error)
	ListByID(ctx context.Context, ids ...string) (map[string]*models.Account, apperrors.ApplicationError)
}

// accountRepository provides all functions related to ledger account.
type accountRepository struct {
	datastore.BaseRepository[*models.Account]
	ledgerRepository LedgerRepository
}

// NewAccountRepository provides instance of `accountRepository`.
func NewAccountRepository(
	ctx context.Context,
	dbPool pool.Pool,
	workMan workerpool.Manager,
	ledgerRepository LedgerRepository,
) AccountRepository {
	return &accountRepository{
		BaseRepository: datastore.NewBaseRepository[*models.Account](
			ctx, dbPool, workMan, func() *models.Account { return &models.Account{} },
		),
		ledgerRepository: ledgerRepository,
	}
}

// GetByID returns an acccount with the given Reference.
func (a *accountRepository) GetByID(
	ctx context.Context,
	id string,
) (*models.Account, error) {
	if id == "" {
		return nil, apperrors.ErrUnspecifiedID
	}

	accList, err := a.ListByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return accList[id], nil
}

// ListByID returns a list of acccounts with the given list of ids.
func (a *accountRepository) ListByID(
	ctx context.Context,
	ids ...string,
) (map[string]*models.Account, apperrors.ApplicationError) {
	if len(ids) == 0 {
		return nil, apperrors.ErrAccountsNotFound.Extend("No Accounts were specified")
	}

	accountsMap := map[string]*models.Account{}

	queryMap := map[string]any{
		"query": map[string]any{
			"must": map[string]any{
				"fields": []map[string]any{
					{
						"id": map[string][]string{
							"in": ids,
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(queryMap)
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err).Extend("Json marshalling error")
	}

	query := string(queryBytes)

	jobResult, err := a.SearchAsESQ(ctx, query)
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err).Extend(fmt.Sprintf("db query error [%s]", query))
	}

	for {
		result, ok := jobResult.ReadResult(ctx)

		if !ok {
			return accountsMap, nil
		}

		if result.IsError() {
			return nil, apperrors.ErrSystemFailure.Override(result.Error())
		}

		for _, acc := range result.Item() {
			accountsMap[acc.ID] = acc
		}
	}
}

func (a *accountRepository) searchAccounts(ctx context.Context, sqlQuery *SearchSQLQuery) ([]*models.Account, error) {
	rows, err := a.Pool().DB(ctx, true).
		Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
		Raw(fmt.Sprintf(`%s WHERE %s`, constAccountQuery, sqlQuery.sql), sqlQuery.args...).Rows()
	if err != nil {
		return nil, err
	}

	defer util.CloseAndLogOnError(ctx, rows, "could not close account rows")

	var accountList []*models.Account
	for rows.Next() {
		acc := models.Account{}
		err = rows.Scan(
			&acc.ID, &acc.Currency, &acc.Data, &acc.Balance, &acc.UnClearedBalance, &acc.ReservedBalance,
			&acc.LedgerID, &acc.LedgerType, &acc.CreatedAt, &acc.ModifiedAt, &acc.Version, &acc.TenantID,
			&acc.PartitionID, &acc.AccessID, &acc.DeletedAt)
		if err != nil {
			return accountList, err
		}
		accountList = append(accountList, &acc)
	}

	return accountList, nil
}

func (a *accountRepository) SearchAsESQ(
	ctx context.Context,
	query string,
) (workerpool.JobResultPipe[[]*models.Account], error) {
	job := workerpool.NewJob(func(ctx context.Context, jobResult workerpool.JobResultPipe[[]*models.Account]) error {
		rawQuery, aerr := NewSearchRawQuery(ctx, query)
		if aerr != nil {
			return jobResult.WriteError(ctx, aerr)
		}

		sqlQuery := rawQuery.ToQueryConditions()

		for sqlQuery.canLoad() {
			accountList, dbErr := a.searchAccounts(ctx, sqlQuery)
			if dbErr != nil {
				if data.ErrorIsNoRows(dbErr) {
					return jobResult.WriteError(ctx, apperrors.ErrLedgerNotFound)
				}
				return jobResult.WriteError(
					ctx,
					apperrors.ErrSystemFailure.Override(dbErr).Extend("Query execution error"),
				)
			}

			dbErr = jobResult.WriteResult(ctx, accountList)
			if dbErr != nil {
				return dbErr
			}

			if sqlQuery.stop(len(accountList)) {
				break
			}
		}
		return nil
	})

	err := workerpool.SubmitJob(ctx, a.WorkManager(), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// Create persists a new account in the ledger if its none existent.
func (a *accountRepository) Create(
	ctx context.Context,
	account *models.Account,
) error {
	if account.LedgerID != "" {
		lg, err := a.ledgerRepository.GetByID(ctx, account.LedgerID)
		if err != nil {
			return apperrors.ErrSystemFailure.Override(err)
		}
		account.LedgerID = lg.ID
		account.LedgerType = lg.Type
	}

	currencyUnit, err := currency.ParseISO(account.Currency)
	if err != nil {
		return apperrors.ErrAccountsCurrencyUnknown
	}

	account.Currency = currencyUnit.String()

	return a.Create(ctx, account)
}
