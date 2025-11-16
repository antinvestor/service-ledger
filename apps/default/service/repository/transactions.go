package repository

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame/datastore"
	"github.com/pitabwire/frame/datastore/pool"
	"github.com/pitabwire/frame/workerpool"
	"github.com/pitabwire/util"
)

type TransactionRepository interface {
	datastore.BaseRepository[*models.Transaction]
	SearchAsESQ(ctx context.Context, query string,
	) (workerpool.JobResultPipe[[]*models.Transaction], error)
	SearchEntries(ctx context.Context, query string,
	) (workerpool.JobResultPipe[[]*models.TransactionEntry], error)
}

// transactionRepository is the interface to all transaction operations.
type transactionRepository struct {
	accountRepo AccountRepository
	datastore.BaseRepository[*models.Transaction]
}

// NewTransactionRepository returns a new instance of `transactionRepository`.
func NewTransactionRepository(
	ctx context.Context,
	dbPool pool.Pool,
	workMan workerpool.Manager,
	accountRepo AccountRepository,
) TransactionRepository {
	return &transactionRepository{
		BaseRepository: datastore.NewBaseRepository[*models.Transaction](
			ctx, dbPool, workMan, func() *models.Transaction { return &models.Transaction{} },
		),
		accountRepo: accountRepo,
	}
}

func (t *transactionRepository) searchTransactions(
	ctx context.Context,
	sqlQuery *SearchSQLQuery,
) ([]*models.Transaction, error) {
	var transactionList []*models.Transaction

	result := t.Pool().DB(ctx, true).Where(sqlQuery.sql, sqlQuery.args...).Offset(sqlQuery.offset).
		Limit(sqlQuery.batchSize).Find(&transactionList)
	err1 := result.Error
	if err1 != nil {
		return transactionList, err1
	}

	if len(transactionList) > 0 {
		var transactionIDs []string
		for _, transaction := range transactionList {
			transactionIDs = append(transactionIDs, transaction.GetID())
		}

		entriesMap, err2 := t.SearchEntriesByTransactionID(ctx, transactionIDs...)
		if err2 != nil {
			return transactionList, err2
		}

		for _, transaction := range transactionList {
			entries, ok := entriesMap[transaction.GetID()]
			if ok {
				transaction.Entries = entries
			}
		}
	}

	return transactionList, nil
}

func (t *transactionRepository) SearchAsESQ(
	ctx context.Context, queryStr string,
) (workerpool.JobResultPipe[[]*models.Transaction], error) {
	job := workerpool.NewJob(
		func(ctx context.Context, jobResult workerpool.JobResultPipe[[]*models.Transaction]) error {
			rawQuery, err := NewSearchRawQuery(ctx, queryStr)
			if err != nil {
				return jobResult.WriteError(ctx, err)
			}

			sqlQuery := rawQuery.ToQueryConditions()

			for sqlQuery.canLoad() {
				transactionList, dbErr := t.searchTransactions(ctx, sqlQuery)
				if dbErr != nil {
					return jobResult.WriteError(ctx, apperrors.ErrSystemFailure.Override(dbErr))
				}
				dbErr = jobResult.WriteResult(ctx, transactionList)
				if dbErr != nil {
					return dbErr
				}

				if sqlQuery.stop(len(transactionList)) {
					break
				}
			}
			return nil
		},
	)

	err := workerpool.SubmitJob(ctx, t.WorkManager(), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (t *transactionRepository) SearchEntriesByTransactionID(
	ctx context.Context,
	transactionIDs ...string,
) (map[string][]*models.TransactionEntry, error) {
	entriesMap := make(map[string][]*models.TransactionEntry)

	queryMap := map[string]any{
		"query": map[string]any{
			"must": map[string]any{
				"fields": []map[string]any{
					{
						"transaction_id": map[string][]string{
							"in": transactionIDs,
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

	logger := util.Log(ctx)

	query := string(queryBytes)

	logger.WithField("query", query).Info("Query from database")

	jobResult, err := t.SearchEntries(ctx, query)
	if err != nil {
		logger.WithError(err).Info("could not query for entries")

		return nil, apperrors.ErrSystemFailure.Override(err).Extend(fmt.Sprintf("db query error [%s]", query))
	}

	for {
		logger.Info("reading results")

		result, ok := jobResult.ReadResult(ctx)

		if !ok {
			return entriesMap, nil
		}
		if result.IsError() {
			logger.WithError(result.Error()).Info("could not read results")
			return nil, apperrors.ErrSystemFailure.Override(result.Error())
		}

		for _, entry := range result.Item() {
			entries, ok0 := entriesMap[entry.TransactionID]
			if !ok0 {
				entries = make([]*models.TransactionEntry, 0)
			}

			entriesMap[entry.TransactionID] = append(entries, entry)
		}
	}
}

func (t *transactionRepository) SearchEntries(
	ctx context.Context,
	query string,
) (workerpool.JobResultPipe[[]*models.TransactionEntry], error) {
	job := workerpool.NewJob(
		func(ctx context.Context, jobResult workerpool.JobResultPipe[[]*models.TransactionEntry]) error {
			rawQuery, err := NewSearchRawQuery(ctx, query)
			if err != nil {
				return jobResult.WriteError(ctx, err)
			}

			sqlQuery := rawQuery.ToQueryConditions()
			var transactionEntriesList []*models.TransactionEntry

			for sqlQuery.canLoad() {
				result := t.Pool().DB(ctx, true).Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
					Where(sqlQuery.sql, sqlQuery.args...).Find(&transactionEntriesList)

				err1 := result.Error
				if err1 != nil {
					return jobResult.WriteError(ctx, apperrors.ErrSystemFailure.Override(err1))
				}

				err1 = jobResult.WriteResult(ctx, transactionEntriesList)
				if err1 != nil {
					return err1
				}

				if sqlQuery.stop(len(transactionEntriesList)) {
					break
				}
			}

			return nil
		},
	)

	err := workerpool.SubmitJob(ctx, t.WorkManager(), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}
