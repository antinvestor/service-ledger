package repository

import (
	"context"
	"fmt"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/datastore"
	"github.com/pitabwire/frame/datastore/pool"
	"github.com/pitabwire/frame/workerpool"
	"github.com/pitabwire/util"
)

type LedgerRepository interface {
	datastore.BaseRepository[*models.Ledger]
	SearchAsESQ(ctx context.Context, query string) (workerpool.JobResultPipe[[]*models.Ledger], error)
}

// LedgerRepository provides all functions related to ledger Ledger.
type ledgerRepository struct {
	datastore.BaseRepository[*models.Ledger]
}

// NewLedgerRepository provides instance of `LedgerRepository`.
func NewLedgerRepository(ctx context.Context, dbPool pool.Pool, workMan workerpool.Manager) LedgerRepository {
	return &ledgerRepository{
		BaseRepository: datastore.NewBaseRepository[*models.Ledger](
			ctx, dbPool, workMan, func() *models.Ledger { return &models.Ledger{} },
		),
	}
}

// Query constants for ledger repository.
const constLedgerQuery = `SELECT id, parent_id, data FROM ledgers`

func (l *ledgerRepository) searchLedgers(ctx context.Context, sqlQuery *SearchSQLQuery) ([]*models.Ledger, error) {
	rows, err := l.Pool().DB(ctx, true).
		Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
		Raw(fmt.Sprintf(`%s WHERE %s`, constLedgerQuery, sqlQuery.sql), sqlQuery.args...).Rows()
	if err != nil {
		return nil, err
	}

	defer util.CloseAndLogOnError(ctx, rows, "could not close ledger rows")

	ledgerList := make([]*models.Ledger, 0)
	for rows.Next() {
		ledger := new(models.Ledger)
		errR := rows.Scan(&ledger.ID, &ledger.ParentID, &ledger.Data)
		if errR != nil {
			return ledgerList, errR
		}
		ledgerList = append(ledgerList, ledger)
	}

	return ledgerList, nil
}

func (l *ledgerRepository) SearchAsESQ(
	ctx context.Context,
	query string,
) (workerpool.JobResultPipe[[]*models.Ledger], error) {
	job := workerpool.NewJob(func(ctxI context.Context, jobResult workerpool.JobResultPipe[[]*models.Ledger]) error {
		rawQuery, err := NewSearchRawQuery(ctxI, query)
		if err != nil {
			return jobResult.WriteError(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()

		for sqlQuery.canLoad() {
			ledgerList, dbErr := l.searchLedgers(ctxI, sqlQuery)
			if dbErr != nil {
				if data.ErrorIsNoRows(dbErr) {
					return jobResult.WriteError(ctx, apperrors.ErrLedgerNotFound)
				}
				return jobResult.WriteError(ctx, apperrors.ErrSystemFailure.Override(dbErr))
			}

			errR := jobResult.WriteResult(ctx, ledgerList)
			if errR != nil {
				return errR
			}

			if sqlQuery.stop(len(ledgerList)) {
				break
			}
		}

		return nil
	})

	err := workerpool.SubmitJob(ctx, l.WorkManager(), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}
