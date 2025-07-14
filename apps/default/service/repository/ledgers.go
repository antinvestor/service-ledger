package repository

import (
	"context"
	"fmt"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame"
)

type LedgerRepository interface {
	GetByID(ctx context.Context, id string) (*models.Ledger, apperrors.ApplicationLedgerError)
	Search(ctx context.Context, query string) (frame.JobResultPipe[[]*models.Ledger], error)
	Create(ctx context.Context, ledger *models.Ledger) (*models.Ledger, apperrors.ApplicationLedgerError)
	Update(ctx context.Context, ledger *models.Ledger) (*models.Ledger, apperrors.ApplicationLedgerError)
}

// LedgerRepository provides all functions related to ledger Ledger.
type ledgerRepository struct {
	service *frame.Service
}

// NewLedgerRepository provides instance of `LedgerRepository`.
func NewLedgerRepository(service *frame.Service) LedgerRepository {
	return &ledgerRepository{service: service}
}

// GetByID returns an acccount with the given id.
func (l *ledgerRepository) GetByID(ctx context.Context, id string) (*models.Ledger, apperrors.ApplicationLedgerError) {
	if id == "" {
		return nil, apperrors.ErrUnspecifiedID
	}

	var ledger models.Ledger
	if err := l.service.DB(ctx, true).Where(&models.Ledger{BaseModel: frame.BaseModel{ID: id}}).First(&ledger).Error; err != nil {
		if frame.ErrorIsNoRows(err) {
			return nil, apperrors.ErrLedgerNotFound
		}
		return nil, apperrors.ErrSystemFailure.Override(err)
	}

	return &ledger, nil
}

// Query constants for ledger repository.
const constLedgerQuery = `SELECT id, parent_id, data FROM ledgers`

func (l *ledgerRepository) searchLedgers(ctx context.Context, sqlQuery *SearchSQLQuery) ([]*models.Ledger, error) {
	rows, err := l.service.DB(ctx, true).
		Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
		Raw(fmt.Sprintf(`%s WHERE %s`, constLedgerQuery, sqlQuery.sql), sqlQuery.args...).Rows()
	if err != nil {
		return nil, err
	}

	defer rows.Close()

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

func (l *ledgerRepository) Search(ctx context.Context, query string) (frame.JobResultPipe[[]*models.Ledger], error) {
	service := l.service
	job := frame.NewJob(func(ctxI context.Context, jobResult frame.JobResultPipe[[]*models.Ledger]) error {
		rawQuery, err := NewSearchRawQuery(ctxI, query)
		if err != nil {
			return jobResult.WriteError(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()

		for sqlQuery.canLoad() {
			ledgerList, dbErr := l.searchLedgers(ctxI, sqlQuery)
			if dbErr != nil {
				if frame.ErrorIsNoRows(dbErr) {
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

	err := frame.SubmitJob(ctx, service, job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// Update persists an existing ledger in the database if it exists and only updates the data component.
func (l *ledgerRepository) Update(
	ctx context.Context,
	lg *models.Ledger,
) (*models.Ledger, apperrors.ApplicationLedgerError) {
	existingLedger, errLedger := l.GetByID(ctx, lg.ID)
	if errLedger != nil {
		return nil, errLedger
	}

	for key, value := range lg.Data {
		if value != "" && value != existingLedger.Data[key] {
			existingLedger.Data[key] = value
		}
	}

	err := l.service.DB(ctx, false).Save(&existingLedger).Error
	if err != nil {
		l.service.Log(ctx).WithError(err).Error("could not save the ledger")
		return nil, apperrors.ErrSystemFailure.Override(err)
	}
	return existingLedger, nil
}

// Create creates a new ledger in the database if it doesn't exist or only updates the data component.
func (l *ledgerRepository) Create(
	ctx context.Context,
	lg *models.Ledger,
) (*models.Ledger, apperrors.ApplicationLedgerError) {
	if lg.ParentID != "" {
		pLg, err := l.GetByID(ctx, lg.ParentID)
		if err != nil {
			return nil, apperrors.ErrSystemFailure.Override(err)
		}
		lg.ParentID = pLg.ID
	}

	err := l.service.DB(ctx, false).Save(lg).Error
	if err != nil {
		l.service.Log(ctx).WithError(err).Error("could not save the ledger")
		return nil, apperrors.ErrSystemFailure.Override(err)
	}

	return lg, nil
}
