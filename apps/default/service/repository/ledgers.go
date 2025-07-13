package repository

import (
	"context"
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

// LedgerRepository provides all functions related to ledger Ledger
type ledgerRepository struct {
	service *frame.Service
}

// NewLedgerRepository provides instance of `LedgerRepository`
func NewLedgerRepository(service *frame.Service) LedgerRepository {
	return &ledgerRepository{service: service}
}

// GetByID returns an acccount with the given id
func (l *ledgerRepository) GetByID(ctx context.Context, id string) (*models.Ledger, apperrors.ApplicationLedgerError) {

	if id == "" {
		return nil, apperrors.ErrorUnspecifiedID
	}

	lg := new(models.Ledger)

	err := l.service.DB(ctx, true).First(lg, "id = ?", id).Error

	if err != nil {
		if frame.ErrorIsNoRows(err) {
			return nil, apperrors.ErrorLedgerNotFound
		}
		return nil, apperrors.ErrorSystemFailure.Override(err)
	}

	return lg, nil
}

func (l *ledgerRepository) Search(ctx context.Context, query string) (frame.JobResultPipe[[]*models.Ledger], error) {

	service := l.service
	job := frame.NewJob(func(ctxI context.Context, jobResult frame.JobResultPipe[[]*models.Ledger]) error {

		rawQuery, err := NewSearchRawQuery(ctxI, query)
		if err != nil {
			return jobResult.WriteError(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()
		var ledgerList []*models.Ledger

		conditions := append([]interface{}{sqlQuery.sql}, sqlQuery.args...)

		for sqlQuery.canLoad() {

			result := service.DB(ctxI, true).
				Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
				Find(&ledgerList, conditions...)
			errR := result.Error
			if errR != nil {
				if frame.ErrorIsNoRows(errR) {
					return jobResult.WriteError(ctx, apperrors.ErrorLedgerNotFound)
				}
				return jobResult.WriteError(ctx, apperrors.ErrorSystemFailure.Override(errR))
			}

			if result.RowsAffected == 0 {
				break // No more rows
			}

			errR = jobResult.WriteResult(ctx, ledgerList)
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

// Update persists an existing ledger in the database if it exists and only updates the data component
func (l *ledgerRepository) Update(ctx context.Context, lg *models.Ledger) (*models.Ledger, apperrors.ApplicationLedgerError) {

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
		return nil, apperrors.ErrorSystemFailure.Override(err)
	}
	return existingLedger, nil

}

// Create creates a new ledger in the database if it doesn't exist or only updates the data component
func (l *ledgerRepository) Create(ctx context.Context, lg *models.Ledger) (*models.Ledger, apperrors.ApplicationLedgerError) {

	if lg.ParentID != "" {

		pLg, err := l.GetByID(ctx, lg.ParentID)
		if err != nil {
			return nil, apperrors.ErrorSystemFailure.Override(err)
		}
		lg.ParentID = pLg.ID
	}

	err := l.service.DB(ctx, false).Save(lg).Error
	if err != nil {
		l.service.Log(ctx).WithError(err).Error("could not save the ledger")
		return nil, apperrors.ErrorSystemFailure.Override(err)
	}

	return lg, nil

}
