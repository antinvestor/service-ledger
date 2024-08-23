package repository

import (
	"context"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/utility"
	"github.com/pitabwire/frame"
)

type LedgerRepository interface {
	GetByID(ctx context.Context, id string) (*models.Ledger, utility.ApplicationLedgerError)
	Search(ctx context.Context, query string) (frame.JobResultPipe, error)
	Create(ctx context.Context, ledger *models.Ledger) (*models.Ledger, utility.ApplicationLedgerError)
	Update(ctx context.Context, ledger *models.Ledger) (*models.Ledger, utility.ApplicationLedgerError)
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
func (l *ledgerRepository) GetByID(ctx context.Context, id string) (*models.Ledger, utility.ApplicationLedgerError) {

	if id == "" {
		return nil, utility.ErrorUnspecifiedID
	}

	lg := new(models.Ledger)

	err := l.service.DB(ctx, true).First(lg, "id = ?", id).Error

	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, utility.ErrorLedgerNotFound
		}
		return nil, utility.ErrorSystemFailure.Override(err)
	}

	return lg, nil
}

func (l *ledgerRepository) Search(ctx context.Context, query string) (frame.JobResultPipe, error) {

	service := l.service
	job := service.NewJob(func(ctxI context.Context, jobResult frame.JobResultPipe) error {

		rawQuery, err := NewSearchRawQuery(ctxI, query)
		if err != nil {
			return jobResult.WriteResult(ctx, err)
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
				if frame.DBErrorIsRecordNotFound(errR) {
					return jobResult.WriteResult(ctx, utility.ErrorLedgerNotFound)
				}
				return jobResult.WriteResult(ctx, utility.ErrorSystemFailure.Override(errR))
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

	err := service.SubmitJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return job, nil

}

// Update persists an existing ledger in the database if it exists and only updates the data component
func (l *ledgerRepository) Update(ctx context.Context, lg *models.Ledger) (*models.Ledger, utility.ApplicationLedgerError) {

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
		l.service.L().WithError(err).Error("could not save the ledger")
		return nil, utility.ErrorSystemFailure.Override(err)
	}
	return existingLedger, nil

}

// Create creates a new ledger in the database if it doesn't exist or only updates the data component
func (l *ledgerRepository) Create(ctx context.Context, lg *models.Ledger) (*models.Ledger, utility.ApplicationLedgerError) {

	if lg.ParentID != "" {

		pLg, err := l.GetByID(ctx, lg.ParentID)
		if err != nil {
			return nil, utility.ErrorSystemFailure.Override(err)
		}
		lg.ParentID = pLg.ID
	}

	err := l.service.DB(ctx, false).Save(lg).Error
	if err != nil {
		l.service.L().WithError(err).Error("could not save the ledger")
		return nil, utility.ErrorSystemFailure.Override(err)
	}

	return lg, nil

}
