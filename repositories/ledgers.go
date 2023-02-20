package repositories

import (
	"context"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	"github.com/pitabwire/frame"
)

type LedgerRepository interface {
	GetByID(ctx context.Context, id string) (*models.Ledger, ledger.ApplicationLedgerError)
	GetByReference(ctx context.Context, reference string) (*models.Ledger, ledger.ApplicationLedgerError)
	Create(ctx context.Context, ledger *models.Ledger) (*models.Ledger, ledger.ApplicationLedgerError)
	Update(ctx context.Context, ledger *models.Ledger) (*models.Ledger, ledger.ApplicationLedgerError)
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
func (l *ledgerRepository) GetByID(ctx context.Context, id string) (*models.Ledger, ledger.ApplicationLedgerError) {

	if id == "" {
		return nil, ledger.ErrorUnspecifiedID
	}

	lg := new(models.Ledger)

	err := l.service.DB(ctx, true).First(lg, "id = ?", id).Error

	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, ledger.ErrorLedgerNotFound
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return lg, nil
}

// GetByReference returns an acccount with the given Reference
func (l *ledgerRepository) GetByReference(ctx context.Context, reference string) (*models.Ledger, ledger.ApplicationLedgerError) {

	if reference == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	lg := new(models.Ledger)

	err := l.service.DB(ctx, true).First(lg, "reference = ?", reference).Error

	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, ledger.ErrorLedgerNotFound
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return lg, nil
}

// Update persists an existing ledger in the database if it exists and only updates the data component
func (l *ledgerRepository) Update(ctx context.Context, lg *models.Ledger) (*models.Ledger, ledger.ApplicationLedgerError) {

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
		return nil, ledger.ErrorSystemFailure.Override(err)
	}
	return existingLedger, nil

}

// Create creates a new ledger in the database if it doesn't exist or only updates the data component
func (l *ledgerRepository) Create(ctx context.Context, lg *models.Ledger) (*models.Ledger, ledger.ApplicationLedgerError) {

	if lg.ParentID != "" {

		pLg, err := l.GetByID(ctx, lg.ParentID)
		if err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}
		lg.ParentID = pLg.ID
	}

	err := l.service.DB(ctx, false).Save(lg).Error
	if err != nil {
		l.service.L().WithError(err).Error("could not save the ledger")
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return lg, nil

}
