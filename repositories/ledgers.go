package repositories

import (
	"context"
	"fmt"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	"github.com/pitabwire/frame"
)

const constLedgerQuery = `SELECT 
    id, parent_id, data, type, created_at, modified_at, 
       version, tenant_id, partition_id, access_id, deleted_at FROM ledgers`

type LedgerRepository interface {
	GetByID(ctx context.Context, id string) (*models.Ledger, ledger.ApplicationLedgerError)
	Search(ctx context.Context, query string) ([]*models.Ledger, ledger.ApplicationLedgerError)
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

func (l *ledgerRepository) Search(ctx context.Context, query string) ([]*models.Ledger, ledger.ApplicationLedgerError) {

	rawQuery, aerr := NewSearchRawQuery(ctx, query)
	if aerr != nil {
		return nil, aerr
	}

	sqlQuery := rawQuery.ToQueryConditions()

	rows, err := l.service.DB(ctx, true).Raw(
		fmt.Sprintf(`%s %s`, constLedgerQuery, sqlQuery.sql), sqlQuery.args...).Rows()
	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, ledger.ErrorLedgerNotFound
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	defer rows.Close()

	var ledgerList []*models.Ledger
	for rows.Next() {
		l := models.Ledger{}
		if err := rows.Scan(&l.ID, &l.ParentID, &l.Data, &l.Type,
			&l.CreatedAt, &l.ModifiedAt, &l.Version, &l.TenantID, &l.PartitionID,
			&l.AccessID, &l.DeletedAt); err != nil {
			return nil, ledger.ErrorSystemFailure.Override(err)
		}

		ledgerList = append(ledgerList, &l)
	}

	return ledgerList, nil
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
