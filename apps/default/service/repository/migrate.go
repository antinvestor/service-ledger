package repository

import (
	"context"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame"
)

func Migrate(ctx context.Context, svc *frame.Service, migrationPath string) error {
	return svc.MigrateDatastore(ctx, migrationPath,
		&models.Ledger{}, &models.Account{},
		&models.Transaction{}, &models.TransactionEntry{})
}
