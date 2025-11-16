package repository

import (
	"context"

	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/pitabwire/frame/datastore"
)

func Migrate(ctx context.Context, dbManager datastore.Manager, migrationPath string) error {
	dbPool := dbManager.GetPool(ctx, datastore.DefaultMigrationPoolName)

	return dbManager.Migrate(ctx, dbPool, migrationPath,
		&models.Ledger{}, &models.Account{}, &models.Transaction{},
		&models.TransactionEntry{})
}
