package repository

import (
	"context"

	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/datastore"
)

func Migrate(ctx context.Context, svc *frame.Service, migrationPath string) error {
	pool := svc.DatastoreManager().GetPool(ctx, datastore.DefaultPoolName)
	return svc.DatastoreManager().Migrate(ctx, pool, migrationPath)
}
