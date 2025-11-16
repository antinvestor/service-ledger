package tests

import (
	"context"
	"testing"

	aconfig "github.com/antinvestor/service-ledger/apps/default/config"
	"github.com/antinvestor/service-ledger/apps/default/service/business"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/config"
	"github.com/pitabwire/frame/datastore"
	"github.com/pitabwire/frame/frametests"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/pitabwire/frame/frametests/deps/testpostgres"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/require"
)

const (
	DefaultRandomStringLength = 8
)

type ServiceResources struct {
	LedgerRepository      repository.LedgerRepository
	AccountRepository     repository.AccountRepository
	TransactionRepository repository.TransactionRepository
	LedgerBusiness        business.LedgerBusiness
	AccountBusiness       business.AccountBusiness
	TransactionBusiness   business.TransactionBusiness
}

type BaseTestSuite struct {
	frametests.FrameBaseTestSuite
	ctx       context.Context
	resources *ServiceResources
}

// ServiceResources returns the shared service dependencies for the test suite
func (bs *BaseTestSuite) ServiceResources() *ServiceResources {
	// Create resources once and cache them to avoid unnecessary reinstantiation
	if bs.resources == nil {
		ctx, _, resources := bs.CreateService(bs.T(), nil)
		bs.ctx = ctx
		bs.resources = resources
	}
	return bs.resources
}

// WithTestDependencies Creates subtests with each known DependancyOption.
func (bs *BaseTestSuite) WithTestDependencies(
	t *testing.T,
	testFn func(t *testing.T, dep *definition.DependencyOption),
) {
	// Use the original working pattern
	resources := bs.Resources()
	deps := make([]definition.DependancyConn, len(resources))
	for i, r := range resources {
		deps[i] = r
	}
	testFn(t, definition.NewDependancyOption("default", util.RandomString(DefaultRandomStringLength), deps))
}

func initResources(_ context.Context) []definition.TestResource {
	pg := testpostgres.NewWithOpts("service_ledger", definition.WithUserName("ant"), definition.WithPassword("s3cr3t"))
	resources := []definition.TestResource{pg}
	return resources
}

func (bs *BaseTestSuite) SetupSuite() {
	bs.InitResourceFunc = initResources
	bs.FrameBaseTestSuite.SetupSuite()
}

func (bs *BaseTestSuite) CreateService(t *testing.T, depOpts *definition.DependencyOption, frameOpts ...frame.Option, ) (context.Context, *frame.Service, *ServiceResources) {
	ctx := t.Context()
	cfg, err := config.FromEnv[aconfig.LedgerConfig]()
	require.NoError(t, err)

	cfg.LogLevel = "debug"
	cfg.DatabaseMigrate = true
	cfg.RunServiceSecurely = false
	cfg.ServerPort = ""

	if depOpts != nil {
		res := depOpts.ByIsDatabase(ctx)
		if res != nil {
			testDS, cleanup, err0 := res.GetRandomisedDS(ctx, depOpts.Prefix())
			require.NoError(t, err0)

			t.Cleanup(func() {
				cleanup(ctx)
			})

			cfg.DatabasePrimaryURL = []string{testDS.String()}
			cfg.DatabaseReplicaURL = []string{testDS.String()}
		}
	}

	if len(frameOpts) == 0 {
		frameOpts = append(frameOpts, frametests.WithNoopDriver())
	}

	ctx, svc := frame.NewServiceWithContext(ctx, frame.WithName("ledger tests"),
		frame.WithConfig(&cfg),
		frame.WithDatastore())

	svc.Init(ctx, frameOpts...)

	err = repository.Migrate(ctx, svc, "../../migrations/0001")
	require.NoError(t, err)

	dbPool := svc.DatastoreManager().GetPool(ctx, datastore.DefaultPoolName)
	workMan := svc.WorkManager()

	ledgerRepo := repository.NewLedgerRepository(ctx, dbPool, workMan)
	accountRepo := repository.NewAccountRepository(ctx, dbPool, workMan, ledgerRepo)
	transactionRepo := repository.NewTransactionRepository(ctx, dbPool, workMan, accountRepo)
	ledgerBusiness := business.NewLedgerBusiness(workMan, ledgerRepo)
	accountBusiness := business.NewAccountBusiness(workMan, accountRepo)
	transactionBusiness := business.NewTransactionBusiness(workMan, transactionRepo)

	resources := &ServiceResources{
		LedgerRepository:      ledgerRepo,
		AccountRepository:     accountRepo,
		TransactionRepository: transactionRepo,
		LedgerBusiness:        ledgerBusiness,
		AccountBusiness:       accountBusiness,
		TransactionBusiness:   transactionBusiness,
	}

	err = svc.Run(ctx, "")
	require.NoError(t, err)

	return ctx, svc, resources
}

func (bs *BaseTestSuite) TearDownSuite() {
	bs.FrameBaseTestSuite.TearDownSuite()
}
