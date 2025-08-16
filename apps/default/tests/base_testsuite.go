package tests

import (
	"context"
	"testing"

	"github.com/antinvestor/service-ledger/apps/default/config"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/frametests"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/pitabwire/frame/frametests/deps/testpostgres"
	"github.com/pitabwire/util"
	"github.com/stretchr/testify/require"
)

const PostgresqlDBImage = "paradedb/paradedb:latest"

const (
	DefaultRandomStringLength = 8
)

type BaseTestSuite struct {
	frametests.FrameBaseTestSuite
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

func (bs *BaseTestSuite) CreateService(
	t *testing.T,
	depOpts *definition.DependancyOption,

	frameOpts ...frame.Option,

) (*frame.Service, context.Context) {
	ctx := t.Context()
	cfg, err := frame.ConfigFromEnv[config.LedgerConfig]()
	require.NoError(t, err)

	cfg.LogLevel = "debug"
	cfg.RunServiceSecurely = false
	cfg.ServerPort = ""

	if depOpts != nil {
		for _, res := range depOpts.Database(ctx) {
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
		frameOpts = append(frameOpts, frame.WithNoopDriver())
	}

	ctx, svc := frame.NewServiceWithContext(ctx, "ledger tests",
		frame.WithConfig(&cfg),
		frame.WithDatastore())

	svc.Init(ctx, frameOpts...)

	err = repository.Migrate(ctx, svc, "../../migrations/0001")
	require.NoError(t, err)

	err = svc.Run(ctx, "")
	require.NoError(t, err)

	return svc, ctx
}

func (bs *BaseTestSuite) TearDownSuite() {
	bs.FrameBaseTestSuite.TearDownSuite()
}

// WithTestDependancies Creates subtests with each known DependancyOption.
func (bs *BaseTestSuite) WithTestDependancies(
	t *testing.T,
	testFn func(t *testing.T, dep *definition.DependancyOption),
) {
	options := []*definition.DependancyOption{
		definition.NewDependancyOption("default", util.RandomString(DefaultRandomStringLength), bs.Resources()),
	}

	frametests.WithTestDependancies(t, options, testFn)
}
