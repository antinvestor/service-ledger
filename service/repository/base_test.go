package repository_test

import (
	"context"
	"fmt"
	"github.com/antinvestor/service-ledger/config"
	"github.com/docker/docker/api/types/container"
	"github.com/pitabwire/frame"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcPostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"net"
	"time"
)

// StdoutLogConsumer is a LogConsumer that prints the log to stdout
type StdoutLogConsumer struct{}

// Accept prints the log to stdout
func (lc *StdoutLogConsumer) Accept(l testcontainers.Log) {
	fmt.Print(string(l.Content))
}

type BaseTestSuite struct {
	suite.Suite
	service     *frame.Service
	ctx         context.Context
	pgContainer *tcPostgres.PostgresContainer
	networks    []string
	postgresUri string
}

func (bs *BaseTestSuite) SetupSuite() {
	ctx := context.Background()

	postgresContainer, err := bs.setupPostgres(ctx)
	assert.NoError(bs.T(), err)

	bs.pgContainer = postgresContainer

	bs.networks, err = bs.pgContainer.Networks(ctx)
	assert.NoError(bs.T(), err)

	postgresqlIp, err := bs.pgContainer.ContainerIP(ctx)
	assert.NoError(bs.T(), err)

	bs.postgresUri = fmt.Sprintf("postgres://ant:secret@%s/service_ledger?sslmode=disable", net.JoinHostPort(postgresqlIp, "5432"))

	databaseUriStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	assert.NoError(bs.T(), err)

	err = bs.setupMigrations(ctx)
	assert.NoError(bs.T(), err)

	fmt.Println(" successfully configured migrations ")

	configLedger := config.LedgerConfig{
		ConfigurationDefault: frame.ConfigurationDefault{
			ServerPort:         "",
			DatabasePrimaryURL: databaseUriStr,
			DatabaseReplicaURL: databaseUriStr,
		},
	}

	bs.ctx, bs.service = frame.NewService("ledger tests",
		frame.Config(&configLedger),
		frame.Datastore(bs.ctx),
		frame.NoopDriver())
	err = bs.service.Run(bs.ctx, "")
	assert.NoError(bs.T(), err)
}

func (bs *BaseTestSuite) setupPostgres(ctx context.Context) (*tcPostgres.PostgresContainer, error) {

	postgresContainer, err := tcPostgres.Run(ctx,
		"postgres:16.3",
		tcPostgres.WithDatabase("service_ledger"),
		tcPostgres.WithUsername("ant"),
		tcPostgres.WithPassword("secret"),

		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return nil, err
	}

	return postgresContainer, nil
}

func (bs *BaseTestSuite) setupMigrations(ctx context.Context) error {

	g := StdoutLogConsumer{}

	cRequest := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context: "../../",
		},
		ConfigModifier: func(config *container.Config) {
			config.Env = []string{
				"LOG_LEVEL=debug",
				"DO_MIGRATION=true",
				fmt.Sprintf("DATABASE_URL=%s", bs.postgresUri),
			}
		},
		Networks:   bs.networks,
		WaitingFor: wait.ForExit().WithExitTimeout(10 * time.Second),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(2 * time.Second)},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}

	migrationC, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: cRequest,
			Started:          true,
		})
	if err != nil {
		return err
	}

	return migrationC.Terminate(ctx)
}

func (bs *BaseTestSuite) TearDownSuite() {

	t := bs.T()
	if bs.pgContainer != nil {
		if err := bs.pgContainer.Terminate(bs.ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}
