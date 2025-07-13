package repository_test

import (
	"context"
	"fmt"
	"github.com/antinvestor/apis/go/common"
	commonv1 "github.com/antinvestor/apis/go/common/v1"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	"github.com/antinvestor/service-ledger/internal/utility"
	"github.com/docker/docker/api/types/container"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/tests/testdef"
	"github.com/rs/xid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/genproto/googleapis/type/money"
	"net"
	"strconv"
	"testing"
	"time"
)

type GrpcApiSuite struct {
	tests.BaseTestSuite
}

func (as *GrpcApiSuite) setupDependencies(t *testing.T, dep *testdef.DependancyOption, ) (context.Context, *ledgerV1.LedgerClient, testcontainers.Container) {
	ctx := t.Context()

	var datastoreDS frame.DataSource

	for _, res := range dep.Database() {
		if res.GetDS().IsDB() {
			datastoreDS = res.GetDS()
			break
		}
	}

	lContainer, err := as.setupLedgerService(ctx, datastoreDS)
	assert.NoError(t, err)

	host, err := lContainer.Host(ctx)
	assert.NoError(t, err)

	port, err := lContainer.MappedPort(ctx, "50051")
	assert.NoError(t, err)

	lc, err := ledgerV1.NewLedgerClient(ctx,
		common.WithEndpoint(net.JoinHostPort(host, strconv.Itoa(port.Int()))),
		common.WithoutAuthentication(),
	)
	assert.NoError(t, err)

	as.createInitialAccounts(ctx, lc)

	return ctx, lc, lContainer
}

func (as *GrpcApiSuite) setupLedgerService(ctx context.Context, datastoreDS frame.DataSource) (testcontainers.Container, error) {

	cRequest := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{Context: "../../../../", Dockerfile: "./apps/default/Dockerfile"},
		ConfigModifier: func(config *container.Config) {
			config.Env = []string{
				"OTEL_TRACES_EXPORTER=none",
				"LOG_LEVEL=debug",
				"RUN_SERVICE_SECURELY=false",
				"HTTP_PORT=80",
				"GRPC_PORT=50051",
				fmt.Sprintf("DATABASE_URL=%s", datastoreDS.String()),
			}
		},
		ExposedPorts: []string{"80", "50051"},
		Networks:     []string{as.Network.Name},
		WaitingFor:   wait.ForLog("Initiating server operations").WithStartupTimeout(5 * time.Second),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts: []testcontainers.LogProductionOption{
				testcontainers.WithLogProductionTimeout(2 * time.Second),
			},
		},
	}

	genericContainer, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: cRequest,
			Started:          true,
		})
	if err != nil {
		return nil, err
	}

	return genericContainer, nil
}

func (as *GrpcApiSuite) createInitialAccounts(ctx context.Context, lc *ledgerV1.LedgerClient) {

	ledgers := []*ledgerV1.Ledger{
		{Reference: "ilAsset", Type: ledgerV1.LedgerType_ASSET},
		{Reference: "ilIncome", Type: ledgerV1.LedgerType_INCOME},
		{Reference: "ilExpense", Type: ledgerV1.LedgerType_EXPENSE},
	}
	accounts := []*ledgerV1.Account{
		{Reference: "ac1", Ledger: "ilAsset", Balance: toMoney(0)},
		{Reference: "ac2", Ledger: "ilAsset", Balance: toMoney(0)},
		{Reference: "ac3", Ledger: "ilAsset", Balance: toMoney(0)},
		{Reference: "ac4", Ledger: "ilIncome", Balance: toMoney(0)},
		{Reference: "ac5", Ledger: "ilExpense", Balance: toMoney(0)},
		{Reference: "ac6", Ledger: "ilExpense", Balance: toMoney(0)},
		{Reference: "ac7", Ledger: "ilExpense", Balance: toMoney(0)},
	}

	for _, req := range ledgers {
		_, err := lc.Client.CreateLedger(ctx, req)
		if err != nil {
			as.T().Fatalf("failed to create ledger container: %s", err)
		}
	}

	for _, req := range accounts {
		_, err := lc.Client.CreateAccount(ctx, req)
		if err != nil {
			as.T().Fatalf("failed to create account container: %s", err)
		}
	}
}

func toMoney(val int) *money.Money {
	m := utility.ToMoney("UGX", decimal.NewFromInt(int64(val)))
	return &m
}

func (as *GrpcApiSuite) TestTransactions() {

	testcases := []struct {
		name      string
		request   *ledgerV1.Transaction
		balance   *money.Money
		reserve   *money.Money
		uncleared *money.Money
		wantErr   bool
	}{
		{
			name: "happy path",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   true,
				Currency:  "UGX",
				Reference: xid.New().String(),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac1", Amount: toMoney(50), Credit: false},
					{Account: "ac2", Amount: toMoney(50), Credit: true},
				},
			},
			balance:   toMoney(50),
			reserve:   toMoney(0),
			uncleared: toMoney(0),
			wantErr:   false,
		},
		{
			name: "reserve transaction path",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_RESERVATION,
				Cleared:   true,
				Currency:  "UGX",
				Reference: xid.New().String(),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac2", Amount: toMoney(20), Credit: false},
				},
			},
			balance:   toMoney(-50),
			reserve:   toMoney(20),
			uncleared: toMoney(0),
			wantErr:   false,
		},
		{
			name: "reduce reserve balance path",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_RESERVATION,
				Cleared:   true,
				Currency:  "UGX",
				Reference: xid.New().String(),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac2", Amount: toMoney(-15), Credit: false},
				},
			},
			balance:   toMoney(-50),
			reserve:   toMoney(5),
			uncleared: toMoney(0),
			wantErr:   false,
		},
	}

	as.WithTestDependancies(as.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		ctx, lc, lContainer := as.setupDependencies(as.T(), dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			as.Run(tt.name, func() {

				result, err := lc.Client.CreateTransaction(ctx, tt.request)
				if err != nil {
					if !tt.wantErr {
						as.T().Errorf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}

				accRef := result.GetEntries()[0].GetAccount()
				accounts, err := lc.Client.SearchAccounts(ctx, &commonv1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", accRef)})
				assert.NoError(as.T(), err)

				acc, err := accounts.Recv()
				assert.NoError(as.T(), err)

				assert.True(as.T(), utility.CompareMoney(tt.balance, acc.GetBalance()))
				assert.True(as.T(), utility.CompareMoney(tt.reserve, acc.GetReservedBalance()))
				assert.True(as.T(), utility.CompareMoney(tt.uncleared, acc.GetUnclearedBalance()))

			})
		}

	})
}

func (as *GrpcApiSuite) TestClearBalances() {

	updateId := xid.New().String()
	testcases := []struct {
		name        string
		request     *ledgerV1.Transaction
		balance     *money.Money
		reserve     *money.Money
		uncleared   *money.Money
		clearUpdate bool
		wantErr     bool
	}{
		{
			name: "happy path",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   true,
				Currency:  "UGX",
				Reference: xid.New().String(),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac3", Amount: toMoney(50), Credit: false},
					{Account: "ac4", Amount: toMoney(50), Credit: true},
				},
			},
			balance:     toMoney(50),
			reserve:     toMoney(0),
			uncleared:   toMoney(0),
			clearUpdate: false,
			wantErr:     false,
		},
		{
			name: "send uncleared entry",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   false,
				Currency:  "UGX",
				Reference: updateId,
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac3", Amount: toMoney(20), Credit: false},
					{Account: "ac4", Amount: toMoney(20), Credit: true},
				},
			},
			balance:     toMoney(50),
			reserve:     toMoney(0),
			uncleared:   toMoney(20),
			clearUpdate: false,
			wantErr:     false,
		},
		{
			name: "reduce reserve balance path",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   true,
				Currency:  "UGX",
				Reference: updateId,
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac3", Amount: toMoney(20), Credit: false},
					{Account: "ac4", Amount: toMoney(20), Credit: true},
				},
			},
			balance:     toMoney(70),
			reserve:     toMoney(0),
			uncleared:   toMoney(0),
			clearUpdate: true,
			wantErr:     false,
		},
	}

	as.WithTestDependancies(as.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		ctx, lc, lContainer := as.setupDependencies(as.T(), dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			as.Run(tt.name, func() {
				var accRef string
				if tt.clearUpdate {
					result, err := lc.Client.UpdateTransaction(ctx, tt.request)
					if err != nil {
						if !tt.wantErr {
							as.T().Fatalf("Update Transaction () error = %v, wantErr %v", err, tt.wantErr)
						}
						return
					}
					accRef = result.GetEntries()[0].GetAccount()
				} else {
					result, err := lc.Client.CreateTransaction(ctx, tt.request)
					if err != nil {
						if !tt.wantErr {
							as.T().Fatalf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
						}
						return
					}
					accRef = result.GetEntries()[0].GetAccount()
				}

				accounts, err := lc.Client.SearchAccounts(ctx, &commonv1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", accRef)})
				assert.NoError(as.T(), err)

				acc, err := accounts.Recv()
				assert.NoError(as.T(), err)

				assert.True(as.T(), utility.CompareMoney(tt.balance, acc.GetBalance()))
				assert.True(as.T(), utility.CompareMoney(tt.reserve, acc.GetReservedBalance()))
				assert.True(as.T(), utility.CompareMoney(tt.uncleared, acc.GetUnclearedBalance()))

			})
		}

	})
}

func (as *GrpcApiSuite) TestReverseTransaction() {

	updateId := xid.New().String()
	testcases := []struct {
		name         string
		request      *ledgerV1.Transaction
		balance      *money.Money
		balanceAfter *money.Money
		createTx     bool
		wantErr      bool
	}{
		{
			name: "normal reversal",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   true,
				Currency:  "UGX",
				Reference: updateId,
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac5", Amount: toMoney(13), Credit: false},
					{Account: "ac4", Amount: toMoney(13), Credit: true},
				},
			},

			balance:      toMoney(13),
			balanceAfter: toMoney(0),
			createTx:     true,
			wantErr:      false,
		},
		{
			name: "uncleared reversal",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   false,
				Currency:  "UGX",
				Reference: xid.New().String(),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac6", Amount: toMoney(26), Credit: false},
					{Account: "ac4", Amount: toMoney(26), Credit: true},
				},
			},
			balance:      toMoney(0),
			balanceAfter: toMoney(0),
			createTx:     true,
			wantErr:      false,
		},
		{
			name: "reservation reversal",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_RESERVATION,
				Cleared:   true,
				Currency:  "UGX",
				Reference: xid.New().String(),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac7", Amount: toMoney(51), Credit: false},
				},
			},
			balance:      toMoney(0),
			balanceAfter: toMoney(0),
			createTx:     true,
			wantErr:      true,
		},
		{
			name: "reversal reversal",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_REVERSAL,
				Cleared:   true,
				Currency:  "UGX",
				Reference: fmt.Sprintf("REVERSAL_%s", updateId),
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac5", Amount: toMoney(13), Credit: false},
					{Account: "ac4", Amount: toMoney(13), Credit: true},
				},
			},
			balance:      toMoney(0),
			balanceAfter: toMoney(0),
			createTx:     false,
			wantErr:      true,
		},
	}

	as.WithTestDependancies(as.T(), func(t *testing.T, dep *testdef.DependancyOption) {

		ctx, lc, lContainer := as.setupDependencies(as.T(), dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			as.Run(tt.name, func() {

				debitAccRef := tt.request.GetEntries()[0].GetAccount()
				activeTx := tt.request

				if tt.createTx {
					_, err := lc.Client.CreateTransaction(ctx, activeTx)
					if err != nil {
						as.T().Fatalf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}

					accounts, err := lc.Client.SearchAccounts(ctx, &commonv1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", debitAccRef)})
					assert.NoError(as.T(), err)

					acc, err := accounts.Recv()
					assert.NoError(as.T(), err)

					assert.True(as.T(), utility.CompareMoney(tt.balance, acc.GetBalance()), " amounts don't match %s %s", tt.balanceAfter, acc.GetBalance())
				}

				_, err := lc.Client.ReverseTransaction(ctx, activeTx)
				if err != nil {
					if !tt.wantErr {
						as.T().Fatalf("Reverse Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}

				accounts, err := lc.Client.SearchAccounts(ctx, &commonv1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", debitAccRef)})
				assert.NoError(as.T(), err)

				acc, err := accounts.Recv()
				assert.NoError(as.T(), err)

				assert.True(as.T(), utility.CompareMoney(tt.balanceAfter, acc.GetBalance()), " amounts don't match %s %s", tt.balanceAfter, acc.GetBalance())

			})
		}

	})
}

func TestGrpcApiSuite(t *testing.T) {
	suite.Run(t, new(GrpcApiSuite))
}
