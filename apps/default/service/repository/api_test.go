package repository_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/antinvestor/apis/go/common"
	commonv1 "github.com/antinvestor/apis/go/common/v1"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	"github.com/antinvestor/service-ledger/internal/utility"
	"github.com/docker/docker/api/types/container"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/rs/xid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/genproto/googleapis/type/money"
)

type GrpcAPISuite struct {
	tests.BaseTestSuite
}

func (as *GrpcAPISuite) setupDependencies(
	t *testing.T,
	dep *definition.DependancyOption,
) (*ledgerV1.LedgerClient, testcontainers.Container) {
	ctx := t.Context()

	if len(dep.Database(ctx)) == 0 {
		return nil, nil
	}

	datastoreDS := dep.Database(ctx)[0].GetInternalDS(ctx)

	_, err := as.setupServiceContainer(ctx, datastoreDS, true)
	require.NoError(t, err)

	lContainer, err := as.setupServiceContainer(ctx, datastoreDS, false)
	require.NoError(t, err)

	host, err := lContainer.Host(ctx)
	require.NoError(t, err)

	port, err := lContainer.MappedPort(ctx, "50051")
	require.NoError(t, err)

	lc, err := ledgerV1.NewLedgerClient(ctx,
		common.WithEndpoint(net.JoinHostPort(host, port.Port())),
		common.WithoutAuthentication(),
	)
	require.NoError(t, err)

	err = as.createInitialAccounts(ctx, lc)
	require.NoError(t, err)

	return lc, lContainer
}

func (as *GrpcAPISuite) setupServiceContainer(
	ctx context.Context,
	datastoreDS frame.DataSource,
	doMigration bool,
) (testcontainers.Container, error) {
	environmentVars := []string{
		"OTEL_TRACES_EXPORTER=none",
		"LOG_LEVEL=debug",
		"RUN_SERVICE_SECURELY=false",
		"HTTP_PORT=80",
		"GRPC_PORT=50051",
		fmt.Sprintf("DATABASE_URL=%s", datastoreDS.String()),
	}

	var waitingForStrategy wait.Strategy

	if doMigration {
		environmentVars = append(environmentVars, "DO_MIGRATION=true")
		waitingForStrategy = wait.ForExit()
	} else {
		waitingForStrategy = wait.ForLog("Initiating server operations").WithStartupTimeout(5 * time.Second)
	}

	cRequest := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{Context: "../../../../", Dockerfile: "./apps/default/Dockerfile"},
		ConfigModifier: func(config *container.Config) {
			config.Env = environmentVars
		},
		ExposedPorts: []string{"80", "50051"},
		Networks:     []string{as.Network.Name},
		WaitingFor:   waitingForStrategy,
	}

	genericContainer, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: cRequest,
			Started:          true,
		})
	if err != nil {
		return nil, err
	}

	if !doMigration {
		return genericContainer, nil
	}

	err = genericContainer.Terminate(ctx)
	if err != nil {
		return nil, err
	}
	return nil, errors.New("container not needed")
}

func (as *GrpcAPISuite) createInitialAccounts(ctx context.Context, lc *ledgerV1.LedgerClient) error {
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
		_, err := lc.Svc().CreateLedger(ctx, req)
		if err != nil {
			return err
		}
	}

	for _, req := range accounts {
		_, err := lc.Svc().CreateAccount(ctx, req)
		if err != nil {
			return err
		}
	}

	return nil
}

func toMoney(val int) *money.Money {
	m := utility.ToMoney("UGX", decimal.NewFromInt(int64(val)))
	return &m
}

func (as *GrpcAPISuite) TestTransactions() {
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

	as.WithTestDependancies(as.T(), func(t *testing.T, dep *definition.DependancyOption) {
		ctx := t.Context()
		lc, lContainer := as.setupDependencies(t, dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			t.Run(tt.name, func(t *testing.T) {
				result, err := lc.Svc().CreateTransaction(ctx, tt.request)
				if err != nil {
					if !tt.wantErr {
						t.Errorf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}

				accRef := result.GetEntries()[0].GetAccount()
				accounts, err := lc.Svc().SearchAccounts(
					ctx,
					&commonv1.SearchRequest{
						Query: fmt.Sprintf(
							"{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}",
							accRef,
						),
					},
				)
				require.NoError(t, err)

				acc, err := accounts.Recv()
				require.NoError(t, err)

				assert.True(t, utility.CompareMoney(tt.balance, acc.GetBalance()))
				assert.True(t, utility.CompareMoney(tt.reserve, acc.GetReservedBalance()))
				assert.True(t, utility.CompareMoney(tt.uncleared, acc.GetUnclearedBalance()))
			})
		}
	})
}

func (as *GrpcAPISuite) TestClearBalances() {
	updateID := xid.New().String()
	testcases := []struct {
		name          string
		request       *ledgerV1.Transaction
		balance       *money.Money
		reserve       *money.Money
		uncleared     *money.Money
		clearUpdate   bool
		wantErr       bool
		clearBalances bool
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
			balance:       toMoney(50),
			reserve:       toMoney(0),
			uncleared:     toMoney(0),
			clearUpdate:   false,
			wantErr:       false,
			clearBalances: false,
		},
		{
			name: "send uncleared entry",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   false,
				Currency:  "UGX",
				Reference: updateID,
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac3", Amount: toMoney(20), Credit: false},
					{Account: "ac4", Amount: toMoney(20), Credit: true},
				},
			},
			balance:       toMoney(50),
			reserve:       toMoney(0),
			uncleared:     toMoney(20),
			clearUpdate:   false,
			wantErr:       false,
			clearBalances: false,
		},
		{
			name: "reduce reserve balance path",
			request: &ledgerV1.Transaction{
				Type:      ledgerV1.TransactionType_NORMAL,
				Cleared:   true,
				Currency:  "UGX",
				Reference: updateID,
				Entries: []*ledgerV1.TransactionEntry{
					{Account: "ac3", Amount: toMoney(20), Credit: false},
					{Account: "ac4", Amount: toMoney(20), Credit: true},
				},
			},
			balance:       toMoney(70),
			reserve:       toMoney(0),
			uncleared:     toMoney(0),
			clearUpdate:   true,
			wantErr:       false,
			clearBalances: false,
		},
	}

	as.WithTestDependancies(as.T(), func(t *testing.T, dep *definition.DependancyOption) {
		ctx := t.Context()
		lc, lContainer := as.setupDependencies(t, dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			t.Run(tt.name, func(t *testing.T) {
				accRef, err := as.processTransaction(ctx, lc, tt)
				if err != nil {
					if !tt.wantErr {
						t.Fatalf("Transaction processing error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}

				accounts, err := lc.Svc().SearchAccounts(
					ctx,
					&commonv1.SearchRequest{
						Query: fmt.Sprintf(
							"{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}",
							accRef,
						),
					},
				)
				require.NoError(t, err)

				acc, err := accounts.Recv()
				require.NoError(t, err)

				assert.True(t, utility.CompareMoney(tt.balance, acc.GetBalance()))
				assert.True(t, utility.CompareMoney(tt.reserve, acc.GetReservedBalance()))
				assert.True(t, utility.CompareMoney(tt.uncleared, acc.GetUnclearedBalance()))
			})
		}
	})
}

func (as *GrpcAPISuite) processTransaction(ctx context.Context, lc *ledgerV1.LedgerClient, tt struct {
	name          string
	request       *ledgerV1.Transaction
	balance       *money.Money
	reserve       *money.Money
	uncleared     *money.Money
	clearUpdate   bool
	wantErr       bool
	clearBalances bool
}) (string, error) {
	if tt.clearUpdate {
		result, err := lc.Svc().UpdateTransaction(ctx, tt.request)
		if err != nil {
			return "", err
		}
		return result.GetEntries()[0].GetAccount(), nil
	}

	result, err := lc.Svc().CreateTransaction(ctx, tt.request)
	if err != nil {
		return "", err
	}
	return result.GetEntries()[0].GetAccount(), nil
}

func (as *GrpcAPISuite) TestReverseTransaction() {
	updateID := xid.New().String()
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
				Reference: updateID,
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
				Reference: fmt.Sprintf("REVERSAL_%s", updateID),
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

	as.WithTestDependancies(as.T(), func(t *testing.T, dep *definition.DependancyOption) {
		ctx := t.Context()
		lc, lContainer := as.setupDependencies(t, dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			t.Run(tt.name, func(t *testing.T) {
				debitAccRef := tt.request.GetEntries()[0].GetAccount()
				activeTx := tt.request

				if tt.createTx {
					_, err := lc.Svc().CreateTransaction(ctx, activeTx)
					if err != nil {
						t.Fatalf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}

					accounts, err := lc.Svc().SearchAccounts(
						ctx,
						&commonv1.SearchRequest{
							Query: fmt.Sprintf(
								"{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}",
								debitAccRef,
							),
						},
					)
					require.NoError(t, err)

					acc, err := accounts.Recv()
					require.NoError(t, err)

					assert.True(
						t,
						utility.CompareMoney(tt.balance, acc.GetBalance()),
						" amounts don't match %s %s",
						tt.balanceAfter,
						acc.GetBalance(),
					)
				}

				_, err := lc.Svc().ReverseTransaction(ctx, activeTx)
				if err != nil {
					if !tt.wantErr {
						t.Fatalf("Reverse Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}

				accounts, err := lc.Svc().SearchAccounts(
					ctx,
					&commonv1.SearchRequest{
						Query: fmt.Sprintf(
							"{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}",
							debitAccRef,
						),
					},
				)
				require.NoError(t, err)

				acc, err := accounts.Recv()
				require.NoError(t, err)

				assert.True(
					t,
					utility.CompareMoney(tt.balanceAfter, acc.GetBalance()),
					" amounts don't match %s %s",
					tt.balanceAfter,
					acc.GetBalance(),
				)
			})
		}
	})
}

func TestGrpcAPISuite(t *testing.T) {
	suite.Run(t, new(GrpcAPISuite))
}
