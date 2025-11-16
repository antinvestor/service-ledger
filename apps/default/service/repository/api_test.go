package repository_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"buf.build/gen/go/antinvestor/ledger/connectrpc/go/ledger/v1/ledgerv1connect"
	"connectrpc.com/connect"
	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	"github.com/antinvestor/service-ledger/internal/utility"
	"github.com/docker/docker/api/types/container"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/rs/xid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/genproto/googleapis/type/money"
)

type ConnectAPISuite struct {
	tests.BaseTestSuite
}

func (as *ConnectAPISuite) setupDependencies(
	t *testing.T,
	dep *definition.DependencyOption,
) (ledgerv1connect.LedgerServiceClient, testcontainers.Container) {
	ctx := t.Context()

	dbDep := dep.ByIsDatabase(ctx)
	if dbDep == nil {
		return nil, nil
	}

	datastoreDS := dbDep.GetInternalDS(ctx)

	_, err := as.setupServiceContainer(ctx, datastoreDS, true)
	require.NoError(t, err)

	lContainer, err := as.setupServiceContainer(ctx, datastoreDS, false)
	require.NoError(t, err)

	host, err := lContainer.Host(ctx)
	require.NoError(t, err)

	port, err := lContainer.MappedPort(ctx, "80")
	require.NoError(t, err)

	client := ledgerv1connect.NewLedgerServiceClient(
		http.DefaultClient,
		fmt.Sprintf("http://%s", net.JoinHostPort(host, port.Port())),
	)

	err = as.createInitialAccounts(ctx, client)
	require.NoError(t, err)

	return client, lContainer
}

func (as *ConnectAPISuite) setupServiceContainer(
	ctx context.Context,
	datastoreDS data.DSN,
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

func (as *ConnectAPISuite) createInitialAccounts(ctx context.Context, client ledgerv1connect.LedgerServiceClient) error {
	ledgers := []*ledgerv1.Ledger{
		{Id: "ilAsset", Type: ledgerv1.LedgerType_ASSET},
		{Id: "ilIncome", Type: ledgerv1.LedgerType_INCOME},
		{Id: "ilExpense", Type: ledgerv1.LedgerType_EXPENSE},
	}
	accounts := []*ledgerv1.Account{
		{Id: "ac1", Ledger: "ilAsset", Balance: toMoney(0)},
		{Id: "ac2", Ledger: "ilAsset", Balance: toMoney(0)},
		{Id: "ac3", Ledger: "ilAsset", Balance: toMoney(0)},
		{Id: "ac4", Ledger: "ilIncome", Balance: toMoney(0)},
		{Id: "ac5", Ledger: "ilExpense", Balance: toMoney(0)},
		{Id: "ac6", Ledger: "ilExpense", Balance: toMoney(0)},
		{Id: "ac7", Ledger: "ilExpense", Balance: toMoney(0)},
	}

	for _, req := range ledgers {
		_, err := client.CreateLedger(ctx, connect.NewRequest(&ledgerv1.CreateLedgerRequest{
			Id:   req.Id,
			Type: req.Type,
		}))
		if err != nil {
			return err
		}
	}

	for _, req := range accounts {
		_, err := client.CreateAccount(ctx, connect.NewRequest(&ledgerv1.CreateAccountRequest{
			Id:       req.Id,
			LedgerId: req.Ledger,
			Currency: "UGX", // Default currency
		}))
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

func (as *ConnectAPISuite) TestTransactions() {
	testcases := []struct {
		name      string
		request   *ledgerv1.Transaction
		balance   *money.Money
		reserve   *money.Money
		uncleared *money.Money
		wantErr   bool
	}{
		{
			name: "happy path",
			request: &ledgerv1.Transaction{
				Type:         ledgerv1.TransactionType_NORMAL,
				Cleared:      true,
				CurrencyCode: "UGX",
				Id:           xid.New().String(),
				Entries: []*ledgerv1.TransactionEntry{
					{AccountId: "ac1", Amount: toMoney(50), Credit: false},
					{AccountId: "ac2", Amount: toMoney(50), Credit: true},
				},
			},
			balance:   toMoney(50),
			reserve:   toMoney(0),
			uncleared: toMoney(0),
			wantErr:   false,
		},
		{
			name: "reserve transaction path",
			request: &ledgerv1.Transaction{
				Type:         ledgerv1.TransactionType_RESERVATION,
				Cleared:      true,
				CurrencyCode: "UGX",
				Id:           xid.New().String(),
				Entries: []*ledgerv1.TransactionEntry{
					{AccountId: "ac2", Amount: toMoney(20), Credit: false},
				},
			},
			balance:   toMoney(-50),
			reserve:   toMoney(20),
			uncleared: toMoney(0),
			wantErr:   false,
		},
		{
			name: "reduce reserve balance path",
			request: &ledgerv1.Transaction{
				Type:         ledgerv1.TransactionType_RESERVATION,
				Cleared:      true,
				CurrencyCode: "UGX",
				Id:           xid.New().String(),
				Entries: []*ledgerv1.TransactionEntry{
					{AccountId: "ac2", Amount: toMoney(-15), Credit: false},
				},
			},
			balance:   toMoney(-50),
			reserve:   toMoney(5),
			uncleared: toMoney(0),
			wantErr:   false,
		},
	}

	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx := t.Context()
		lc, lContainer := as.setupDependencies(t, dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			t.Run(tt.name, func(t *testing.T) {
				result, err := lc.CreateTransaction(ctx, connect.NewRequest(&ledgerv1.CreateTransactionRequest{
				Id:           tt.request.Id,
				Currency:     tt.request.CurrencyCode,
				TransactedAt: tt.request.TransactedAt,
				Data:         tt.request.Data,
				Entries:      tt.request.Entries,
				Cleared:      tt.request.Cleared,
				Type:         tt.request.Type,
			}))
				if err != nil {
					if !tt.wantErr {
						t.Errorf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}

				accRef := result.Msg.Data.Entries[0].AccountId
				searchReq := &commonv1.SearchRequest{
					Query: fmt.Sprintf(
						"{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}",
						accRef,
					),
				}
				accountsStream, err := lc.SearchAccounts(ctx, connect.NewRequest(searchReq))
				require.NoError(t, err)

				var acc *ledgerv1.SearchAccountsResponse
				for accountsStream.Receive() {
					acc = accountsStream.Msg()
					break // We only need the first result for this test
				}
				require.NotNil(t, acc, "No account received")
				require.NotEmpty(t, acc.Data, "No account data in response")

				accountData := acc.Data[0]
				assert.True(t, utility.CompareMoney(tt.balance, accountData.GetBalance()))
				assert.True(t, utility.CompareMoney(tt.reserve, accountData.GetReservedBalance()))
				assert.True(t, utility.CompareMoney(tt.uncleared, accountData.GetUnclearedBalance()))
			})
		}
	})
}

func (as *ConnectAPISuite) TestClearBalances() {
	updateID := xid.New().String()
	testcases := []struct {
		name          string
		request       *ledgerv1.Transaction
		balance       *money.Money
		reserve       *money.Money
		uncleared     *money.Money
		clearUpdate   bool
		wantErr       bool
		clearBalances bool
	}{
		{
			name: "happy path",
			request: &ledgerv1.Transaction{
				Type:         ledgerv1.TransactionType_NORMAL,
				Cleared:      true,
				CurrencyCode: "UGX",
				Id:           xid.New().String(),
				Entries: []*ledgerv1.TransactionEntry{
					{AccountId: "ac3", Amount: toMoney(50), Credit: false},
					{AccountId: "ac4", Amount: toMoney(50), Credit: true},
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
			request: &ledgerv1.Transaction{
				Type:         ledgerv1.TransactionType_NORMAL,
				Cleared:      false,
				CurrencyCode: "UGX",
				Id:           updateID,
				Entries: []*ledgerv1.TransactionEntry{
					{AccountId: "ac3", Amount: toMoney(20), Credit: false},
					{AccountId: "ac4", Amount: toMoney(20), Credit: true},
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
			request: &ledgerv1.Transaction{
				Type:         ledgerv1.TransactionType_NORMAL,
				Cleared:      true,
				CurrencyCode: "UGX",
				Id:           updateID,
				Entries: []*ledgerv1.TransactionEntry{
					{AccountId: "ac3", Amount: toMoney(20), Credit: false},
					{AccountId: "ac4", Amount: toMoney(20), Credit: true},
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

	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx := t.Context()
		lc, lContainer := as.setupDependencies(t, dep)
		defer lContainer.Terminate(ctx)

		for _, tt := range testcases {
			t.Run(tt.name, func(t *testing.T) {
				result, err := lc.CreateTransaction(ctx, connect.NewRequest(&ledgerv1.CreateTransactionRequest{
				Id:           tt.request.Id,
				Currency:     tt.request.CurrencyCode,
				TransactedAt: tt.request.TransactedAt,
				Data:         tt.request.Data,
				Entries:      tt.request.Entries,
				Cleared:      tt.request.Cleared,
				Type:         tt.request.Type,
			}))
			if err != nil {
				if !tt.wantErr {
					t.Fatalf("Transaction processing error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			accRef := result.Msg.Data.Entries[0].AccountId

				accountsStream, err := lc.SearchAccounts(ctx, connect.NewRequest(&commonv1.SearchRequest{
				Query: fmt.Sprintf(
					"{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}",
					accRef,
				),
			}))
				require.NoError(t, err)

				var acc *ledgerv1.SearchAccountsResponse
				for accountsStream.Receive() {
					acc = accountsStream.Msg()
					break // We only need the first result for this test
				}
				require.NotNil(t, acc, "No account received")
				require.NotEmpty(t, acc.Data, "No account data in response")

				accountData := acc.Data[0]
				assert.True(t, utility.CompareMoney(tt.balance, accountData.GetBalance()))
				assert.True(t, utility.CompareMoney(tt.reserve, accountData.GetReservedBalance()))
				assert.True(t, utility.CompareMoney(tt.uncleared, accountData.GetUnclearedBalance()))
			})
		}
	})
}

