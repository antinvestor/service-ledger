package repository_test

import (
	"context"
	"fmt"
	"github.com/antinvestor/apis/go/common"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/service/utility"
	"github.com/docker/docker/api/types/container"
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
	BaseTestSuite

	ledgerContainer testcontainers.Container
	lc              *ledgerV1.LedgerClient
}

func (as *GrpcApiSuite) SetupSuite() {
	as.BaseTestSuite.SetupSuite()

	err := as.setupLedgerService(as.ctx)
	assert.NoError(as.T(), err)

	host, err := as.ledgerContainer.Host(as.ctx)
	assert.NoError(as.T(), err)

	port, err := as.ledgerContainer.MappedPort(as.ctx, "50051")
	assert.NoError(as.T(), err)

	as.lc, err = ledgerV1.NewLedgerClient(as.ctx,
		common.WithEndpoint(net.JoinHostPort(host, strconv.Itoa(port.Int()))),
		common.WithoutAuthentication(),
	)
	assert.NoError(as.T(), err)

	as.createInitialAccounts()
}

func (as *GrpcApiSuite) setupLedgerService(ctx context.Context) error {

	g := StdoutLogConsumer{}

	cRequest := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{Context: "../../"},
		ConfigModifier: func(config *container.Config) {
			config.Env = []string{
				"LOG_LEVEL=debug",
				"SECURELY_RUN_SERVICE=false",
				"HTTP_PORT=80",
				"GRPC_PORT=50051",
				fmt.Sprintf("DATABASE_URL=%s", as.postgresUri),
			}
		},
		ExposedPorts: []string{"80", "50051"},
		Networks:     as.networks,
		WaitingFor:   wait.ForLog("Initiating server operations").WithStartupTimeout(5 * time.Second),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts: []testcontainers.LogProductionOption{
				testcontainers.WithLogProductionTimeout(2 * time.Second),
			},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}

	var err error
	as.ledgerContainer, err = testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: cRequest,
			Started:          true,
		})
	if err != nil {
		return err
	}

	return err
}

func (as *GrpcApiSuite) createInitialAccounts() {

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
		_, err := as.lc.Client.CreateLedger(as.ctx, req)
		if err != nil {
			as.T().Fatalf("failed to create ledger container: %s", err)
		}
	}

	for _, req := range accounts {
		_, err := as.lc.Client.CreateAccount(as.ctx, req)
		if err != nil {
			as.T().Fatalf("failed to create account container: %s", err)
		}
	}
}

func (as *GrpcApiSuite) TearDownSuite() {

	if as.ledgerContainer != nil {
		if err := as.ledgerContainer.Terminate(as.ctx); err != nil {
			as.T().Fatalf("failed to terminate ledger container: %s", err)
		}
	}
	as.BaseTestSuite.TearDownSuite()

}

func toMoney(val int) *money.Money {
	m := utility.ToMoney("UGX", decimal.NewFromInt(int64(val)))
	return &m
}

func (as *GrpcApiSuite) TestTransactions() {

	tests := []struct {
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

	for _, tt := range tests {
		as.Run(tt.name, func() {

			result, err := as.lc.Client.CreateTransaction(as.ctx, tt.request)
			if err != nil {
				if !tt.wantErr {
					as.T().Errorf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			accRef := result.GetEntries()[0].GetAccount()
			accounts, err := as.lc.Client.SearchAccounts(as.ctx, &ledgerV1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", accRef)})
			assert.NoError(as.T(), err)

			acc, err := accounts.Recv()
			assert.NoError(as.T(), err)

			assert.True(as.T(), utility.CompareMoney(tt.balance, acc.GetBalance()))
			assert.True(as.T(), utility.CompareMoney(tt.reserve, acc.GetReservedBalance()))
			assert.True(as.T(), utility.CompareMoney(tt.uncleared, acc.GetUnclearedBalance()))

		})
	}
}

func (as *GrpcApiSuite) TestClearBalances() {

	updateId := xid.New().String()
	tests := []struct {
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

	for _, tt := range tests {
		as.Run(tt.name, func() {
			var accRef string
			if tt.clearUpdate {
				result, err := as.lc.Client.UpdateTransaction(as.ctx, tt.request)
				if err != nil {
					if !tt.wantErr {
						as.T().Fatalf("Update Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}
				accRef = result.GetEntries()[0].GetAccount()
			} else {
				result, err := as.lc.Client.CreateTransaction(as.ctx, tt.request)
				if err != nil {
					if !tt.wantErr {
						as.T().Fatalf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
					}
					return
				}
				accRef = result.GetEntries()[0].GetAccount()
			}

			accounts, err := as.lc.Client.SearchAccounts(as.ctx, &ledgerV1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", accRef)})
			assert.NoError(as.T(), err)

			acc, err := accounts.Recv()
			assert.NoError(as.T(), err)

			assert.True(as.T(), utility.CompareMoney(tt.balance, acc.GetBalance()))
			assert.True(as.T(), utility.CompareMoney(tt.reserve, acc.GetReservedBalance()))
			assert.True(as.T(), utility.CompareMoney(tt.uncleared, acc.GetUnclearedBalance()))

		})
	}
}

func (as *GrpcApiSuite) TestReverseTransaction() {

	updateId := xid.New().String()
	tests := []struct {
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

	for _, tt := range tests {
		as.Run(tt.name, func() {

			debitAccRef := tt.request.GetEntries()[0].GetAccount()
			activeTx := tt.request

			if tt.createTx {
				_, err := as.lc.Client.CreateTransaction(as.ctx, activeTx)
				if err != nil {
					as.T().Fatalf("Create Transaction () error = %v, wantErr %v", err, tt.wantErr)
				}

				accounts, err := as.lc.Client.SearchAccounts(as.ctx, &ledgerV1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", debitAccRef)})
				assert.NoError(as.T(), err)

				acc, err := accounts.Recv()
				assert.NoError(as.T(), err)

				assert.True(as.T(), utility.CompareMoney(tt.balance, acc.GetBalance()), " amounts don't match %s %s", tt.balanceAfter, acc.GetBalance())
			}

			_, err := as.lc.Client.ReverseTransaction(as.ctx, activeTx)
			if err != nil {
				if !tt.wantErr {
					as.T().Fatalf("Reverse Transaction () error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			accounts, err := as.lc.Client.SearchAccounts(as.ctx, &ledgerV1.SearchRequest{Query: fmt.Sprintf("{\"query\": {\"must\": { \"fields\": [{\"id\": {\"eq\": \"%s\"}}]}}}", debitAccRef)})
			assert.NoError(as.T(), err)

			acc, err := accounts.Recv()
			assert.NoError(as.T(), err)

			assert.True(as.T(), utility.CompareMoney(tt.balanceAfter, acc.GetBalance()), " amounts don't match %s %s", tt.balanceAfter, acc.GetBalance())

		})
	}
}

func TestGrpcApiSuite(t *testing.T) {
	suite.Run(t, new(GrpcApiSuite))
}
