package handlers

import (
	"testing"

	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"connectrpc.com/connect"
	"github.com/pitabwire/frame/frametests/definition"
)

// LedgerHandlersTestSuite extends BaseTestSuite for handler tests
type LedgerHandlersTestSuite struct {
	tests.BaseTestSuite
}

func (s *LedgerHandlersTestSuite) TestCreateLedger() {
	s.WithTestDependencies(s.T(), func(t *testing.T, depOpt *definition.DependencyOption) {
		// Set up the service with proper database connection
		ctx, svc, resources := s.CreateService(t, depOpt)
		defer svc.Stop(ctx)
		
		// Create handler with injected business layer 
		ledgerServer := NewLedgerServer(
			resources.LedgerBusiness,
			resources.AccountBusiness,
			resources.TransactionBusiness,
		)

		// Test request with correct field names
		req := &connect.Request[ledgerv1.CreateLedgerRequest]{
			Msg: &ledgerv1.CreateLedgerRequest{
				Id:       "test-ledger",
				Type:     ledgerv1.LedgerType_ASSET,
				ParentId: "",
				Data:     nil,
			},
		}

		// Call the method
		resp, err := ledgerServer.CreateLedger(ctx, req)

		// Assertions
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "test-ledger", resp.Msg.Data.Id)
		assert.Equal(t, ledgerv1.LedgerType_ASSET, resp.Msg.Data.Type)
	})
}

func TestLedgerHandlersSuite(t *testing.T) {
	suite.Run(t, &LedgerHandlersTestSuite{})
}
