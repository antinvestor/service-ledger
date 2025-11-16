package business_test

import (
	"context"
	"testing"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/structpb"
)

type LedgerBusinessSuite struct {
	tests.BaseTestSuite
}

func TestLedgerBusinessSuite(t *testing.T) {
	suite.Run(t, new(LedgerBusinessSuite))
}

func (ls *LedgerBusinessSuite) TestCreateLedger() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ls.CreateService(t, dep)

		ledgerBusiness := resources.LedgerBusiness

		createLedgerReq := &ledgerv1.CreateLedgerRequest{
			Id:   "test-ledger",
			Type: ledgerv1.LedgerType_ASSET,
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"description": {Kind: &structpb.Value_StringValue{StringValue: "Test asset ledger"}},
				},
			},
		}

		ledger, err := ledgerBusiness.CreateLedger(ctx, createLedgerReq)
		require.NoError(t, err, "Error creating ledger through business layer")
		require.NotNil(t, ledger, "Ledger should be created")

		assert.Equal(t, "test-ledger", ledger.GetId(), "Invalid ledger ID")
		assert.Equal(t, ledgerv1.LedgerType_ASSET, ledger.GetType(), "Invalid ledger type")
		assert.Equal(t, "Test asset ledger", ledger.GetData().GetFields()["description"].GetStringValue())
	})
}

func (ls *LedgerBusinessSuite) TestCreateLedgerWithMissingId() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ls.CreateService(t, dep)

		ledgerBusiness := resources.LedgerBusiness

		createLedgerReq := &ledgerv1.CreateLedgerRequest{
			Type: ledgerv1.LedgerType_LIABILITY,
			// Missing ID
		}

		ledger, err := ledgerBusiness.CreateLedger(ctx, createLedgerReq)
		assert.Error(t, err, "Should fail with missing ledger ID")
		assert.Nil(t, ledger, "Ledger should not be created")
		assert.Contains(t, err.Error(), "reference is required", "Error should mention missing reference")
	})
}

func (ls *LedgerBusinessSuite) TestGetLedger() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ls.CreateService(t, dep)

		ledgerBusiness := resources.LedgerBusiness

		// First create a ledger
		createLedgerReq := &ledgerv1.CreateLedgerRequest{
			Id:   "get-test-ledger",
			Type: ledgerv1.LedgerType_EXPENSE,
		}

		createdLedger, err := ledgerBusiness.CreateLedger(ctx, createLedgerReq)
		require.NoError(t, err, "Error creating ledger")

		// Now retrieve it
		retrievedLedger, err := ledgerBusiness.GetLedger(ctx, "get-test-ledger")
		require.NoError(t, err, "Error retrieving ledger")

		assert.Equal(t, createdLedger.GetId(), retrievedLedger.GetId(), "Retrieved ledger should match created ledger")
		assert.Equal(t, createdLedger.GetType(), retrievedLedger.GetType(), "Type should match")
	})
}

func (ls *LedgerBusinessSuite) TestGetLedgerNotFound() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ls.CreateService(t, dep)

		ledgerBusiness := resources.LedgerBusiness

		ledger, err := ledgerBusiness.GetLedger(ctx, "non-existent-ledger")
		assert.Error(t, err, "Should fail with non-existent ledger")
		assert.Nil(t, ledger, "Ledger should be nil")
	})
}

func (ls *LedgerBusinessSuite) TestUpdateLedger() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ls.CreateService(t, dep)

		ledgerBusiness := resources.LedgerBusiness

		// Create a ledger first
		createLedgerReq := &ledgerv1.CreateLedgerRequest{
			Id:   "update-test-ledger",
			Type: ledgerv1.LedgerType_INCOME,
		}

		_, err := ledgerBusiness.CreateLedger(ctx, createLedgerReq)
		require.NoError(t, err, "Error creating ledger")

		// Update the ledger data
		updateLedgerReq := &ledgerv1.UpdateLedgerRequest{
			Id: "update-test-ledger",
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"name":        {Kind: &structpb.Value_StringValue{StringValue: "Updated Income Ledger"}},
					"description": {Kind: &structpb.Value_StringValue{StringValue: "Updated description"}},
				},
			},
		}

		updatedLedger, err := ledgerBusiness.UpdateLedger(ctx, updateLedgerReq)
		require.NoError(t, err, "Error updating ledger")
		require.NotNil(t, updatedLedger, "Updated ledger should not be nil")

		// Verify the update
		assert.Equal(t, "Updated Income Ledger", updatedLedger.GetData().GetFields()["name"].GetStringValue())
		assert.Equal(t, "Updated description", updatedLedger.GetData().GetFields()["description"].GetStringValue())
	})
}

func (ls *LedgerBusinessSuite) TestSearchLedgers() {
	ls.WithTestDependencies(ls.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := ls.CreateService(t, dep)

		ledgerBusiness := resources.LedgerBusiness

		// Create multiple ledgers
		ledgers := []struct {
			id   string
			typ  ledgerv1.LedgerType
			name string
		}{
			{"search-ledger-1", ledgerv1.LedgerType_ASSET, "Asset Ledger 1"},
			{"search-ledger-2", ledgerv1.LedgerType_LIABILITY, "Liability Ledger 1"},
			{"search-ledger-3", ledgerv1.LedgerType_CAPITAL, "Equity Ledger 1"},
		}

		for _, l := range ledgers {
			strct, err := structpb.NewStruct(map[string]any{
				"name": l.name,
			})
			require.NoError(t, err, "Error creating struct")
			createReq := &ledgerv1.CreateLedgerRequest{
				Id:   l.id,
				Type: l.typ,
				Data: strct,
			}
			_, err = ledgerBusiness.CreateLedger(ctx, createReq)
			require.NoError(t, err, "Error creating ledger %s", l.id)
		}

		// Search for ledgers by type
		searchReq := &commonv1.SearchRequest{
			Query: `{"query": {"must": {"fields": [{"type": {"eq": "ASSET"}}]}}}`,
		}

		var foundLedgers []*ledgerv1.Ledger
		err := ledgerBusiness.SearchLedgers(ctx, searchReq, func(ctx context.Context, batch []*ledgerv1.Ledger) error {
			foundLedgers = append(foundLedgers, batch...)
			return nil
		})

		require.NoError(t, err, "Error searching ledgers")
		assert.Len(t, foundLedgers, 1, "Should find 1 asset ledger")
	})
}
