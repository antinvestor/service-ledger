package handlers

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	"buf.build/gen/go/antinvestor/ledger/connectrpc/go/ledger/v1/ledgerv1connect"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"connectrpc.com/connect"
	"github.com/antinvestor/service-ledger/apps/default/service/business"
)

type LedgerServer struct {
	Ledger      business.LedgerBusiness
	Account     business.AccountBusiness
	Transaction business.TransactionBusiness
}

// NewLedgerServer creates a new LedgerServer with injected dependencies.
func NewLedgerServer(
	ledgerBusiness business.LedgerBusiness,
	accountBusiness business.AccountBusiness,
	transactionBusiness business.TransactionBusiness,
) ledgerv1connect.LedgerServiceHandler {
	return &LedgerServer{
		Ledger:      ledgerBusiness,
		Account:     accountBusiness,
		Transaction: transactionBusiness,
	}
}

// SearchLedgers finds ledgers in the chart of accounts.
// Supports filtering by type, parent, and custom properties.
func (ledgerSrv *LedgerServer) SearchLedgers(
	ctx context.Context,
	req *connect.Request[commonv1.SearchRequest],
	stream *connect.ServerStream[ledgerv1.SearchLedgersResponse],
) error {
	// Search ledgers using business layer
	result, err := ledgerSrv.Ledger.SearchLedgers(ctx, req.Msg)
	if err != nil {
		return err
	}

	for {
		res, ok := result.ReadResult(ctx)
		if !ok {
			return nil
		}

		if res.IsError() {
			return res.Error()
		}

		// Send response with ledger data
		response := &ledgerv1.SearchLedgersResponse{
			Data: res.Item(),
		}

		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

// CreateLedger creates a new ledger in the chart of accounts.
// Ledgers can be hierarchical with parent-child relationships.
func (ledgerSrv *LedgerServer) CreateLedger(
	ctx context.Context,
	req *connect.Request[ledgerv1.CreateLedgerRequest],
) (*connect.Response[ledgerv1.CreateLedgerResponse], error) {
	// Create the ledger using business layer
	createdLedger, err := ledgerSrv.Ledger.CreateLedger(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with created ledger
	response := &ledgerv1.CreateLedgerResponse{
		Data: createdLedger,
	}

	return connect.NewResponse(response), nil
}

// UpdateLedger updates an existing ledger's metadata.
// The ledger type and reference cannot be changed.
func (ledgerSrv *LedgerServer) UpdateLedger(
	ctx context.Context,
	req *connect.Request[ledgerv1.UpdateLedgerRequest],
) (*connect.Response[ledgerv1.UpdateLedgerResponse], error) {
	// Update the ledger using business layer
	updatedLedger, err := ledgerSrv.Ledger.UpdateLedger(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with updated ledger
	response := &ledgerv1.UpdateLedgerResponse{
		Data: updatedLedger,
	}

	return connect.NewResponse(response), nil
}
