package handlers

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"connectrpc.com/connect"
)

// SearchTransactions finds transactions matching specified criteria.
// Supports filtering by date range, account, currency, and status.
func (ledgerSrv *LedgerServer) SearchTransactions(
	ctx context.Context,
	req *connect.Request[commonv1.SearchRequest],
	stream *connect.ServerStream[ledgerv1.SearchTransactionsResponse],
) error {
	// Search transactions using business layer
	result, err := ledgerSrv.Transaction.SearchTransactions(ctx, req.Msg)
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

		// Send response with transaction data
		response := &ledgerv1.SearchTransactionsResponse{
			Data: res.Item(),
		}

		streamErr := stream.Send(response)
		if streamErr != nil {
			return streamErr
		}
	}
}

// CreateTransaction creates a new double-entry transaction.
// All entries must be balanced (sum of debits = sum of credits).
func (ledgerSrv *LedgerServer) CreateTransaction(
	ctx context.Context,
	req *connect.Request[ledgerv1.CreateTransactionRequest],
) (*connect.Response[ledgerv1.CreateTransactionResponse], error) {
	// Create the transaction using business layer
	createdTransaction, err := ledgerSrv.Transaction.CreateTransaction(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with created transaction
	response := &ledgerv1.CreateTransactionResponse{
		Data: createdTransaction,
	}

	return connect.NewResponse(response), nil
}

// ReverseTransaction reverses a transaction by creating offsetting entries.
// Creates a new REVERSAL transaction that negates the original.
func (ledgerSrv *LedgerServer) ReverseTransaction(
	ctx context.Context,
	req *connect.Request[ledgerv1.ReverseTransactionRequest],
) (*connect.Response[ledgerv1.ReverseTransactionResponse], error) {
	// Reverse the transaction using business layer
	reversedTransaction, err := ledgerSrv.Transaction.ReverseTransaction(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with reversed transaction
	response := &ledgerv1.ReverseTransactionResponse{
		Data: reversedTransaction,
	}

	return connect.NewResponse(response), nil
}

// UpdateTransaction updates a transaction's metadata.
// Entries and amounts cannot be changed after creation.
func (ledgerSrv *LedgerServer) UpdateTransaction(
	ctx context.Context,
	req *connect.Request[ledgerv1.UpdateTransactionRequest],
) (*connect.Response[ledgerv1.UpdateTransactionResponse], error) {
	// Update the transaction using business layer
	updatedTransaction, err := ledgerSrv.Transaction.UpdateTransaction(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with updated transaction
	response := &ledgerv1.UpdateTransactionResponse{
		Data: updatedTransaction,
	}

	return connect.NewResponse(response), nil
}
