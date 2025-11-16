package handlers

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"connectrpc.com/connect"
)

// SearchAccounts finds accounts matching specified criteria.
// Supports filtering by ledger, balance range, and custom properties.
func (ledgerSrv *LedgerServer) SearchAccounts(
	ctx context.Context,
	req *connect.Request[commonv1.SearchRequest],
	stream *connect.ServerStream[ledgerv1.SearchAccountsResponse],
) error {
	// Search accounts using business layer
	result, err := ledgerSrv.Account.SearchAccounts(ctx, req.Msg)
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

		// Send response with account data
		response := &ledgerv1.SearchAccountsResponse{
			Data: res.Item(),
		}

		err = stream.Send(response)
		if err != nil {
			return err
		}
	}
}

// CreateAccount creates a new account within a ledger.
// Each account tracks balances and transaction history.
func (ledgerSrv *LedgerServer) CreateAccount(
	ctx context.Context,
	req *connect.Request[ledgerv1.CreateAccountRequest],
) (*connect.Response[ledgerv1.CreateAccountResponse], error) {
	// Create the account using business layer
	createdAccount, err := ledgerSrv.Account.CreateAccount(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with created account
	response := &ledgerv1.CreateAccountResponse{
		Data: createdAccount,
	}

	return connect.NewResponse(response), nil
}

// UpdateAccount updates an existing account's metadata.
// Balances are updated through transactions, not directly.
func (ledgerSrv *LedgerServer) UpdateAccount(
	ctx context.Context,
	req *connect.Request[ledgerv1.UpdateAccountRequest],
) (*connect.Response[ledgerv1.UpdateAccountResponse], error) {
	// Update the account using business layer
	updatedAccount, err := ledgerSrv.Account.UpdateAccount(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	// Return response with updated account
	response := &ledgerv1.UpdateAccountResponse{
		Data: updatedAccount,
	}

	return connect.NewResponse(response), nil
}
