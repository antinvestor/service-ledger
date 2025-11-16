package handlers

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"connectrpc.com/connect"
)

// SearchTransactionEntries finds transaction entries matching specified criteria.
// Supports filtering by account, transaction, date range, and amount ranges.
func (ledgerSrv *LedgerServer) SearchTransactionEntries(
	ctx context.Context,
	req *connect.Request[commonv1.SearchRequest],
	stream *connect.ServerStream[ledgerv1.SearchTransactionEntriesResponse],
) error {
	// Search transaction entries using business layer
	result, err := ledgerSrv.Transaction.SearchEntries(ctx, req.Msg.GetQuery())
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
		streamErr := stream.Send(&ledgerv1.SearchTransactionEntriesResponse{
			Data: res.Item(),
		})
		if streamErr != nil {
			return streamErr
		}
	}

}
