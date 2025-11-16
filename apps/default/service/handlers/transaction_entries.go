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
	return ledgerSrv.Transaction.SearchEntries(
		ctx,
		req.Msg,
		func(_ context.Context, batch []*ledgerv1.TransactionEntry) error {
			// Send response with transaction data
			return stream.Send(&ledgerv1.SearchTransactionEntriesResponse{
				Data: batch,
			})
		},
	)
}
