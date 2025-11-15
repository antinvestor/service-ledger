package handlers

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	utility2 "github.com/antinvestor/service-ledger/internal/utility"
	"connectrpc.com/connect"
)

func transactionEntryToAPI(mEntry *models.TransactionEntry) *ledgerv1.TransactionEntry {
	entryAmount := utility2.ToMoney("", mEntry.Amount.Decimal)

	return &ledgerv1.TransactionEntry{
		Id:            mEntry.ID,
		AccountId:     mEntry.AccountID,
		TransactionId: mEntry.TransactionID,
		Amount:        &entryAmount,
		Credit:        mEntry.Credit,
	}
}

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

	// Convert model entries to API entries and stream them
	apiEntries := make([]*ledgerv1.TransactionEntry, len(result))
	for i, entry := range result {
		apiEntries[i] = transactionEntryToAPI(entry)
	}
	
	response := &ledgerv1.SearchTransactionEntriesResponse{
		Data: apiEntries,
	}
	
	if err := stream.Send(response); err != nil {
		return err
	}

	return nil
}
