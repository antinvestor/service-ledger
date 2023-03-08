package repositories

import (
	"context"
	"encoding/json"
	"github.com/antinvestor/service-ledger/models"
	"github.com/pitabwire/frame"
	"regexp"
	"strconv"
	"strings"

	"github.com/antinvestor/service-ledger/ledger"
)

var (
	// SearchNamespaceLedgers holds search namespace of ledgers
	SearchNamespaceLedgers = "ledgers"
	// SearchNamespaceAccounts holds search namespace of accounts
	SearchNamespaceAccounts = "accounts"
	// SearchNamespaceTransactions holds search namespace of transactions
	SearchNamespaceTransactions = "transactions"
	// SearchNamespaceTransactionEntries holds search namespace of transaction entries
	SearchNamespaceTransactionEntries = "transaction_entries"
)

// SearchEngine is the interface for all search operations
type SearchEngine struct {
	service   *frame.Service
	namespace string
}

// NewSearchEngine returns a new instance of `SearchEngine`
func NewSearchEngine(service *frame.Service, namespace string) (*SearchEngine, ledger.ApplicationLedgerError) {
	if namespace != SearchNamespaceAccounts &&
		namespace != SearchNamespaceTransactions &&
		namespace != SearchNamespaceLedgers &&
		namespace != SearchNamespaceTransactionEntries {
		return nil, ledger.ErrorSearchNamespaceUnknown
	}

	return &SearchEngine{service: service, namespace: namespace}, nil
}

// Query returns the results of a searc query
func (engine *SearchEngine) Query(ctx context.Context, q string) (interface{}, ledger.ApplicationLedgerError) {
	rawQuery, aerr := NewSearchRawQuery(ctx, q)
	if aerr != nil {
		return nil, aerr
	}

	sqlQuery := rawQuery.ToSQLQuery(engine.namespace)
	rows, err := engine.service.DB(ctx, true).Raw(sqlQuery.sql, sqlQuery.args...).Rows()
	if err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}
	defer rows.Close()

	switch engine.namespace {
	case SearchNamespaceLedgers:
		ledgers := make([]*models.Ledger, 0)
		for rows.Next() {
			lg := &models.Ledger{}
			if err := rows.Scan(&lg.ID, &lg.Type, &lg.ParentID, &lg.Data); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}
			ledgers = append(ledgers, lg)
		}
		return ledgers, nil

	case SearchNamespaceAccounts:
		accounts := make([]*models.Account, 0)
		for rows.Next() {
			acc := &models.Account{}
			if err := rows.Scan(&acc.ID, &acc.LedgerID, &acc.Currency, &acc.Balance, &acc.Data); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}
			accounts = append(accounts, acc)
		}
		return accounts, nil

	case SearchNamespaceTransactions:
		transactions := make([]*models.Transaction, 0)
		for rows.Next() {
			txn := &models.Transaction{}
			var rawAccounts, rawamount string
			if err := rows.Scan(&txn.ID, &txn.Currency, &txn.TransactedAt, &txn.Data, &rawAccounts, &rawamount); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}

			var accounts []string
			var amount []*models.Int
			json.Unmarshal([]byte(rawAccounts), &accounts)
			json.Unmarshal([]byte(rawamount), &amount)
			var entries []models.TransactionEntry
			for i, acc := range accounts {
				l := models.TransactionEntry{
					AccountID: acc,
					Amount:    amount[i],
				}
				entries = append(entries, l)
			}
			txn.Entries = entries
			transactions = append(transactions, txn)
		}
		return transactions, nil

	case SearchNamespaceTransactionEntries:
		transactionEntries := make([]models.TransactionEntry, 0)
		for rows.Next() {
			txnEntry := models.TransactionEntry{}
			if err := rows.Scan(&txnEntry.ID, &txnEntry.AccountID,
				&txnEntry.AccountID, &txnEntry.TransactionID,
				&txnEntry.Amount, &txnEntry.Credit, &txnEntry.Balance,
				&txnEntry.Currency, &txnEntry.TransactedAt); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}

			transactionEntries = append(transactionEntries, txnEntry)
		}
		return transactionEntries, nil

	default:
		return nil, ledger.ErrorSearchNamespaceUnknown
	}
}

// QueryContainer represents the format of query subsection inside `must` or `should`
type QueryContainer struct {
	Fields     []map[string]map[string]interface{} `json:"fields"`
	Terms      []map[string]interface{}            `json:"terms"`
	RangeItems []map[string]map[string]interface{} `json:"ranges"`
}

// SearchRawQuery represents the format of search query
type SearchRawQuery struct {
	Offset int `json:"from,omitempty"`
	Limit  int `json:"size,omitempty"`
	Query  struct {
		MustClause   QueryContainer `json:"must"`
		ShouldClause QueryContainer `json:"should"`
	} `json:"query"`
}

// SearchSQLQuery hold information of search SQL query
type SearchSQLQuery struct {
	sql  string
	args []interface{}
}

func hasValidKeys(items interface{}) bool {
	var validKey = regexp.MustCompile(`^[a-z_A-Z.]+$`)
	switch t := items.(type) {
	case []map[string]interface{}:
		for _, item := range t {
			for key := range item {
				if !validKey.MatchString(key) {
					return false
				}
			}
		}
		return true
	case []map[string]map[string]interface{}:
		for _, item := range t {
			for key := range item {
				if !validKey.MatchString(key) {
					return false
				}
			}
		}
		return true
	default:
		return false
	}
}

// NewSearchRawQuery returns a new instance of `SearchRawQuery`
func NewSearchRawQuery(ctx context.Context, q string) (*SearchRawQuery, ledger.ApplicationLedgerError) {
	var rawQuery *SearchRawQuery
	err := json.Unmarshal([]byte(q), &rawQuery)
	if err != nil {
		return nil, ledger.ErrorSearchQueryHasInvalidFormart
	}

	checkList := []interface{}{
		rawQuery.Query.MustClause.Fields,
		rawQuery.Query.MustClause.Terms,
		rawQuery.Query.MustClause.RangeItems,
		rawQuery.Query.ShouldClause.Fields,
		rawQuery.Query.MustClause.Terms,
		rawQuery.Query.MustClause.RangeItems,
	}
	for _, item := range checkList {
		if !hasValidKeys(item) {
			return nil, ledger.ErrorSearchQueryHasInvalidKeys
		}
	}
	return rawQuery, nil
}

// ToSQLQuery converts a raw search query to SQL format of the same
func (rawQuery *SearchRawQuery) ToSQLQuery(namespace string) *SearchSQLQuery {
	var q string
	var args []interface{}

	switch namespace {
	case SearchNamespaceLedgers:
		q = "SELECT id, type, parent_id, data FROM ledgers"
		break
	case SearchNamespaceAccounts:
		q = "SELECT a.id, a.ledger_id, a.currency, cb.balance, a.data FROM accounts a LEFT JOIN  current_balances cb ON a.id=cb.account_id"
		break
	case SearchNamespaceTransactions:
		q = `SELECT t.id, t.currency, t.transacted_at, t.data,
					array_to_json(ARRAY(
						SELECT a.id FROM transaction_entries e LEFT JOIN accounts a ON a.id = e.account_id
							WHERE e.transaction_id=t.id
							ORDER BY e.account_id
					)) AS account_array,
					array_to_json(ARRAY(
						SELECT e.amount FROM transaction_entries e
							WHERE e.transaction_id=t.id
							ORDER BY e.account_id
					)) AS amount_array
			FROM transactions t`
		break
	case SearchNamespaceTransactionEntries:
		q = `SELECT 
				e.id, e.account_id, 
				e.transaction_id, e.amount, 
				e.credit, e.account_balance, t.currency, t.transacted_at  
			FROM transaction_entries e LEFT JOIN accounts a ON a.id=e.account_id LEFT JOIN transactions t ON e.transaction_id=t.id`
		break
	default:
		return nil
	}

	// Process must queries
	var mustWhere []string
	mustClause := rawQuery.Query.MustClause
	fieldsWhere, fieldsArgs := convertFieldsToSQL(mustClause.Fields)
	mustWhere = append(mustWhere, fieldsWhere...)
	args = append(args, fieldsArgs...)

	termsWhere, termsArgs := convertTermsToSQL(mustClause.Terms)
	mustWhere = append(mustWhere, termsWhere...)
	args = append(args, termsArgs...)

	rangesWhere, rangesArgs := convertRangesToSQL(mustClause.RangeItems)
	mustWhere = append(mustWhere, rangesWhere...)
	args = append(args, rangesArgs...)

	// Process should queries
	var shouldWhere []string
	shouldClause := rawQuery.Query.ShouldClause
	fieldsWhere, fieldsArgs = convertFieldsToSQL(shouldClause.Fields)
	shouldWhere = append(shouldWhere, fieldsWhere...)
	args = append(args, fieldsArgs...)

	termsWhere, termsArgs = convertTermsToSQL(shouldClause.Terms)
	shouldWhere = append(shouldWhere, termsWhere...)
	args = append(args, termsArgs...)

	rangesWhere, rangesArgs = convertRangesToSQL(shouldClause.RangeItems)
	shouldWhere = append(shouldWhere, rangesWhere...)
	args = append(args, rangesArgs...)

	var offset = rawQuery.Offset
	var limit = rawQuery.Limit

	if len(mustWhere) == 0 && len(shouldWhere) == 0 {
		return &SearchSQLQuery{sql: q, args: args}
	}

	q = q + " WHERE "
	if len(mustWhere) != 0 {
		q = q + "(" + strings.Join(mustWhere, " AND ") + ")"
		if len(shouldWhere) != 0 {
			q = q + " AND "
		}
	}

	if len(shouldWhere) != 0 {
		q = q + "(" + strings.Join(shouldWhere, " OR ") + ")"
	}

	if namespace == "transactions" {
		q = q + " ORDER BY transacted_at"
	}

	if offset > 0 {
		q = q + " OFFSET " + strconv.Itoa(offset) + " "
	}
	if limit > 0 {
		q = q + " LIMIT " + strconv.Itoa(limit)
	}

	return &SearchSQLQuery{sql: q, args: args}
}
