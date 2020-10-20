package models

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/antinvestor/service-ledger/ledger"
)

var (
	// SearchNamespaceAccounts holds search namespace of accounts
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
	db        *sql.DB
	namespace string
}

// NewSearchEngine returns a new instance of `SearchEngine`
func NewSearchEngine(db *sql.DB, namespace string) (*SearchEngine, ledger.ApplicationLedgerError) {
	if namespace != SearchNamespaceAccounts &&
		namespace != SearchNamespaceTransactions &&
		namespace != SearchNamespaceLedgers &&
		namespace != SearchNamespaceTransactionEntries{
		return nil, ledger.ErrorSearchNamespaceUnknown
	}

	return &SearchEngine{db: db, namespace: namespace}, nil
}

// Query returns the results of a searc query
func (engine *SearchEngine) Query(q string) (interface{}, ledger.ApplicationLedgerError) {
	rawQuery, aerr := NewSearchRawQuery(q)
	if aerr != nil {
		return nil, aerr
	}

	sqlQuery := rawQuery.ToSQLQuery(engine.namespace)
	rows, err := engine.db.Query(sqlQuery.sql, sqlQuery.args...)
	if err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}
	defer rows.Close()

	switch engine.namespace {
	case SearchNamespaceLedgers:
		ledgers := make([]*Ledger, 0)
		for rows.Next() {
			lg := &Ledger{}
			if err := rows.Scan(&lg.ID, &lg.Reference, &lg.Type, &lg.Parent, &lg.Data); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}
			ledgers = append(ledgers, lg)
		}
		return ledgers, nil

	case SearchNamespaceAccounts:
		accounts := make([]*Account, 0)
		for rows.Next() {
			acc := &Account{}
			if err := rows.Scan(&acc.ID, &acc.Reference, &acc.Ledger, &acc.Currency, &acc.Balance, &acc.Data); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}
			accounts = append(accounts, acc)
		}
		return accounts, nil

	case SearchNamespaceTransactions:
		transactions := make([]*Transaction, 0)
		for rows.Next() {
			txn := &Transaction{}
			var rawAccounts, rawamount string
			if err := rows.Scan(&txn.ID, &txn.Reference, &txn.Currency, &txn.TransactedAt, &txn.Data, &rawAccounts, &rawamount); err != nil {
				return nil, ledger.ErrorSystemFailure.Override(err)
			}

			var accounts []string
			var amount []int64
			json.Unmarshal([]byte(rawAccounts), &accounts)
			json.Unmarshal([]byte(rawamount), &amount)
			var entries []*TransactionEntry
			for i, acc := range accounts {
				l := &TransactionEntry{
					Account: sql.NullString{String: acc, Valid: true},
					Amount: sql.NullInt64{Int64: amount[i], Valid: true},
				}
				entries = append(entries, l)
			}
			txn.Entries = entries
			transactions = append(transactions, txn)
		}
		return transactions, nil

	case SearchNamespaceTransactionEntries:
		transactionEntries := make([]*TransactionEntry, 0)
		for rows.Next() {
			txnEntry := &TransactionEntry{}
			if err := rows.Scan(&txnEntry.ID, &txnEntry.AccountID,
				&txnEntry.Account, &txnEntry.TransactionID, &txnEntry.Transaction,
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
	var validKey = regexp.MustCompile(`^[a-z_A-Z]+$`)
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
func NewSearchRawQuery(q string) (*SearchRawQuery, ledger.ApplicationLedgerError) {
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
		q = "SELECT ledger_id, reference, ledger_type, parent_ledger_id, data FROM ledgers"
		break
	case SearchNamespaceAccounts:
		q = "SELECT account_id, reference, ledger_id, currency, balance, data FROM accounts LEFT JOIN  current_balances USING(account_id)"
		break
	case SearchNamespaceTransactions:
		q = `SELECT transaction_id, reference, currency, transacted_at, data,
					array_to_json(ARRAY(
						SELECT accounts.reference FROM entries LEFT JOIN accounts USING(account_id)
							WHERE transaction_id=transactions.transaction_id
							ORDER BY entries.account_id
					)) AS account_array,
					array_to_json(ARRAY(
						SELECT entries.amount FROM entries
							WHERE transaction_id=transactions.transaction_id
							ORDER BY entries.account_id
					)) AS amount_array
			FROM transactions`
		break
	case SearchNamespaceTransactionEntries:
		q= `SELECT 
				entry_id, entries.account_id, accounts.reference, 
				entries.transaction_id, transactions.reference, amount, 
				credit, account_balance, transactions.currency, transactions.transacted_at  
			FROM entries LEFT JOIN accounts USING(account_id) LEFT JOIN transactions USING(transaction_id)`
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

	q = enumerateSQLPlacholder(q)
	return &SearchSQLQuery{sql: q, args: args}
}
