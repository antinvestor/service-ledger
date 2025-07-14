package repository

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame"
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
func NewSearchEngine(service *frame.Service, namespace string) (*SearchEngine, apperrors.ApplicationLedgerError) {
	if namespace != SearchNamespaceAccounts &&
		namespace != SearchNamespaceTransactions &&
		namespace != SearchNamespaceLedgers &&
		namespace != SearchNamespaceTransactionEntries {
		return nil, apperrors.ErrorSearchNamespaceUnknown
	}

	return &SearchEngine{service: service, namespace: namespace}, nil
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
	sql    string
	args   []interface{}
	offset int
	limit  int

	batchSize int
}

func (sq *SearchSQLQuery) canLoad() bool {
	return sq.offset < sq.limit
}

func (sq *SearchSQLQuery) stop(loadedCount int) bool {
	sq.offset += loadedCount
	if sq.offset+sq.batchSize > sq.limit {
		sq.batchSize = sq.limit - sq.offset
	}

	return loadedCount < sq.batchSize
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
func NewSearchRawQuery(_ context.Context, q string) (*SearchRawQuery, apperrors.ApplicationLedgerError) {
	var rawQuery *SearchRawQuery
	err := json.Unmarshal([]byte(q), &rawQuery)
	if err != nil {
		return nil, apperrors.ErrorSearchQueryHasInvalidFormart
	}

	// TODO: extend Must Fields with tenant and partition id

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
			return nil, apperrors.ErrorSearchQueryHasInvalidKeys
		}
	}
	return rawQuery, nil
}

// ToQueryConditions converts a raw search query conditions
func (rawQuery *SearchRawQuery) ToQueryConditions() *SearchSQLQuery {
	var conditionSQL string
	var conditionArgs []interface{}

	// Process must queries
	var mustWhere []string
	mustClause := rawQuery.Query.MustClause
	fieldsWhere, fieldsArgs := convertFieldsToSQL(mustClause.Fields)
	mustWhere = append(mustWhere, fieldsWhere...)
	conditionArgs = append(conditionArgs, fieldsArgs...)

	termsWhere, termsArgs := convertTermsToSQL(mustClause.Terms)
	mustWhere = append(mustWhere, termsWhere...)
	conditionArgs = append(conditionArgs, termsArgs...)

	rangesWhere, rangesArgs := convertRangesToSQL(mustClause.RangeItems)
	mustWhere = append(mustWhere, rangesWhere...)
	conditionArgs = append(conditionArgs, rangesArgs...)

	// Process should queries
	var shouldWhere []string
	shouldClause := rawQuery.Query.ShouldClause
	fieldsWhere, fieldsArgs = convertFieldsToSQL(shouldClause.Fields)
	shouldWhere = append(shouldWhere, fieldsWhere...)
	conditionArgs = append(conditionArgs, fieldsArgs...)

	termsWhere, termsArgs = convertTermsToSQL(shouldClause.Terms)
	shouldWhere = append(shouldWhere, termsWhere...)
	conditionArgs = append(conditionArgs, termsArgs...)

	rangesWhere, rangesArgs = convertRangesToSQL(shouldClause.RangeItems)
	shouldWhere = append(shouldWhere, rangesWhere...)
	conditionArgs = append(conditionArgs, rangesArgs...)

	if len(mustWhere) == 0 && len(shouldWhere) == 0 {
		return &SearchSQLQuery{sql: conditionSQL, args: conditionArgs, offset: 0, limit: SystemBatchSize, batchSize: SystemBatchSize}
	}

	if len(mustWhere) != 0 {
		conditionSQL = conditionSQL + "(" + strings.Join(mustWhere, " AND ") + ")"
		if len(shouldWhere) != 0 {
			conditionSQL = conditionSQL + " AND "
		}
	}

	if len(shouldWhere) != 0 {
		conditionSQL = conditionSQL + "(" + strings.Join(shouldWhere, " OR ") + ")"
	}

	var offset = rawQuery.Offset
	var limit = rawQuery.Limit

	if offset <= 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}

	batchSize := limit
	if SystemBatchSize < limit {
		batchSize = SystemBatchSize
	}

	return &SearchSQLQuery{sql: conditionSQL, args: conditionArgs, offset: offset, limit: limit, batchSize: batchSize}
}
