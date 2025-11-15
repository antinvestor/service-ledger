package repository

import (
	"encoding/json"
	"fmt"
	"strings"
)

func jsonify(input interface{}) string {
	j, _ := json.Marshal(input)
	return string(j)
}

func sqlComparisonOp(op string) string {
	switch op {
	case "gt":
		return ">"
	case "lt":
		return "<"
	case "gte":
		return ">="
	case "lte":
		return "<="
	case "ne":
		return "!="
	case "like":
		return "LIKE"
	case "notlike":
		return "NOT LIKE"
	case "is":
		return "IS"
	case "isnot":
		return "IS NOT"
	case "in":
		return "IN"
	case "notin":
		return "NOT IN"
	}
	return "="
}

func convertTermsToSQL(terms []map[string]interface{}) ([]string, []interface{}) {
	// Sample terms
	/*
	   "terms": [
	       {"status": "completed", "active": true},
	       {"charge": 2000},
	       {"colours": ["red", "green"]},
	       {"products":{"qw":{"coupons":["x001"]}}}
	   ]
	*/
	// Corresponding SQL
	/*
	   -- string value
	   SELECT id FROM transactions WHERE data->'status' @> '"completed"'::jsonb;
	   -- boolean value
	   SELECT id FROM transactions WHERE data->'active' @> 'true'::jsonb;
	   -- numeric value
	   SELECT id FROM transactions WHERE data->'charge' @> '2000'::jsonb;
	   -- array value
	   SELECT id FROM transactions WHERE data->'colors' @> '["red", "green"]'::jsonb;
	   -- object value
	   SELECT id FROM transactions WHERE data->'products' @> '{"qw":{"coupons": ["x001"]}}'::jsonb;
	*/

	// TODO incooporate ts_vector : db = db.Where(" search_properties @@ plainto_tsquery(?) ", query.Query)

	where := []string{}
	args := []interface{}{}
	for _, term := range terms {
		var conditions []string
		for key, value := range term {
			conditions = append(
				conditions,
				fmt.Sprintf("data->'%s' @> ?::jsonb", key),
			)
			args = append(args, jsonify(value))
		}
		where = append(where, "("+strings.Join(conditions, " AND ")+")")
	}
	return where, args
}

func convertRangesToSQL(ranges []map[string]map[string]interface{}) ([]string, []interface{}) {
	// Sample ranges
	/*
	   "ranges": [
	       {"charge": {"gte": 2000, "lte": 4000}},
	       {"date": {"gt": "2017-01-01","lt": "2017-06-31"}}
	   ]
	*/
	// Corresponding SQL
	/*
	   -- numeric value
	   SELECT id, data->'charge' FROM transactions WHERE (data->>'charge')::float >= 2000 AND (data->>'charge')::float <= 4000;
	   -- other values
	   SELECT id, data->'date' FROM transactions WHERE data->>'date' >= '2017-01-01' AND data->>'date' < '2017-06-31';
	*/
	where := []string{}
	args := []interface{}{}
	for _, rangeItem := range ranges {
		var conditions []string
		for key, comparison := range rangeItem {
			for op, value := range comparison {
				condn, arguments := getSQLConditionAndArgsFromRange(key, op, value)
				conditions = append(conditions, condn)
				for _, arg := range arguments {
					if arg != nil {
						args = append(args, arg)
					}
				}
			}
		}
		where = append(where, "("+strings.Join(conditions, " AND ")+")")
	}
	return where, args
}

func getSQLConditionAndArgsFromRange(key string, op string, value interface{}) (string, []interface{}) {
	getConditionAndArgs := func(key string, op string, val interface{}) (string, interface{}) {
		var condn string
		var arg interface{}
		switch val.(type) {
		case int, int8, int16, int32, int64, float32, float64:
			condn = fmt.Sprintf("(data->>'%s')::float %s ?", key, sqlComparisonOp(op))
			arg = val
		case nil:
			condn = fmt.Sprintf("data->>'%s' %s null", key, sqlComparisonOp(op))
			arg = nil
		default:
			condn = fmt.Sprintf("data->>'%s' %s ?", key, sqlComparisonOp(op))
			arg = val
		}
		return condn, arg
	}

	condition := ""
	args := []interface{}{}
	switch op {
	case "in", "nin":
		// Convert IN, NOT IN condition to OR of EQ and NE conditions
		var opnew string
		if op == "in" {
			opnew = "eq"
		} else {
			opnew = "ne"
		}
		values, _ := value.([]interface{})
		for i, val := range values {
			c, arg := getConditionAndArgs(key, opnew, val)
			args = append(args, arg)
			if i == 0 {
				condition = c
			} else {
				condition = condition + " OR " + c
			}
		}
	default:
		c, arg := getConditionAndArgs(key, op, value)
		condition = c
		args = append(args, arg)
	}
	return condition, args
}

func convertFieldsToSQL(fields []map[string]map[string]interface{}) ([]string, []interface{}) {
	// Sample ranges
	/*
	   "fields": [
	       {"reference": {"eq": "ACME.CREDIT"}, "balance": {"lt": 0}},
	   ]
	*/
	where := []string{}
	args := []interface{}{}
	for _, field := range fields {
		var conditions []string
		for key, comparison := range field {
			for op, value := range comparison {
				condn := fmt.Sprintf("%s %s ?", key, sqlComparisonOp(op))
				conditions = append(conditions, condn)
				args = append(args, value)
			}
		}
		where = append(where, "("+strings.Join(conditions, " AND ")+")")
	}
	return where, args
}
