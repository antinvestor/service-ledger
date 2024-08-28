package utility

import (
	"github.com/shopspring/decimal"
	"google.golang.org/genproto/googleapis/type/money"
	"math"
	"time"
)

const NanoSize = 1000000000

var MaxDecimalValue = decimal.NewFromInt(math.MaxInt64).Add(decimal.New(999999999, -9))

func ToMoney(currency string, amount decimal.Decimal) money.Money {

	amount = CleanDecimal(amount)

	// Split the decimal value into units and nanos
	units := amount.IntPart()
	nanos := amount.Sub(decimal.NewFromInt(units)).Mul(decimal.NewFromInt(NanoSize)).IntPart()

	return money.Money{CurrencyCode: currency, Units: units, Nanos: int32(nanos)}
}

func FromMoney(m *money.Money) (naive decimal.Decimal) {
	units := decimal.NewFromInt(m.Units)
	nanos := decimal.NewFromInt(int64(m.Nanos)).Div(decimal.NewFromInt(NanoSize))
	return units.Add(nanos)
}

func CompareMoney(a, b *money.Money) bool {
	if a.CurrencyCode != b.CurrencyCode {
		return false
	}
	if a.Units != b.Units {
		return false
	}
	if a.Nanos != b.Nanos {
		return false
	}
	return true
}

func CleanDecimal(d decimal.Decimal) decimal.Decimal {

	truncatedStr := d.StringFixed(9)

	// Convert the string back to a decimal
	rounded, _ := decimal.NewFromString(truncatedStr)

	// Check if the value fits within the range for NUMERIC(20,9)
	// max allowed value for NUMERIC(28,9)
	minValue := MaxDecimalValue.Neg() // min allowed value (negative of max)

	if rounded.GreaterThan(MaxDecimalValue) {
		return MaxDecimalValue
	} else if rounded.LessThan(minValue) {
		return minValue
	}

	return rounded
}

func IsValidTime(t *time.Time) bool {
	return t != nil && !t.IsZero()
}
