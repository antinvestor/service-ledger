package utility

import (
	"math"
	"time"

	"github.com/shopspring/decimal"
	"google.golang.org/genproto/googleapis/type/money"
)

// Precision constants.
const (
	// DecimalPrecision is the precision used for decimal calculations.
	DecimalPrecision = 9
	// NanoSize is the multiplier for converting decimal fractions to nano units.
	NanoSize = 1000000000
	// MaxNanosValue is the maximum value for nanos (10^9 - 1).
	MaxNanosValue = 999999999
)

// GetMaxDecimalValue returns the maximum decimal value supported.
func GetMaxDecimalValue() decimal.Decimal {
	return decimal.NewFromInt(math.MaxInt64).Add(decimal.New(MaxNanosValue, -9))
}

func ToMoney(currency string, amount decimal.Decimal) money.Money {
	amount = CleanDecimal(amount)

	// Split the decimal value into units and nanos
	units := amount.IntPart()
	nanos := amount.Sub(decimal.NewFromInt(units)).Mul(decimal.NewFromInt(NanoSize)).IntPart()

	// Ensure nanos value is within int32 range
	if nanos > math.MaxInt32 || nanos < math.MinInt32 {
		nanos %= (math.MaxInt32 + 1)
	}

	// Validate that nanos is now within int32 range to prevent overflow
	if nanos > math.MaxInt32 {
		nanos = math.MaxInt32
	} else if nanos < math.MinInt32 {
		nanos = math.MinInt32
	}

	// Safe to cast to int32 now as we've validated the range
	//nolint:gosec // G115: integer overflow conversion is safe after range validation
	return money.Money{CurrencyCode: currency, Units: units, Nanos: int32(nanos)}
}

func FromMoney(m *money.Money) decimal.Decimal {
	if m == nil {
		return decimal.Zero
	}
	units := decimal.NewFromInt(m.GetUnits())
	nanos := decimal.New(int64(m.GetNanos()), -9)
	return units.Add(nanos)
}

func CompareMoney(a, b *money.Money) bool {
	if a.GetCurrencyCode() != b.GetCurrencyCode() {
		return false
	}
	if a.GetUnits() != b.GetUnits() {
		return false
	}
	if a.GetNanos() != b.GetNanos() {
		return false
	}
	return true
}

func CleanDecimal(d decimal.Decimal) decimal.Decimal {
	truncatedStr := d.StringFixed(DecimalPrecision)

	// Convert the string back to a decimal
	rounded, _ := decimal.NewFromString(truncatedStr)

	// Check if the value fits within the range for NUMERIC(20,9)
	// max allowed value for NUMERIC(28,9)
	minValue := GetMaxDecimalValue().Neg() // min allowed value (negative of max)

	if rounded.GreaterThan(GetMaxDecimalValue()) {
		return GetMaxDecimalValue()
	} else if rounded.LessThan(minValue) {
		return minValue
	}

	return rounded
}

func IsValidTime(t *time.Time) bool {
	return t != nil && !t.IsZero()
}
