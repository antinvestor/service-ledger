package utility

import (
	"github.com/shopspring/decimal"
	"google.golang.org/genproto/googleapis/type/money"
	"math/big"
)

func ToMoney(currency string, naive decimal.Decimal) money.Money {
	return money.Money{CurrencyCode: currency, Units: naive.IntPart(), Nanos: naive.Exponent()}
}

func FromMoney(m *money.Money) (naive decimal.Decimal) {
	return decimal.NewFromBigInt(new(big.Int).SetInt64(m.Units), m.Nanos)
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
