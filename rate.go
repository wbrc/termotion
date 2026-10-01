package termotion

import (
	"math"
	"strconv"
	"strings"
)

// maxRateDecimals bounds the decimal places for very small rates so neither this
// formatter nor its JavaScript mirror (where toFixed throws above 100) blows up.
const maxRateDecimals = 20

// Canonical Rate Display: mirrored by fmtRate in web/termotion.js — keep the two
// in step. The unit is producer-owned and reproduced verbatim; the magnitude is
// never rescaled.
func formatRate(v float64, unit string) string {
	return formatRateValue(v) + " " + unit
}

// formatRateValue renders v with three significant figures and trailing zeros
// trimmed, never in exponent notation.
func formatRateValue(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "0"
	}

	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	if v == 0 {
		return "0"
	}

	exp := int(math.Floor(math.Log10(v)))
	if exp >= 3 {
		scale := math.Pow(10, float64(exp-2))
		return sign + strconv.FormatFloat(math.Round(v/scale)*scale, 'f', 0, 64)
	}

	dec := min(2-exp, maxRateDecimals)
	s := strconv.FormatFloat(v, 'f', dec, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
	}
	return sign + s
}
