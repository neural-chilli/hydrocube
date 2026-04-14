// src/aggregation/decompose.rs
//
// Classifies aggregate expressions as decomposable (can combine partial results)
// or non-decomposable (must scan all raw data).

/// Returns true if the aggregate expression can be combined from partial window results.
///
/// Decomposable aggregates (SUM, COUNT, MIN, MAX, AVG, STDDEV, VARIANCE variants)
/// allow the hot-path to merge slice-level partials rather than re-scanning raw rows.
///
/// Non-decomposable aggregates (COUNT DISTINCT, MEDIAN, QUANTILE_*, etc.) require a
/// full scan of all contributing rows and therefore force a full-aggregation path.
pub fn is_decomposable(expr: &str) -> bool {
    let upper = expr.trim().to_uppercase();

    // Non-decomposable — check first so COUNT(DISTINCT ...) is caught before COUNT(
    let non_decomposable = [
        "COUNT(DISTINCT",
        "MEDIAN(",
        "QUANTILE_CONT(",
        "QUANTILE_DISC(",
        "PERCENTILE_CONT(",
        "PERCENTILE_DISC(",
        "STRING_AGG(",
        "LIST(",
        "ARRAY_AGG(",
        "MODE(",
    ];

    for prefix in &non_decomposable {
        if upper.contains(prefix) {
            return false;
        }
    }

    // Decomposable
    let decomposable = [
        "SUM(",
        "COUNT(",
        "MIN(",
        "MAX(",
        "AVG(",
        "STDDEV(",
        "VARIANCE(",
        "STDDEV_POP(",
        "STDDEV_SAMP(",
        "VAR_POP(",
        "VAR_SAMP(",
    ];

    for prefix in &decomposable {
        if upper.contains(prefix) {
            return true;
        }
    }

    // Unknown — safe default
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_is_decomposable() {
        assert!(is_decomposable("SUM(notional)"));
    }

    #[test]
    fn count_star_is_decomposable() {
        assert!(is_decomposable("COUNT(*)"));
    }

    #[test]
    fn count_distinct_is_not_decomposable() {
        assert!(!is_decomposable("COUNT(DISTINCT counterparty)"));
    }

    #[test]
    fn median_is_not_decomposable() {
        assert!(!is_decomposable("MEDIAN(price)"));
    }

    #[test]
    fn quantile_cont_is_not_decomposable() {
        assert!(!is_decomposable("QUANTILE_CONT(price, 0.95)"));
    }

    #[test]
    fn unknown_returns_false() {
        assert!(!is_decomposable("SOME_CUSTOM_AGG(x)"));
    }

    #[test]
    fn case_insensitive() {
        assert!(is_decomposable("sum(notional)"));
        assert!(!is_decomposable("count(distinct x)"));
    }
}
