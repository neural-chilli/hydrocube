// src/aggregation/sql_gen.rs
//
// Parses user-supplied aggregation SQL and generates optimised hot-path queries.

use super::decompose;
use crate::error::{HcError, HcResult};

/// A single measure (aggregate expression) extracted from the user SQL.
#[derive(Debug, Clone)]
pub struct MeasureDef {
    /// The raw aggregate expression, e.g. `SUM(notional)`.
    pub expression: String,
    /// The column alias after `AS`, e.g. `total_notional`.
    pub alias: String,
    /// Whether this measure can be computed from partial slice results.
    pub decomposable: bool,
}

/// Parses a user-supplied GROUP BY query and generates optimised SQL variants.
#[derive(Debug)]
pub struct AggSqlGenerator {
    dimensions: Vec<String>,
    measures: Vec<MeasureDef>,
    original_sql: String,
}

impl AggSqlGenerator {
    /// Parse `sql` and build the generator.
    ///
    /// Expects a SQL query of the form:
    /// ```sql
    /// SELECT dim1, dim2, AGG(col) AS alias, ... FROM <table> GROUP BY dim1, dim2
    /// ```
    pub fn from_user_sql(sql: &str) -> HcResult<Self> {
        let normalised = normalise_whitespace(sql);

        let dimensions = parse_dimensions(&normalised)?;
        let measures = parse_measures(&normalised, &dimensions)?;

        Ok(Self {
            dimensions,
            measures,
            original_sql: sql.to_owned(),
        })
    }

    /// Dimension columns (from GROUP BY).
    pub fn dimensions(&self) -> &[String] {
        &self.dimensions
    }

    /// Measures (aggregate expressions) extracted from SELECT.
    pub fn measures(&self) -> &[MeasureDef] {
        &self.measures
    }

    /// True if any measure is non-decomposable.
    pub fn has_non_decomposable(&self) -> bool {
        self.measures.iter().any(|m| !m.decomposable)
    }

    /// Generates the hot-path slice-aggregation query.
    ///
    /// Reads from the `slices` table, filters by `_window_id > $cutoff`,
    /// and re-aggregates the same dimensions and measures.
    pub fn slice_aggregation_sql(&self) -> String {
        let dims = self.dimensions.join(", ");
        let measures: Vec<String> = self
            .measures
            .iter()
            .map(|m| format!("{} AS {}", m.expression, m.alias))
            .collect();

        let select_parts = if dims.is_empty() {
            measures.join(", ")
        } else if measures.is_empty() {
            dims.clone()
        } else {
            format!("{}, {}", dims, measures.join(", "))
        };

        if dims.is_empty() {
            format!(
                "SELECT {} FROM slices WHERE _window_id > $cutoff",
                select_parts
            )
        } else {
            format!(
                "SELECT {} FROM slices WHERE _window_id > $cutoff GROUP BY {}",
                select_parts, dims
            )
        }
    }

    /// Returns the original user SQL unchanged (used for full re-aggregation).
    pub fn full_aggregation_sql(&self) -> String {
        self.original_sql.clone()
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Collapse all runs of whitespace (including newlines) to a single space.
fn normalise_whitespace(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Extract dimension names from the GROUP BY clause.
fn parse_dimensions(sql: &str) -> HcResult<Vec<String>> {
    let upper = sql.to_uppercase();

    let group_by_pos = upper
        .find("GROUP BY")
        .ok_or_else(|| HcError::Aggregation("SQL must contain GROUP BY".into()))?;

    let after_group_by = &sql[group_by_pos + "GROUP BY".len()..];

    // Stop at ORDER BY, HAVING, LIMIT, or end of string
    let stop_keywords = ["ORDER BY", "HAVING", "LIMIT"];
    let mut end = after_group_by.len();
    let upper_after = after_group_by.to_uppercase();
    for kw in &stop_keywords {
        if let Some(pos) = upper_after.find(kw) {
            if pos < end {
                end = pos;
            }
        }
    }

    let group_by_clause = &after_group_by[..end];
    let dims: Vec<String> = group_by_clause
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();

    if dims.is_empty() {
        return Err(HcError::Aggregation("GROUP BY clause is empty".into()));
    }

    Ok(dims)
}

/// Extract measure definitions from the SELECT clause, skipping bare dimension names.
fn parse_measures(sql: &str, dimensions: &[String]) -> HcResult<Vec<MeasureDef>> {
    let upper = sql.to_uppercase();

    let select_pos = upper
        .find("SELECT")
        .ok_or_else(|| HcError::Aggregation("SQL must contain SELECT".into()))?;

    let from_pos = upper
        .find(" FROM ")
        .ok_or_else(|| HcError::Aggregation("SQL must contain FROM".into()))?;

    if from_pos <= select_pos {
        return Err(HcError::Aggregation("FROM must appear after SELECT".into()));
    }

    let select_clause = &sql[select_pos + "SELECT".len()..from_pos];
    let dim_set: std::collections::HashSet<String> =
        dimensions.iter().map(|d| d.to_uppercase()).collect();

    let parts = split_by_comma_respecting_parens(select_clause);
    let mut measures = Vec::new();

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Skip bare dimension names
        let part_upper = part.to_uppercase();
        if dim_set.contains(part_upper.trim()) {
            continue;
        }

        // Extract alias from "expr AS alias"
        let (expression, alias) = if let Some(alias) = extract_alias(part) {
            let expr_end = part.to_uppercase().rfind(" AS ").unwrap();
            (part[..expr_end].trim().to_owned(), alias)
        } else {
            // No alias — skip (we require aliases for measures)
            continue;
        };

        let decomposable = decompose::is_decomposable(&expression);
        measures.push(MeasureDef {
            expression,
            alias,
            decomposable,
        });
    }

    Ok(measures)
}

/// Split a comma-separated list of SQL expressions, respecting parentheses depth.
fn split_by_comma_respecting_parens(input: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth: usize = 0;

    for ch in input.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                parts.push(current.trim().to_owned());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }

    let tail = current.trim().to_owned();
    if !tail.is_empty() {
        parts.push(tail);
    }

    parts
}

/// Extract the alias from an expression of the form `expr AS alias`.
/// Returns `None` if no `AS` keyword is found.
fn extract_alias(expr: &str) -> Option<String> {
    let upper = expr.to_uppercase();
    // Find last occurrence to handle e.g. CASE WHEN ... AS x AS outer_alias
    if let Some(pos) = upper.rfind(" AS ") {
        let alias = expr[pos + 4..].trim().to_owned();
        if alias.is_empty() {
            None
        } else {
            Some(alias)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalise_whitespace() {
        let sql = "SELECT\n  book,\n  SUM(x) AS y\nFROM slices\nGROUP BY book";
        let n = normalise_whitespace(sql);
        assert_eq!(n, "SELECT book, SUM(x) AS y FROM slices GROUP BY book");
    }

    #[test]
    fn test_split_commas_respects_parens() {
        let parts = split_by_comma_respecting_parens("a, SUM(b, c), d");
        assert_eq!(parts, vec!["a", "SUM(b, c)", "d"]);
    }

    #[test]
    fn test_extract_alias_present() {
        assert_eq!(extract_alias("SUM(x) AS total"), Some("total".to_owned()));
    }

    #[test]
    fn test_extract_alias_absent() {
        assert_eq!(extract_alias("book"), None);
    }

    #[test]
    fn test_multiline_sql_parses() {
        let sql = "SELECT\n  book,\n  desk,\n  SUM(notional) AS total_notional\nFROM slices\nGROUP BY book, desk";
        let gen = AggSqlGenerator::from_user_sql(sql).unwrap();
        assert_eq!(gen.dimensions(), &["book", "desk"]);
        assert_eq!(gen.measures().len(), 1);
        assert_eq!(gen.measures()[0].alias, "total_notional");
        assert!(gen.measures()[0].decomposable);
    }
}
