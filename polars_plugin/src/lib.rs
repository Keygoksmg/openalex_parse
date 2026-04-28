use pyo3_polars::derive::polars_expr;
use pyo3_polars::export::polars_core::prelude::*;

/// Reconstruct abstract text from an inverted index JSON string.
///
/// Input format: {"word": [pos1, pos2], ...}
/// Output: words joined in position order
fn reconstruct(input: &str) -> Option<String> {
    if input == "null" || input == "{}" {
        return None;
    }

    let parsed: serde_json::Value = serde_json::from_str(input).ok()?;
    let obj = parsed.as_object()?;

    if obj.is_empty() {
        return None;
    }

    // Find max position (O(n) scan)
    let mut max_pos: usize = 0;
    for positions in obj.values() {
        for pos in positions.as_array()? {
            let p = pos.as_u64()? as usize;
            if p > max_pos {
                max_pos = p;
            }
        }
    }

    if max_pos > 100_000 {
        return None;
    }

    // Pre-allocate and fill (O(n) direct indexing)
    let mut words: Vec<&str> = vec![""; max_pos + 1];
    for (word, positions) in obj {
        for pos in positions.as_array()? {
            let p = pos.as_u64()? as usize;
            words[p] = word.as_str();
        }
    }

    Some(words.join(" "))
}

#[polars_expr(output_type=String)]
fn reconstruct_abstract(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let out: StringChunked = ca
        .into_iter()
        .map(|opt_val| opt_val.and_then(reconstruct))
        .collect();
    Ok(out.with_name(ca.name().clone()).into_series())
}
