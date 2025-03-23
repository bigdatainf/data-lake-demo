# Example 3: Data Quality Check Implementation
# data_quality_check.py

def check_data_quality(df, checks):
    """
    Perform data quality checks on a DataFrame.

    Args:
        df: pandas DataFrame to check
        checks: list of check dictionaries with format:
               {"type": "check_type", "column": "column_name", "condition": condition_value}

    Returns:
        Dictionary with check results
    """
    results = {}

    for check in checks:
        check_type = check["type"]
        column = check.get("column")

        if check_type == "null_check" and column:
            # Check for null values
            null_count = df[column].isnull().sum()
            results[f"null_check_{column}"] = {
                "pass": null_count == 0,
                "null_count": int(null_count),
                "total_count": len(df)
            }

        elif check_type == "unique_check" and column:
            # Check for uniqueness
            unique_count = df[column].nunique()
            duplicate_count = len(df) - unique_count
            results[f"unique_check_{column}"] = {
                "pass": duplicate_count == 0,
                "unique_count": int(unique_count),
                "duplicate_count": int(duplicate_count)
            }

        elif check_type == "range_check" and column:
            # Check value range
            min_val = check.get("min")
            max_val = check.get("max")

            if min_val is not None:
                out_of_range = (df[column] < min_val).sum()
                results[f"min_range_check_{column}"] = {
                    "pass": out_of_range == 0,
                    "out_of_range_count": int(out_of_range),
                    "min_value": min_val
                }

            if max_val is not None:
                out_of_range = (df[column] > max_val).sum()
                results[f"max_range_check_{column}"] = {
                    "pass": out_of_range == 0,
                    "out_of_range_count": int(out_of_range),
                    "max_value": max_val
                }

        elif check_type == "pattern_check" and column:
            # Check for pattern match using regex
            pattern = check.get("pattern")
            if pattern:
                mismatches = (~df[column].str.match(pattern)).sum()
                results[f"pattern_check_{column}"] = {
                    "pass": mismatches == 0,
                    "mismatch_count": int(mismatches),
                    "pattern": pattern
                }

    # Overall pass/fail
    results["overall_pass"] = all(check["pass"] for check in results.values() if isinstance(check, dict))

    return results
