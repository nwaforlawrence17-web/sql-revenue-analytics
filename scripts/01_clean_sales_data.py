import argparse
import difflib
import re
import warnings

import pandas as pd


EXPECTED_COLUMNS = [
    "Order_ID",
    "Date",
    "Product",
    "Region",
    "Quantity",
    "Price",
    "Total",
]

REGIONS = {"North", "South", "East", "West"}
PRODUCTS = ["Rice", "Beans", "Garri", "Yam", "Oil"]


def _to_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def standardize_region(value):
    text = _to_str(value).strip()
    if not text:
        return pd.NA
    text = text.title()
    return text if text in REGIONS else pd.NA


def standardize_product(value):
    raw = _to_str(value).strip()
    if not raw:
        return pd.NA

    lowered = raw.lower()
    compact = re.sub(r"[^a-z0-9]+", "", lowered)

    if any(token in lowered for token in ["palm", "veg", "vegetable", "oil"]) or "oil" in compact or "oyl" in compact:
        return "Oil"
    if "rice" in compact or compact in {"rce", "rcie"} or "rice50kg" in compact:
        return "Rice"
    if "bean" in compact or compact in {"bens", "beens"}:
        return "Beans"
    if "gar" in compact:
        return "Garri"
    if "yam" in compact:
        return "Yam"

    match = difflib.get_close_matches(compact, [p.lower() for p in PRODUCTS], n=1, cutoff=0.75)
    if match:
        return match[0].title()
    return pd.NA


_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?")


def parse_price(value):
    text = _to_str(value).strip()
    if not text:
        return pd.NA

    lowered = text.lower()
    if lowered in {"n/a", "na", "null", "unknown", "free", "ten"}:
        return pd.NA

    cleaned = text.replace(",", "").replace("₦", "")
    match = _NUM_RE.search(cleaned)
    if not match:
        return pd.NA
    try:
        return float(match.group(0))
    except ValueError:
        return pd.NA


def parse_quantity(value):
    text = _to_str(value).strip()
    if not text:
        return pd.NA

    lowered = text.lower()
    if lowered in {"n/a", "na", "null", "unknown", "two", "three"}:
        return pd.NA

    cleaned = text.replace(",", "")
    match = _NUM_RE.search(cleaned)
    if not match:
        return pd.NA
    try:
        number = float(match.group(0))
    except ValueError:
        return pd.NA

    if pd.isna(number) or number <= 0:
        return pd.NA
    return int(round(number))


def parse_date(value):
    text = _to_str(value).strip()
    if not text:
        return pd.NaT
    # This dataset is expected to be around 2025–2026, but includes intentionally
    # messy 2-digit years like "25/11/13" or "25-06-09 00:00" (meaning 2025-06-09).
    expected_min = pd.Timestamp("2025-01-01")
    expected_max = pd.Timestamp("2026-12-31 23:59:59")

    candidates = []

    if re.match(r"^\d{2}-\d{2}-\d{2}(?:\s|$)", text):
        candidates.append(pd.to_datetime(text, format="%y-%m-%d %H:%M", errors="coerce"))
        candidates.append(pd.to_datetime(text, format="%y-%m-%d", errors="coerce"))

    if re.match(r"^\d{2}/\d{2}/\d{2}(?:\s|$)", text):
        for fmt in ("%y/%m/%d", "%m/%d/%y", "%d/%m/%y"):
            candidates.append(pd.to_datetime(text, format=fmt, errors="coerce"))

    if re.match(r"^\d{2}\.\d{2}\.\d{2}(?:\s|$)", text):
        for fmt in ("%y.%m.%d", "%d.%m.%y", "%m.%d.%y"):
            candidates.append(pd.to_datetime(text, format=fmt, errors="coerce"))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        candidates.append(pd.to_datetime(text, errors="coerce", dayfirst=False))
        candidates.append(pd.to_datetime(text, errors="coerce", dayfirst=True))

    for dt in candidates:
        if not pd.isna(dt) and expected_min <= dt <= expected_max:
            return dt
    for dt in candidates:
        if not pd.isna(dt):
            return dt
    return pd.NaT


def clean_sales_df(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")

    working = working[EXPECTED_COLUMNS].copy()

    working["Order_ID"] = working["Order_ID"].astype("string").str.strip()
    working = working[working["Order_ID"].notna() & (working["Order_ID"] != "")]

    working["Region"] = working["Region"].apply(standardize_region)
    working["Product"] = working["Product"].apply(standardize_product)
    working["Date"] = working["Date"].apply(parse_date)

    working["Quantity"] = working["Quantity"].apply(parse_quantity)
    working["Price"] = working["Price"].apply(parse_price)

    working = working.dropna(subset=["Date", "Product", "Region", "Quantity", "Price"])
    working = working[(working["Quantity"] > 0) & (working["Price"] > 0)]

    working["Total"] = (working["Quantity"].astype(float) * working["Price"].astype(float)).round(2)

    working = working.drop_duplicates(subset=["Order_ID"], keep="first")
    working = working.drop_duplicates()

    working["Date"] = pd.to_datetime(working["Date"]).dt.strftime("%Y-%m-%d")
    working["Quantity"] = working["Quantity"].astype(int)
    working["Price"] = working["Price"].astype(float).round(2)
    working["Total"] = working["Total"].astype(float).round(2)

    working = working.sort_values(["Date", "Order_ID"], kind="stable").reset_index(drop=True)
    return working


def clean_sales(input_csv: str, output_csv: str) -> pd.DataFrame:
    df = pd.read_csv(input_csv, dtype=str)
    cleaned = clean_sales_df(df)
    cleaned.to_csv(output_csv, index=False)

    return cleaned


def main():
    parser = argparse.ArgumentParser(description="Clean messy retail sales dataset (pandas).")
    parser.add_argument("--input", default="messy_retail_sales_500.csv", help="Input CSV path")
    parser.add_argument("--output", default="cleaned_sales.csv", help="Output CSV path")
    args = parser.parse_args()

    cleaned = clean_sales(args.input, args.output)
    print(f"Cleaned rows: {len(cleaned)}")
    print(f"Saved: {args.output}")
    print(cleaned.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
