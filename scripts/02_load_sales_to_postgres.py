import argparse
import logging
import os
import sys
from decimal import Decimal, InvalidOperation
import warnings

import pandas as pd
from sqlalchemy import Column, Date, Integer, MetaData, Numeric, String, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


REQUIRED_COLUMNS = ["Order_ID", "Date", "Product", "Region", "Quantity", "Price", "Total"]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def get_engine(db_url: str) -> Engine:
    if not db_url:
        raise ValueError("Database URL is missing. Pass --db-url or set DATABASE_URL.")
    try:
        return create_engine(db_url, future=True, pool_pre_ping=True)
    except SQLAlchemyError as exc:
        raise RuntimeError("Failed to create SQLAlchemy engine.") from exc


def define_sales_table(metadata: MetaData) -> Table:
    return Table(
        "sales_data",
        metadata,
        Column("order_id", String(32), primary_key=True),
        Column("order_date", Date, nullable=False),
        Column("product", String(20), nullable=False),
        Column("region", String(10), nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("price", Numeric(12, 2), nullable=False),
        Column("total", Numeric(14, 2), nullable=False),
    )


def create_table(engine: Engine, table: Table) -> None:
    try:
        table.metadata.create_all(engine, tables=[table], checkfirst=True)
        logging.info("Ensured table exists: sales_data")
    except SQLAlchemyError as exc:
        raise RuntimeError("Failed to create table sales_data.") from exc


def read_csv(csv_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"CSV file not found: {csv_path}") from exc
    except pd.errors.EmptyDataError as exc:
        raise ValueError(f"CSV file is empty: {csv_path}") from exc
    except pd.errors.ParserError as exc:
        raise ValueError(f"CSV parsing failed for: {csv_path}") from exc

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"CSV is missing required columns: {missing_cols}")

    return df[REQUIRED_COLUMNS].copy()


def _to_decimal(value: float, column_name: str) -> Decimal:
    try:
        return Decimal(f"{value:.2f}")
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"Invalid numeric value in {column_name}: {value}") from exc


def _parse_mixed_date(value) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            parsed = pd.to_datetime(text, errors="coerce", dayfirst=False)
    return parsed


def prepare_records(df: pd.DataFrame) -> list[dict]:
    working = df.copy()

    working["Order_ID"] = working["Order_ID"].astype(str).str.strip()
    working["Product"] = working["Product"].astype(str).str.strip()
    working["Region"] = working["Region"].astype(str).str.strip()

    working["Date"] = working["Date"].apply(_parse_mixed_date)
    working["Quantity"] = pd.to_numeric(working["Quantity"], errors="coerce")
    working["Price"] = pd.to_numeric(working["Price"], errors="coerce")
    working["Total"] = pd.to_numeric(working["Total"], errors="coerce")

    valid_mask = (
        working["Order_ID"].ne("")
        & working["Date"].notna()
        & working["Product"].ne("")
        & working["Region"].ne("")
        & working["Quantity"].notna()
        & working["Price"].notna()
        & working["Total"].notna()
    )
    invalid_count = int((~valid_mask).sum())
    if invalid_count:
        logging.warning("Dropping %s invalid row(s) before insert.", invalid_count)
    working = working.loc[valid_mask].copy()

    working["Quantity"] = working["Quantity"].astype(int)
    working["Price"] = working["Price"].round(2)
    working["Total"] = working["Total"].round(2)

    records: list[dict] = []
    for row in working.itertuples(index=False):
        records.append(
            {
                "order_id": row.Order_ID,
                "order_date": row.Date.date(),
                "product": row.Product,
                "region": row.Region,
                "quantity": int(row.Quantity),
                "price": _to_decimal(row.Price, "Price"),
                "total": _to_decimal(row.Total, "Total"),
            }
        )

    if not records:
        raise ValueError("No valid records found to insert.")

    return records


def load_records(engine: Engine, table: Table, records: list[dict], replace: bool) -> int:
    try:
        with engine.begin() as conn:
            if replace:
                conn.execute(text("TRUNCATE TABLE sales_data"))
                logging.info("Truncated table sales_data before loading.")

            insert_stmt = pg_insert(table).values(records)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[table.c.order_id],
                set_={
                    "order_date": insert_stmt.excluded.order_date,
                    "product": insert_stmt.excluded.product,
                    "region": insert_stmt.excluded.region,
                    "quantity": insert_stmt.excluded.quantity,
                    "price": insert_stmt.excluded.price,
                    "total": insert_stmt.excluded.total,
                },
            )
            result = conn.execute(upsert_stmt)
            return result.rowcount if result.rowcount is not None else len(records)
    except SQLAlchemyError as exc:
        raise RuntimeError("Failed to insert data into sales_data.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create sales_data table and load cleaned_sales.csv into PostgreSQL."
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", ""),
        help="PostgreSQL SQLAlchemy URL, e.g. postgresql+psycopg2://user:pass@localhost:5432/dbname",
    )
    parser.add_argument(
        "--csv",
        default="cleaned_sales.csv",
        help="Path to cleaned sales CSV file (default: cleaned_sales.csv)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Truncate sales_data before loading CSV.",
    )
    return parser.parse_args()


def main() -> int:
    configure_logging()
    args = parse_args()

    try:
        engine = get_engine(args.db_url)
        metadata = MetaData()
        sales_table = define_sales_table(metadata)
        create_table(engine, sales_table)

        df = read_csv(args.csv)
        records = prepare_records(df)
        loaded_count = load_records(engine, sales_table, records, replace=args.replace)

        logging.info("Loaded %s row(s) into sales_data.", loaded_count)
        return 0
    except (ValueError, RuntimeError, FileNotFoundError) as exc:
        logging.error(str(exc))
        return 1
    except Exception as exc:
        logging.exception("Unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
