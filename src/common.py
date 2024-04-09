from pathlib import Path

import pandas as pd


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # drop all columns that don't contain "distance" or "order"
    df = df.loc[:, df.columns.str.contains("distance|order|result|source")]

    # copy index (+ 1) to new column "rank"
    original_rank_col = "original_rank"
    df[original_rank_col] = df.index + 1

    # sort texts by each distance order (distance) column
    for col in df.columns:
        if col.endswith("_order"):
            result_col = col.replace("_order", "")
            distance_col = f"{result_col}_distance"
            helper_df = df[
                ["result", original_rank_col, col, distance_col]
            ].sort_values(by=col)
            df[result_col + "_result"] = helper_df["result"].values
            df[distance_col] = helper_df[distance_col].values
            df[col] = helper_df[col].values

    return df.reindex(sorted(df.columns), axis=1)


def create_clean_csv_from_result_file(result_file: Path) -> None:
    df = pd.read_parquet(result_file)
    df = clean_df(df)
    df.to_csv(result_file.parent / (result_file.stem + ".csv"), index=False)
