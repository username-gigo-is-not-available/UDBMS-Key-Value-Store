import pandas as pd


def load_csv(path: str,
             index_col: str | None = None,
             to_dict: bool = False,
             n_rows: int | None = None) -> pd.DataFrame | dict | list[dict]:
    df = pd.read_csv(filepath_or_buffer=path, header=0, encoding='utf-8',
                     index_col=index_col, nrows=n_rows)
    df_is_unique = df.index.is_unique
    if to_dict and df_is_unique:
        # The index of the DataFrame is unique, so we can use it as the key of the dictionary
        # {index: {column: value}}
        return df.to_dict(orient='index')
    elif to_dict and not df_is_unique:
        # The index of the DataFrame is not unique, so we can't use it as the key of the dictionary
        # [{column: value}]
        return df.to_dict(orient='records')
    else:
        return df
