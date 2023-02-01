import pandas as pd
from dagster import asset

@asset
def load_file() -> pd.DataFrame:
    input_file = './data.csv'
    df = pd.read_csv(input_file)
    return df


@asset
def normalize_file(load_file: pd.DataFrame) -> pd.DataFrame:
    df = load_file
    df_column = df['url']
    l = list()
    for data in df_column:
        cur_index = data.find("://")
        cur_str = data[cur_index + 3:]
        cur_str_new = cur_str[:cur_str.find("/")]
        l.append(cur_str_new)
    df['domain_of_url'] = l
    return df


@asset
def save_file(normalize_file: pd.DataFrame) -> None:
    df = normalize_file
    df.to_csv('./output.csv', index=False)
