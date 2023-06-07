import glob
from tqdm import tqdm
import dask.dataframe as dd

# from config import start_data_year, end_data_year

# for year in tqdm(range(start_data_year, end_data_year + 1), position=0, desc=" Years"):
# with open("data/statcast/statcast_all.csv", "w") as f:
#     f.write("")
files = glob.glob(f"downloads/statcast/*.csv")
dfs = []
for idx, file in enumerate(tqdm(files)):
    with open(file) as f:
        if f.read() == "\n":
            continue
    f = dd.read_csv(  # type: ignore
        file,
        dtype={
            "spin_axis": "float64",
            "zone": "float64",
            "release_spin_rate": "float64",
            "bb_type": "object",
            "on_1b": "Int64",
            "on_2b": "Int64",
            "on_3b": "Int64",
            "release_pos_y": "float64",
        },
    )

    dfs.append(f)  # type: ignore

df_csv_concat = dd.concat(dfs)  # type: ignore
dfs = []

df_csv_concat["game_year"] = df_csv_concat["game_year"].apply(int, meta=("game_year", "int64")).apply(str, meta=("game_year", "object"))  # type: ignore
df_csv_concat = df_csv_concat[df_csv_concat["events"].notnull()]  # type: ignore
df_csv_concat = df_csv_concat[  # type: ignore
    ~df_csv_concat["events"].isin(("caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home"))  # type: ignore
]
df_csv_concat = df_csv_concat[df_csv_concat["game_type"] == "R"]  # type: ignore
df_csv_concat.to_csv(f"data/statcast/statcast_all.csv", single_file=True, mode="w")  # type: ignore
