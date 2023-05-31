import glob
from tqdm import tqdm
import pandas as pd

files = glob.glob("data/*.csv")

df_csv_concat = pd.concat([pd.read_csv(file) for file in tqdm(files)])  # type: ignore
df_csv_concat.to_csv("data/statcast_all.csv")  # type: ignore

print("Done concatenating CSVs")
all_statcast = pd.read_csv("data/statcast_all.csv")  # type: ignore

all_statcast = all_statcast[all_statcast["events"].notna()]
all_statcast = all_statcast[  # type: ignore
    ~all_statcast["events"].isin(("caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home"))  # type: ignore
]  # type: ignore
all_statcast = all_statcast[all_statcast["game_type"] == "R"]
all_statcast.to_csv("data/statcast_all.csv")
