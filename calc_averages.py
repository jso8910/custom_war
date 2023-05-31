import tqdm
import pandas as pd
import argparse
import sys
import os
import json


def calc_average(plays: pd.DataFrame):
    totals: dict[str, int] = {
        "PA": 0,  # Plate appearance
        "AB": 0,  # At bat
        "1B": 0,  # Single
        "2B": 0,  # Double
        "3B": 0,  # Triple
        "HR": 0,  # Home run
        "UBB": 0,  # Unintentional walk
        "IBB": 0,  # Intentional walk
        "HBP": 0,  # Hit by pitch
        "SF": 0,  # Sacrifice fly
        "SH": 0,  # Sacrifice hit
        "K": 0,  # Strikeout
        "SB": 0,  # Stolen base
        "CS": 0,  # Caught stealing
        "PK": 0,  # Pickoff
        "WP": 0,  # Wild pitch
        "PB": 0,  # Passed ball
        "BK": 0,  # Balk
        "INT": 0,  # Interference
        "E": 0,  # Error
        "FC": 0,  # Fielder's choice
        "R": 0,  # Runs
    }

    # Correspondance of event_cd to totals
    fields: dict[int, str] = {
        3: "K",
        # 4: "SB",
        # 6: "CS",
        # 8: "PK",
        # 9: "WP",
        # 10: "PB",
        11: "BK",
        14: "UBB",
        15: "IBB",
        16: "HBP",
        17: "INT",
        18: "E",
        19: "FC",
        20: "1B",
        21: "2B",
        22: "3B",
        23: "HR",
    }

    baserunning_outcomes_not_pa: list[int] = [4, 5, 6, 7, 8, 9, 10, 11, 12]
    # baserunning_outcomes: list[int] = [4, 5, 6, 7, 8, 9, 10, 12]

    # If 39 or 40 (SH_FL or SF_FL, sacrifice hit flag, sacrifice fly flag) are set to "T" (yes it's a string), then it's not an AB
    for _, row in tqdm.tqdm(plays.iterrows(), total=plays.shape[0]):  # type: ignore
        if row["EVENT_CD"] not in baserunning_outcomes_not_pa and row["EVENT_CD"] != 13:
            totals["PA"] += 1
        if row["AB_FL"] == "T":
            totals["AB"] += 1
        if row["SH_FL"] == "T":
            totals["SH"] += 1
        if row["SF_FL"] == "T":
            totals["SF"] += 1
        totals["R"] += int(row["EVENT_RUNS_CT"])  # type: ignore

        if row["RUN1_SB_FL"] == "T":
            totals["SB"] += 1
        if row["RUN2_SB_FL"] == "T":
            totals["SB"] += 1
        if row["RUN3_SB_FL"] == "T":
            totals["SB"] += 1

        if row["RUN1_CS_FL"] == "T":
            totals["CS"] += 1
        if row["RUN2_CS_FL"] == "T":
            totals["CS"] += 1
        if row["RUN3_CS_FL"] == "T":
            totals["CS"] += 1

        if row["RUN1_PK_FL"] == "T":
            totals["PK"] += 1
        if row["RUN2_PK_FL"] == "T":
            totals["PK"] += 1
        if row["RUN3_PK_FL"] == "T":
            totals["PK"] += 1

        if row["WP_FL"] == "T":
            totals["WP"] += 1
        if row["PB_FL"] == "T":
            totals["PB"] += 1

        if row["EVENT_CD"] in fields:
            totals[fields[row["EVENT_CD"]]] += 1  # type: ignore
    per_600_pa: dict[str, float] = totals.copy()  # type: ignore
    scaling = 600 / totals["PA"]
    for field in per_600_pa:
        per_600_pa[field] = totals[field] * scaling

    return totals, per_600_pa


def main(start_year: int, end_year: int):
    if start_year > end_year:
        print("START_YEAR must be less than END_YEAR", file=sys.stderr)
        sys.exit(1)
    elif start_year < 1915 or end_year > 2022:
        print(
            "START_YEAR and END_YEAR must be between 1915 and 2022. If 2023 or a future year has been added to retrosheet, feel free to edit this file.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not os.path.isdir("chadwick_csv"):
        print(
            "The folder chadwick_csv doesn't exist. Have you run retrosheet_to_csv.sh?",
            file=sys.stderr,
        )
        sys.exit(1)
    files = sorted(os.listdir("chadwick_csv"))
    if not len(files):
        print(
            "The folder chadwick_csv doesn't have any folders. Have you run retrosheet_to_csv.sh?",
            file=sys.stderr,
        )
        sys.exit(1)

    files_filtered = []
    for file in files:
        if int(file[0:4]) < start_year or int(file[0:4]) > end_year:  # type: ignore
            continue
        else:
            files_filtered.append(file)  # type: ignore
    # plays = pd.DataFrame()  # type: ignore
    years = []

    for idx, file in enumerate(tqdm.tqdm(files_filtered)):  # type: ignore
        if int(file[0:4]) < start_year or int(file[0:4]) > end_year:  # type: ignore
            continue
        file = "chadwick_csv/" + file  # type: ignore
        with open(file, "r") as f:  # type: ignore
            reader = pd.read_csv(f)  # type: ignore
            years.append(reader)  # type: ignore
    plays = pd.concat(years)  # type: ignore
    totals, per_600_pa = calc_average(plays)
    with open("weights_averages/total_mlb_stats.json", "w") as f:
        json.dump(totals, f)

    with open("weights_averages/average_mlb_stats_per_600.json", "w") as f:
        json.dump(per_600_pa, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-year",
        "-s",
        help="Start year of data gathering (defaults to 2015 for the first year of statcast data (the lowest that I would use this for))",
        type=int,
        default=2015,
    )
    parser.add_argument(
        "--end-year",
        "-e",
        help="End year of data gathering (defaults to 2022, current retrosheet year as of coding)",
        type=int,
        default=2022,
    )

    args = parser.parse_args(sys.argv[1:])
    main(args.start_year, args.end_year)
