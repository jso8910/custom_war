import tqdm
import dask.dataframe as dd
import argparse
import sys
import os
import json
from config import end_data_year, start_data_year


def calc_woba_weights(plays):  # type: ignore
    run_exp_by_sit = [
        # ["Outs", "Runner state", "RUNS", "COUNT", "AVG"],
        [n % 3, n // 3, 0, 0, 0.0]
        for n in range(24)
    ]

    for _, play in tqdm.tqdm(plays.iterrows(), total=plays.shape[0]):  # type: ignore
        base_state = int(play["END_BASES_CD"])  # type: ignore
        outs = int(play["OUTS_CT"]) + int(play["EVENT_OUTS_CT"])  # type: ignore
        if outs >= 3:
            continue
        run_exp_by_sit[base_state * 3 + outs][2] += int(play["FATE_RUNS_CT"])  # type: ignore
        run_exp_by_sit[base_state * 3 + outs][3] += 1

    for idx in range(len(run_exp_by_sit)):
        run_exp_by_sit[idx][4] = run_exp_by_sit[idx][2] / run_exp_by_sit[idx][3]

    run_expectancy_total = {  # type: ignore
        "1B": 0.0,
        "2B": 0.0,
        "3B": 0.0,
        "HR": 0.0,
        "UBB": 0.0,
        "HBP": 0.0,
        "K": 0.0,
        "BIP": 0.0,
        "Out": 0.0,
    }

    run_expectancy_freq = {  # type: ignore
        "1B": 0,
        "2B": 0,
        "3B": 0,
        "HR": 0,
        "UBB": 0,
        "HBP": 0,
        "K": 0,
        "BIP": 0,
        "Out": 0,
    }

    run_expectancy_avg = {  # type: ignore
        "1B": 0.0,
        "2B": 0.0,
        "3B": 0.0,
        "HR": 0.0,
        "UBB": 0.0,
        "HBP": 0.0,
        "K": 0.0,
        "BIP": 0.0,  # BIP = Balls In Park (doesn't include HR or any of the other TTOs)
        "Out": 0.0,
    }

    event_code_to_event = {
        2: "Out",
        3: "K",
        14: "UBB",
        16: "HBP",
        20: "1B",
        21: "2B",
        22: "3B",
        23: "HR",
    }

    for _, play in tqdm.tqdm(plays.iterrows(), total=plays.shape[0]):  # type: ignore
        if int(play["EVENT_CD"]) not in event_code_to_event:  # type: ignore
            continue
        base_state = int(play["START_BASES_CD"])  # type: ignore
        end_base_state = int(play["END_BASES_CD"])  # type: ignore
        outs = int(play["OUTS_CT"])  # type: ignore
        end_outs = outs + int(play["EVENT_OUTS_CT"])  # type: ignore
        if end_outs >= 3:
            end_run_exp = 0.0
        else:
            # print(end_base_state, end_outs)
            end_run_exp = run_exp_by_sit[end_base_state * 3 + end_outs][4]

        start_run_exp = run_exp_by_sit[base_state * 3 + outs][4]
        run_expectancy_total[
            event_code_to_event[int(play["EVENT_CD"])]  # type: ignore
        ] += (
            end_run_exp + int(play["EVENT_RUNS_CT"])  # type: ignore
        ) - start_run_exp
        run_expectancy_freq[event_code_to_event[int(play["EVENT_CD"])]] += 1  # type: ignore
        if event_code_to_event[int(play["EVENT_CD"])] in ("1B", "2B", "3B", "Out"):  # type: ignore
            run_expectancy_total["BIP"] += (
                end_run_exp + int(play["EVENT_RUNS_CT"])  # type: ignore
            ) - start_run_exp
            run_expectancy_freq["BIP"] += 1

    for event in run_expectancy_total:
        run_expectancy_avg[event] = run_expectancy_total[event] / run_expectancy_freq[event]  # type: ignore

    for event in run_expectancy_total:
        run_expectancy_avg[event] -= run_expectancy_avg["Out"]

    # print(run_expectancy_avg)
    with open("weights_averages/average_mlb_stats_per_600.json", "r") as f:
        mlb_stats = json.load(f)

    obp_numerator: int = (
        mlb_stats["1B"]
        + mlb_stats["2B"]
        + mlb_stats["3B"]
        + mlb_stats["HR"]
        + mlb_stats["HBP"]
        + mlb_stats["IBB"]
        + mlb_stats["UBB"]
    )

    woba_numerator: float = (
        run_expectancy_avg["1B"] * mlb_stats["1B"]
        + run_expectancy_avg["2B"] * mlb_stats["2B"]
        + run_expectancy_avg["3B"] * mlb_stats["3B"]
        + run_expectancy_avg["HR"] * mlb_stats["HR"]
        + run_expectancy_avg["UBB"] * mlb_stats["UBB"]
        + run_expectancy_avg["HBP"] * mlb_stats["HBP"]
    )

    for event in run_expectancy_total:
        run_expectancy_avg[event] *= obp_numerator / woba_numerator

    return run_expectancy_avg


def main(start_year: int, end_year: int):
    if start_year > end_year:
        print("START_YEAR must be less than END_YEAR", file=sys.stderr)
        sys.exit(1)
    elif start_year < start_data_year or end_year > end_data_year:
        print(
            f"START_YEAR and END_YEAR must be between {start_data_year} and {end_data_year}. If {end_data_year + 1} or a future year has been added to retrosheet, feel free to edit this file.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not os.path.isdir("data/chadwick"):
        print(
            "The folder data/chadwick doesn't exist. Have you run retrosheet_to_csv.sh?",
            file=sys.stderr,
        )
        sys.exit(1)
    files = sorted(os.listdir("data/chadwick"))
    if not len(files):
        print(
            "The folder data/chadwick doesn't have any files. Have you run retrosheet_to_csv.sh?",
            file=sys.stderr,
        )
        sys.exit(1)

    files_filtered = []
    for file in files:
        if int(file[0:4]) < start_year or int(file[0:4]) > end_year:  # type: ignore
            continue
        else:
            files_filtered.append(file)  # type: ignore
    years = []

    for idx, file in enumerate(tqdm.tqdm(files_filtered)):  # type: ignore
        if int(file[0:4]) < start_year or int(file[0:4]) > end_year:  # type: ignore
            continue
        file = "chadwick_csv/" + file  # type: ignore
        with open(file, "r") as f:  # type: ignore
            reader = dd.read_csv(f)  # type: ignore
            years.append(reader)  # type: ignore
    plays = dd.concat(years)  # type: ignore
    linear_weights = calc_woba_weights(plays)
    with open("weights_averages/woba_weights.json", "w") as f:
        json.dump(linear_weights, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-year",
        "-s",
        help=f"Start year of data gathering (defaults to {start_data_year} for the first year of statcast data)",
        type=int,
        default=start_data_year,
    )
    parser.add_argument(
        "--end-year",
        "-e",
        help=f"End year of data gathering (defaults to {end_data_year}, current retrosheet year as of coding)",
        type=int,
        default=end_data_year,
    )

    args = parser.parse_args(sys.argv[1:])
    main(args.start_year, args.end_year)
