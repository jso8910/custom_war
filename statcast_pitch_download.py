import pybaseball  # type: ignore

month_to_days = {
    1: 31,
    2: 28,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}

pybaseball.cache.enable()  # type: ignore
for year in range(2015, 2023 + 1):
    for month in range(1, 12):
        pybaseball.statcast(  # type: ignore
            start_dt=f"{year}-{month:02}-01",
            end_dt=f"{year}-{month:02}-{month_to_days[month]}",
            parallel=True,
        ).to_csv(
            f"downloads/statcast/statcast_y{year}_m{month}.csv", index=False
        )  # type: ignore
