# Custom WAR

This is my work in progress WAR calculator. It will be my own custom implementation of WAR. I'm looking to make it so pitcher quality of contact (xBA, xSLG) is considered to give contact pitchers credit for what they do, and that's all I know already (lol). This will probably take me a while, so I'm taking it one step at a time.

## Credits and copyrights

Some data downloaded comes from retrosheet. This means that, as of now, you can only calculate WAR for completed years after they're added to retrosheet. This is their data usage notice:

```
     The information used here was obtained free of
     charge from and is copyrighted by Retrosheet.  Interested
     parties may contact Retrosheet at "www.retrosheet.org".
```

There's also Statcast data that you must download. I use [PyBaseball](https://github.com/jldbc/pybaseball) to download it.

## Tutorial

There are different keyword arguments for all of the Python files (mostly just to set the start and end years). Just use the `--help` keyword argument after the python file command to see the syntax. For the year arguments, never use a value below 2015 because this relies on statcast data. Why I haven't restricted it? I don't know, but I haven't.

First run this in order to create all the necessary folders:

```
$ ./init_project.sh
```

Then, run these commands to download the retrosheet data:

```
$ python3 download.py
$ ./retrosheet_to_csv.sh
```

Also, run this to download statcast data:

```
$ python3 statcast_pitch_download.py
$ python3 combine_csv.py
```

To calculate the cumulative and average stats and store it, run:

```
$ python3 calc_averages.py
```
