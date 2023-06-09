#!/bin/bash
echoerr() { echo "$@" 1>&2; }


if [ ! -d downloads/retrosheet ]; then
    echoerr "You haven't downloaded retrosheet files. Have you run download.py?";
    exit 1;
fi

# rm -rf downloads/*.ROS
# rm -rf downloads/*.EDN
pushd downloads/retrosheet
# Removed *.EVE because that's generally playoffs
for filename in *.EVN *.EVA; do
    # ${filename%????} removes the last 4 characters of the string, aka the file extention
    # temp="${filename:10}"
    # echo ""${temp:0:4}""
    cwevent -q -f 2,4,10,11,15,26-28,31,34,36-45,47,48,58-74 -x 0-3,8,13,14,45,51-55 -y "${filename:0:4}" -n "${filename}" > "../../data/chadwick/${filename}.events.csv";
    echo $filename
done
popd