#!/bin/sh

# Arguments:
#      -n: dry-run key, if enabled, then script does nothing
#     arg: up to n database names
# Description:
#     Iterates trough db names. If dry run is disabled, optimizes all tables in given databases

i=1
dry_run=0
for arg do
    # -n is first argument
    if [ "$arg" = "-n" ] && [ "$i" -eq "1" ]; then  
        dry_run=1
    fi
    # if not dry run; then optimize all tables in db
    if [ ! "$dry_run" -eq "1" ]; then
        echo "=========="
        mysqlcheck -o $arg -u root
    fi

    i=$((i + 1))
done


