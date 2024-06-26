#!/bin/bash

function arg_conversion {
    args=("$@")

    # Initialize an empty array to hold the modified arguments
    modified_args=()

    for arg in "${args[@]}"
    do
        # Check if the argument starts with a single hyphen and is not a double hyphen
        if [[ $arg == -[!-]* ]]; then
            # Modify the argument to start with a double hyphen
            modified_args+=(--"${arg:1}")
        else
            # Add the argument as is
            modified_args+=("$arg")
        fi
    done
    
    echo "${modified_args[@]}"
}
