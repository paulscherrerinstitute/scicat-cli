#!/bin/bash

# shellcheck disable=SC2034
RED='\033[0;31m'
YEL='\033[1;33m'
NC='\033[0m' # No Color
WARN_STRING="${RED}Warning!${YEL} These backwards compatibilty scripts will soon be deprecated!${NC} Please use, or update your code to use, the scicat-cli executable directly."

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
