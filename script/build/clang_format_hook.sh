#!/bin/bash

# This script is a Git pre-commit hook which runs clang-format on all staged files, fixing any problems found and
# re-staging the result. The hook can be installed by this script itself, using the --install option.
#
# The script is configurable via the following parameters:
#   --install Install the script as a pre-commit hook instead of running it. This operation overwrites any existing
#             pre-commit hooks on this repository and is therefore idempotent.

set -e
exitWithError() {
    echo "$1";
    echo "Usage: $0 [--install]";
    exit 1;
}

INSTALL=false
while [ "$#" -gt 0 ]; do
    case $1 in
        --install) INSTALL=true ;;
            *) exitWithError "Unknown parameter: $1" ;;
    esac
    shift
done

if [ "$INSTALL" = true ]; then
  SCRIPT_FILE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
  GIT_HOOK_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.git/hooks/"; pwd)
  cp "${SCRIPT_FILE}" "${GIT_HOOK_DIR}/pre-commit"
else
  python3 script/build/run_clang_format.py --clang_format_binary clang-format --source_dir src --as_git_hook
fi
