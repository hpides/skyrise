#!/bin/bash

set -euo pipefail

commit_coverage=$(cat coverage_percentage.txt)
master_coverage=$(curl --max-time 10 --silent https://skyrise-ci.epic-hpi.de/job/skyrise/job/master/lastStableBuild/artifact/coverage_percentage.txt)

if [ ${master_coverage} ]; then
  if [ $(bc -l <<< "${commit_coverage%\%} >= ${master_coverage%\%}") -eq 1 ]; then
    echo -n "SUCCESS"
  else
    echo -n "FAILURE"
  fi
  echo ";This commit has a code coverage of ${commit_coverage} vs. ${master_coverage} on the master brach"
else
  echo -n "FAILURE"
  echo ";The code coverage of the master branch cannot be read"
fi
