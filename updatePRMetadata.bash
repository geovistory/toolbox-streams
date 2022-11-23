#!/bin/bash

# Explanation
# Call this script without argument:
# bash updatePRMetadata.bash
# > will set pullRequestNumber to null
# > will set pullRequestPushCount to null
#
# Call this script with the PR number as first argument:
# bash updatePRMetadata.bash 23
# > will set pullRequestNumber to 23 and increase pullRequestPushCount
# > in case pullRequestNumber was already 23, else sets it to 0

file="build.gradle"
regexPrNr="(.*)(pullRequestNumber = )([0-9null]*)(.*)"
regexPrPushCount="(.*)(pullRequestPushCount = )([0-9null]*)(.*)"

if [[ -z $1 ]]; then
  echo 'No PR-Nr.'

  if [[ $(<"$file") =~ $regexPrNr ]]; then
    echo 'set pullRequestNumber = null'
    echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}null${BASH_REMATCH[4]}" >$file
  fi

  if [[ $(<"$file") =~ $regexPrPushCount ]]; then
    echo 'set pullRequestPushCount = null'
    echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}null${BASH_REMATCH[4]}" >$file
  fi
else

  if [[ $(<"$file") =~ $regexPrNr ]]; then
    oldPrNr="${BASH_REMATCH[3]}"
    echo "Old PR: $oldPrNr – New PR: $1"

    if [[ $oldPrNr == $1 ]]; then
      if [[ $(<"$file") =~ $regexPrPushCount ]]; then
        oldPrPushCount="${BASH_REMATCH[3]}"
        newPrPushCount="$((oldPrPushCount + 1))"
        echo "Increase pullRequestPushCount from $oldPrPushCount to $newPrPushCount"
        echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}$newPrPushCount${BASH_REMATCH[4]}" >$file
      fi
    else
      echo "set pullRequestNumber = $1"
      echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}$1${BASH_REMATCH[4]}" >$file

      if [[ $(<"$file") =~ $regexPrPushCount ]]; then
        echo 'set pullRequestPushCount = 0'
        echo "${BASH_REMATCH[1]}${BASH_REMATCH[2]}0${BASH_REMATCH[4]}" >$file
      fi
    fi
  fi
fi
