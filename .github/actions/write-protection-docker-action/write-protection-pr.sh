#!/bin/bash
#
# Perform write protection over the critical files under a GITHUB pull request
# The critical files will be passed in via positional arguments.
#
# Author:
#   Ryan,Gao (ryangao-au@outlook.com)
# Revision History:
#   Date	   Author	   Comments
# -----------------------------------------------------------------------------
#   01/09/2024     Ryan, Gao       Created this script
#   03/09/2024     Ryan, Gao       Add more cmdline options preceding env
#
# Inputs:
#   Environmental Variables:
#     ENV_GITHUB_SOURCE_REF
#     ENV_GITHUB_TARGET_REF
#     ENV_GITHUB_ACTING_USER
# Returns:
#   OS Exit Code: 1 -- check fails; 0 -- check passes

IS_FORKED_PULL_REQUEST=0
GITHUB_SOURCE_REF=${ENV_GITHUB_SOURCE_REF}
GITHUB_TARGET_REF=${ENV_GITHUB_TARGET_REF}
GITHUB_ACTING_USER=${ENV_GITHUB_ACTING_USER}
ADMINISTRATIVE_USERS="EMPTY_LIST"


############################################################
# Check if it is a pull request under GITHUB action runtime
# Globals:
#   GITHUB_SOURCE_REF
#   GITHUB_TARGET_REF
# Arguments:
#   None
# Returns:
#   0 -- non-github run; 1 -- github action run
############################################################
function is_github_pr() {
  if [[ -n ${GITHUB_SOURCE_REF} && -n ${GITHUB_TARGET_REF} ]]; then
    return 1
  fi
  return 0
}

############################################################
# Check if a user is of administrative role
# Globals:
#   ADMINISTRATIVE_USERS
# Arguments:
#   candidate_user
# Returns:
#   0 -- non administrative user; 1 -- administrative user
############################################################
function is_admin_user() {
  OIFS=$IFS
  IFS=";"
  for admin in ${ADMINISTRATIVE_USERS}; do
    if [[ $1 == $admin ]]; then
      IFS=$OIFS
      return 1
    fi
  done
  IFS=$OIFS
  return 0
}

############################################################
# Check if a candidate file changed within the PR scope
# Globals:
#
# Arguments:
#   merge_reference
#   base_reference
#   candidate_file
# Returns:
#   0 -- no changes; 1 -- candidate file changed
############################################################
function is_file_changed() {
  is_forked_diff=$1
  dest_ref=$3
  src_ref=$2
  shift;shift;shift
  if [[ ${is_forked_diff} -eq 1 ]]; then
    git diff --name-only --exit-code "${dest_ref}" "${src_ref}" -- $@
  else
    git diff --name-only --exit-code --merge-base "${dest_ref}" "${src_ref}" -- $@
  fi
  if [[ $? -ne 0 ]]; then
    return 1
  fi
  return 0
}


# Main procedure starts

# Parse arguments
args=$(getopt "k:s:t:u:r" $*)
if [ $? -ne 0 ]; then
  echo 'Usage: ...'
  exit 2
fi

set -- $args
candidate_filenames=()
while :; do
  case "$1" in
  "-k")
    ADMINISTRATIVE_USERS=$2
    shift;shift
  ;;
  "-s")
    GITHUB_SOURCE_REF=$2
    shift;shift
  ;;
  "-t")
    GITHUB_TARGET_REF=$2
    shift;shift
  ;;
  "-u")
    GITHUB_ACTING_USER=$2
    shift;shift
  ;;
  "-r")
    IS_FORKED_PULL_REQUEST=1
    shift
  ;;
  "--")
    shift; break
  ;;
  "*")
    echo "hit $1"
    shift; break
  ;;
  esac
done
printf "admin_list=%s\n" $ADMINISTRATIVE_USERS

# Check GITHUB runtime
is_github_pr
if [[ $? -eq 0 ]]; then
  printf "It is not github action run, exiting...\n"
  exit 0
fi

# Check auther role
is_admin_user ${GITHUB_ACTING_USER}
if [[ $? -ne 0 ]]; then
  printf "User %s is a administrator of this repository, pass all checks\n" ${GITHUB_ACTING_USER}
  exit 0
fi

## Checking candidates
is_file_changed ${IS_FORKED_PULL_REQUEST} "${GITHUB_SOURCE_REF}" "${GITHUB_TARGET_REF}" $@

if [[ $? -ne 0 ]]; then
  printf "protected files changed\n"
  exit 1
fi
printf "protected files are not changed\n"
exit 0

#for filename in $@; do
#  candidate_filenames+=($filename)
#  is_file_changed "${GITHUB_SOURCE_REF}" "${GITHUB_TARGET_REF}" ${filename}
#  if [[ $? -ne 0 ]]; then
#    printf "%s file changed\n" $filename
#    exit 1
#  fi
#  printf "%s file is not changed\n" $filename
#done
#
#exit 0
