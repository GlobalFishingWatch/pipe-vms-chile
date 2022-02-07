#!/usr/bin/env bash

THIS_SCRIPT_DIR="$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )"

display_usage() {
  echo "Available Commands"
  echo "  fetch_normalized_vms   Copy and normalize vms data."
}


if [[ $# -le 0 ]]
then
    display_usage
    exit 1
fi

case $1 in

  fetch_normalized_vms)
    xdaterange ${THIS_SCRIPT_DIR}/fetch_normalized_vms.sh "${@:2}"
    ;;

  *)
    display_usage
    exit 1
    ;;
esac
