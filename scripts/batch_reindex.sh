#!/bin/bash

##
# batch_reindex.sh
# The tool to batch reindex ROS bags.
#
# Version: 1.0.0
# Author:  David Qiu
# Email:   david@davidqiu.com
# Website: www.davidqiu.com
#
# Copyright (C) 2019, Dicong Qiu. In MIT License.
##

DATADIR=$1

function procdir {
  dirpath=$1
  for itempath in "$dirpath"/*; do
    item=`basename "$itempath"`;
    if [[ -f "$itempath" ]]; then
      if [[ "$item" == *.bag.active ]]; then
        echo "Reindex: $itempath";
        rosbag reindex "$itempath";
        path_prefix=${itempath%%.bag.active}
        mv "$path_prefix"".bag.active" "$path_prefix"".bag";
        rm "$path_prefix"".bag.orig.active";
      fi
    elif [[ -d "$itempath" ]]; then
      procdir "$itempath";
    fi
  done
}

procdir "$DATADIR";
