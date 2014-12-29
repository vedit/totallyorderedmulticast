#!/bin/bash
kill_processes() {
  ps uax | grep com.isikun.firat.totallyorderedmulticast | awk '{ print $2 }' | xargs kill
}

kill_processes

rm config*.properties