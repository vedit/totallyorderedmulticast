#!/bin/bash

set -o errexit
set -o nounset

launch_newterminal (){
  x-terminal-emulator -x sh -c "$1; bash"
}

launch_node (){
  echo "Launching PID $1"
#  echo "Press any key to continue..."
#  read -n 1
  launch_newterminal "mvn exec:java -D exec.mainClass=com.isikun.firat.totallyorderedmulticast.Main -DconfigFile=config$(expr $1).properties" &
}

generate_ports() {
for NODE in $(seq 0 $1)
do
  echo $(expr 40000 + ${NODE})
done
}

generate_propfiles(){
ports=$(generate_ports $1)
for pid in $(seq 0 $1)
do
  printf "${pid}\n$(expr $1 + 1)\n${ports}" > config${pid}.properties
done
}

main() {
mvn clean install

generate_propfiles $1

for CTR in $(seq 0 $1)
do
  launch_node ${CTR}
  sleep 2
done
}

main $1