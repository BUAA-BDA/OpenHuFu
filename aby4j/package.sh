#!/bin/bash
if [ ${OPENHUFU_ROOT} ];then
  mkdir -p ${OPENHUFU_ROOT}/lib
  export LD_LIBRARY_PATH=${OPENHUFU_ROOT}/lib
  if [[ $1 == "all" ]];then
    mkdir -p lib
    cd swig/
    rm -rf build && mkdir build && cd build
    cmake .. && make -j 8
    cp lib/* ${OPENHUFU_ROOT}/lib
    cd ../..
  fi
  mvn clean install -T 0.5C -Dmaven.test.skip=true
  cp target/*.jar ${OPENHUFU_ROOT}/lib/aby4j.jar
else
  echo "\${OPENHUFU_ROOT} is not defined"
fi
