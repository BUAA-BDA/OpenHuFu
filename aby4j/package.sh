#!/bin/bash
if [ ${OPENHUFU_ROOT} ];then
  mkdir -p ${OPENHUFU_ROOT}/lib
  export LD_LIBRARY_PATH=${OPENHUFU_ROOT}/lib
  mkdir -p lib
  cd swig/
  rm -rf build && mkdir build && cd build
  cmake .. && make -j 8
  cp lib/* ${OPENHUFU_ROOT}/lib
  cd ../..
else
  echo "\${OPENHUFU_ROOT} is not defined"
fi
