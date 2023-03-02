include_directories(${INCLUDE_DIRECTORIES})
add_library(party aby4j_party.cpp)
target_link_libraries(party ABY::aby ENCRYPTO_utils::encrypto_utils)


set (UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
set_property(SOURCE aby4j_party.i PROPERTY SWIG_MODULE_NAME abyparty4j)
set_property(SOURCE aby4j_party.i PROPERTY CPLUSPLUS ON)
set_property(SOURCE aby4j_party.i PROPERTY COMPILE_OPTIONS -package com.hufudb.openhufu.mpc.aby.party)
swig_add_library(abyparty4j
        TYPE MODULE
        LANGUAGE java
        OUTPUT_DIR ${PROJECT_SOURCE_DIR}/../src/main/java/com/hufudb/openhufu/mpc/aby/party/
        SOURCES aby4j_party.i)

target_include_directories(abyparty4j PRIVATE ${JNI_INCLUDE_DIRS})
swig_link_libraries(abyparty4j PRIVATE party ${JNI_LIBRARIES})
target_compile_options(abyparty4j PRIVATE -w)
target_link_libraries(abyparty4j PRIVATE party ABY::aby ENCRYPTO_utils::encrypto_utils)
