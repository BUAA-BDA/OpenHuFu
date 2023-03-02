%module aby4j_party
%include "std_string.i"
%include "stdint.i"
%include "enums.swg"
%javaconst(1);
enum e_role { SERVER, CLIENT };

enum e_mt_gen_alg {
  MT_OT = 0,
  MT_PAILLIER = 1,
  MT_DGK = 2,
  MT_LAST = 3
};

enum e_sharing {
  S_BOOL = 0,
  S_YAO = 1,
  S_ARITH = 2,
  S_YAO_REV= 3,
  S_SPLUT = 4,
  S_LAST = 5,
};

%{
#include "aby4j/aby4j_party.h"
%}

%include "aby4j/aby4j_party.h"