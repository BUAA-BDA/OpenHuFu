#ifndef __ABY4J_PARTY_H__
#define __ABY4J_PARTY_H__


#include <abycore/circuit/booleancircuits.h>
#include <abycore/circuit/arithmeticcircuits.h>
#include <abycore/circuit/circuit.h>
#include <abycore/sharing/sharing.h>
#include <abycore/aby/abyparty.h>
#include <memory>
#include <map>

namespace aby4j {
  using std::unique_ptr;
  using std::shared_ptr;

  class Party {
  public:
    Party(uint32_t party_id, const std::string& server_addr, uint16_t server_port, seclvl seclvl = LT, uint32_t bitlen = 32, uint32_t nthreads = 2, e_mt_gen_alg mg_algo = MT_OT, uint32_t reservegates = 65536, const std::string& abycircdir = ABY_CIRCUIT_DIR);

    bool AddClient(uint32_t party_id, const std::string& client_addr, uint16_t client_port);
    void Reset(uint32_t party_id);

    bool GreaterI32(e_role role, uint32_t pid, int32_t value, e_sharing sharing=S_YAO);

  private:
    const uint32_t pid_;
    const seclvl seclvl_;
    const uint32_t bitlen_;
    const uint32_t nthreads_;
    const e_mt_gen_alg mg_algo_;
    const uint32_t reservegates_;
    const std::string abycircdir_;
    const shared_ptr<ABYParty> server_party_;
    std::map<uint32_t, shared_ptr<ABYParty>> client_parties_;
  };
}

#endif //__ABY4J_PARTY_H__