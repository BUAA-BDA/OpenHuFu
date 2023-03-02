#include "aby4j/aby4j_party.h"

#include <iostream>

namespace aby4j {
  Party::Party(uint32_t party_id, const std::string& server_addr, uint16_t server_port, seclvl seclvl, uint32_t bitlen, uint32_t nthreads, e_mt_gen_alg mg_algo , uint32_t reservegates, const std::string& abycircdir) : pid_(party_id), seclvl_(seclvl), bitlen_(bitlen), nthreads_(nthreads), mg_algo_(mg_algo), reservegates_(reservegates), abycircdir_(abycircdir), server_party_(shared_ptr<ABYParty>(new ABYParty(SERVER, server_addr, server_port, seclvl, bitlen, nthreads, mg_algo, reservegates, abycircdir))) {}

  bool Party::AddClient(uint32_t party_id, const std::string& server_addr, uint16_t server_port) {
    if (client_parties_.count(party_id)) {
      return false;
    }
    this->client_parties_.emplace(party_id, shared_ptr<ABYParty>(new ABYParty(CLIENT, server_addr, server_port, this->seclvl_, this->bitlen_, this->nthreads_, this->mg_algo_, this->reservegates_, this->abycircdir_)));
    return true;
  }

  void Party::Reset(uint32_t party_id) {
    if (party_id == this->pid_) {
      server_party_->Reset();
    } else if (client_parties_.count(party_id)) {
      client_parties_[party_id]->Reset();
    }
  }

  bool Party::GreaterI32(e_role role, uint32_t pid, int32_t value, e_sharing sharing) {
    shared_ptr<ABYParty> party = role == SERVER ? this->server_party_ : this->client_parties_[pid];
    party->Reset();
    std::vector<Sharing*>& sharings = party->GetSharings();
    Circuit* circ = sharings[sharing]->GetCircuitBuildRoutine();
    share *s_alice, *s_bob, *s_out;
    uint32_t output;
    uint32_t uvalue = value < 0 ? (uint32_t) (value - INT_MIN) : (uint32_t) value - INT_MIN;
    if (role == SERVER) {
      s_alice = circ->PutDummyINGate(32);
      s_bob = circ->PutINGate(uvalue, 32, SERVER);
    } else {
      s_alice = circ->PutINGate(uvalue, 32, CLIENT);
      s_bob = circ->PutDummyINGate(32);
    }
    s_out = circ->PutGTGate(s_alice, s_bob);
    s_out = circ->PutOUTGate(s_out, ALL);
    party->ExecCircuit();
    output = s_out->get_clear_value<uint32_t>();
    return output;
  }
}