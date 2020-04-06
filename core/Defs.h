//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace scar {

enum class ExecutorStatus {
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Kiva_READ,
  Kiva_COMMIT,
  Aria_READ,
  Aria_COMMIT,
  Aria_Fallback,
  Bohm_Analysis,
  Bohm_Insert,
  Bohm_Execute,
  Bohm_GC,
  Pwv_Analysis,
  Pwv_Execute,
  STOP,
  EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

} // namespace scar
