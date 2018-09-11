//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace scar {
enum class ExecutorStatus { START, CLEANUP, C_PHASE, S_PHASE, STOP, EXIT };

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

} // namespace scar