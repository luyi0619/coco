//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "protocol/Silo/SiloHelper.h"

// RStoreHelper is as the same as SiloHelper (at least for now)
namespace scar {

enum class RStoreWorkerStatus { S_PHASE, C_PHASE, STOP, EXIT };

class RStoreHelper : public SiloHelper {};
} // namespace scar