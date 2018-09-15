//
// Created by Yi Lu on 9/15/18.
//

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

namespace scar {

class CalvinHelper {

public:
  static std::vector<std::size_t>
  get_replica_group_sizes(const std::string &replica_group) {
    std::vector<std::string> replica_group_sizes_string;
    boost::algorithm::split(replica_group_sizes_string, replica_group,
                            boost::is_any_of(","));
    std::vector<std::size_t> replica_group_sizes;
    for (auto i = 0u; i < replica_group_sizes_string.size(); i++) {
      replica_group_sizes.push_back(
          std::atoi(replica_group_sizes_string[i].c_str()));
    }

    return replica_group_sizes;
  }
};
} // namespace scar