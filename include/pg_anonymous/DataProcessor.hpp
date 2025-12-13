#pragma once

#include "Rules.hpp"
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

using ReplacementRules = std::map<std::string, std::map<std::string, std::shared_ptr<IRule>>>;

class DataProcessor
{
  public:
    DataProcessor(const std::string &config_file_path);

    int process_dump(const std::string &input_file_path, const std::string &output_file_path);

  private:
    YAML::Node config_;
    ReplacementRules replacement_rules_;

    enum class ParserState
    {
        SearchingForCopy,
        ReadingData
    };

    ReplacementRules load_rules(const YAML::Node &config) const;
    void parse_copy_columns(const std::string &raw_columns, std::vector<std::string> &columns) const;
};