#include "pg_anonymous/DataProcessor.hpp"
#include <algorithm>
#include <fstream>
#include <regex>
#include <sstream>

void DataProcessor::parse_copy_columns(const std::string &raw_columns, std::vector<std::string> &columns) const
{
    size_t start = raw_columns.find('(');
    size_t end = raw_columns.find_last_of(')');
    if (start == std::string::npos || end == std::string::npos || end <= start)
        return;

    std::string columns_str = raw_columns.substr(start + 1, end - start - 1);
    columns_str.erase(std::remove(columns_str.begin(), columns_str.end(), ' '), columns_str.end());
    columns_str.erase(std::remove(columns_str.begin(), columns_str.end(), '"'), columns_str.end());

    std::stringstream ss(columns_str);
    std::string col;
    while (std::getline(ss, col, ','))
    {
        if (!col.empty())
            columns.push_back(col);
    }
}

DataProcessor::DataProcessor(const std::string &config_file_path)
{
    try
    {
        config_ = YAML::LoadFile(config_file_path);
        replacement_rules_ = load_rules(config_);
    }
    catch (const std::exception &e)
    {
        std::cerr << "Initialization Error: " << e.what() << std::endl;
    }
}

ReplacementRules DataProcessor::load_rules(const YAML::Node &config) const
{
    ReplacementRules rules;
    if (!config["rules"] || !config["rules"].IsMap())
        return rules;

    const YAML::Node &rules_node = config["rules"];

    for (auto schema_it = rules_node.begin(); schema_it != rules_node.end(); ++schema_it)
    {
        std::string schema_name = schema_it->first.as<std::string>();
        const YAML::Node &schema_node = schema_it->second;
        if (!schema_node.IsMap())
            continue;

        for (auto table_it = schema_node.begin(); table_it != schema_node.end(); ++table_it)
        {
            std::string table_name = schema_name + "." + table_it->first.as<std::string>();
            const YAML::Node &table_node = table_it->second;
            if (!table_node.IsSequence())
                continue;

            for (const auto &rule_entry : table_node)
            {
                if (rule_entry.IsMap())
                {
                    for (auto rule_it = rule_entry.begin(); rule_it != rule_entry.end(); ++rule_it)
                    {
                        std::string col = rule_it->first.as<std::string>();
                        std::string raw_template = rule_it->second.as<std::string>();

                        // --- NEW: Use Parse Template ---
                        rules[table_name][col] = RuleFactory::parse_template(raw_template);

                        std::cout << "Loaded rule for " << table_name << "." << col << ": " << raw_template << "\n";
                    }
                }
            }
        }
    }
    return rules;
}

int DataProcessor::process_dump(const std::string &input_file_path, const std::string &output_file_path)
{
    std::ifstream file(input_file_path);
    std::ofstream out(output_file_path);

    if (!file.is_open() || !out.is_open())
        return 1;

    // Use raw pointer regex for performance
    const std::regex copy_pattern(R"(^\s*COPY\s+([\w\.]+)\s*(\([^;]+\))?\s+FROM\s+stdin\s*;\s*$)", std::regex::icase);
    const std::regex end_pattern(R"(^\s*\\\.\s*$)", std::regex::optimize);
    std::smatch matches;

    ParserState state = ParserState::SearchingForCopy;
    std::string line, current_table;
    std::vector<std::string> columns;

    while (std::getline(file, line))
    {
        if (state == ParserState::SearchingForCopy)
        {
            if (std::regex_match(line, matches, copy_pattern))
            {
                current_table = matches[1].str();
                out << line << "\n";
                columns.clear();
                if (matches.size() > 2 && matches[2].matched)
                    parse_copy_columns(matches[2].str(), columns);
                state = ParserState::ReadingData;
            }
            else
            {
                out << line << "\n";
            }
        }
        else if (state == ParserState::ReadingData)
        {
            if (std::regex_match(line, end_pattern))
            {
                out << line << "\n";
                state = ParserState::SearchingForCopy;
                columns.clear();
            }
            else
            {
                if (replacement_rules_.count(current_table) && !columns.empty())
                {
                    std::stringstream ss(line);
                    std::string token, processed_line;
                    size_t idx = 0;
                    auto &table_rules = replacement_rules_.at(current_table);

                    while (std::getline(ss, token, '\t'))
                    {
                        if (idx < columns.size() && table_rules.count(columns[idx]))
                        {
                            // Apply the rule (Composite or Simple)
                            token = table_rules.at(columns[idx])->apply(token);
                        }
                        if (idx > 0)
                            processed_line += '\t';
                        processed_line += token;
                        idx++;
                    }
                    out << processed_line << "\n";
                }
                else
                {
                    out << line << "\n";
                }
            }
        }
    }
    return 0;
}