#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

// --- Context Definition ---

struct RowContext
{
    const std::vector<std::string> &headers;
    const std::vector<std::string> &row_values;

    std::string get_column_value(const std::string &col_name) const
    {
        auto it = std::find(headers.begin(), headers.end(), col_name);
        if (it != headers.end())
        {
            size_t index = std::distance(headers.begin(), it);
            if (index < row_values.size())
            {
                return row_values[index];
            }
        }
        return "";
    }
};

// --- Abstract Base Class ---

class IRule
{
  public:
    virtual ~IRule() = default;
    virtual std::string apply(const std::string &original_value, const RowContext &context) = 0;
};

// --- Concrete Implementations ---

class NoneRule : public IRule
{
    std::string apply(const std::string &original_value, const RowContext &context)
    {
        return original_value;
    }
};

class StaticTextRule : public IRule
{
    std::string text_;

  public:
    explicit StaticTextRule(std::string text) : text_(std::move(text))
    {
    }
    std::string apply(const std::string &, const RowContext &) override
    {
        return text_;
    }
};

/**
 * @brief Generates a random integer. Usage: {{RAND(min, max)}}
 */
class RandomIntRule : public IRule
{
    int min_, max_;
    std::mt19937 rng_;

  public:
    RandomIntRule(int min, int max) : min_(min), max_(max), rng_(std::random_device{}())
    {
    }
    std::string apply(const std::string &, const RowContext &) override
    {
        std::uniform_int_distribution<int> dist(min_, max_);
        return std::to_string(dist(rng_));
    }
};

/**
 * @brief Selects a random option from a list. Usage: {{PICK(A, B, C)}}
 */
class PickRule : public IRule
{
    std::vector<std::string> options_;
    std::mt19937 rng_;

  public:
    explicit PickRule(std::vector<std::string> options) : options_(std::move(options)), rng_(std::random_device{}())
    {
    }
    std::string apply(const std::string &, const RowContext &) override
    {
        if (options_.empty())
            return "";
        std::uniform_int_distribution<size_t> dist(0, options_.size() - 1);
        return options_[dist(rng_)];
    }
};

/**
 * @brief Applies a Regex replacement to the ORIGINAL value.
 * Usage: {{REGEX(pattern, replacement)}}
 */
class RegexRule : public IRule
{
    std::regex pattern_;
    std::shared_ptr<IRule> replacement_rule_;

  public:
    RegexRule(const std::string &pattern, std::shared_ptr<IRule> replacement_rule)
        : pattern_(pattern), replacement_rule_(std::move(replacement_rule))
    {
    }

    std::string apply(const std::string &original_value, const RowContext &context) override
    {
        std::string dynamic_replacement_template = replacement_rule_->apply(original_value, context);

        return std::regex_replace(original_value, pattern_, dynamic_replacement_template);
    }
};

class HashRule : public IRule
{
    unsigned int salt_;

  public:
    explicit HashRule(unsigned int salt) : salt_(salt)
    {
    }
    std::string apply(const std::string &original_value, const RowContext &) override
    {
        uint32_t hash = 2166136261u;
        std::string salt_str = std::to_string(salt_);
        // Mix Salt
        for (char c : salt_str)
        {
            hash ^= static_cast<unsigned char>(c);
            hash *= 16777619u;
        }
        // Mix Value
        for (char c : original_value)
        {
            hash ^= static_cast<unsigned char>(c);
            hash *= 16777619u;
        }
        return std::to_string(hash & 0x7FFFFFFF);
    }
};

/**
 * @brief Checks if a target column's value matches a regex pattern.
 * Usage: {{MATCHES(column_name, pattern)}}. Returns "true" or "false".
 */
class MatchesRule : public IRule
{
    std::string target_col_;
    std::regex pattern_;

  public:
    MatchesRule(std::string col, const std::string &pattern) : target_col_(std::move(col)), pattern_(pattern)
    {
    }

    std::string apply(const std::string &, const RowContext &context) override
    {
        std::string actual_val = context.get_column_value(target_col_);
        if (std::regex_match(actual_val, pattern_))
        {
            return "true";
        }
        return "false";
    }
};

class ConditionalRule : public IRule
{
    // FIX: Changed from std::string target_col_ to an IRule to allow nested evaluation
    std::shared_ptr<IRule> condition_check_rule_;
    std::string op_;
    std::string target_val_;
    std::shared_ptr<IRule> true_rule_;
    std::shared_ptr<IRule> false_rule_;

  public:
    ConditionalRule(std::shared_ptr<IRule> cond_rule, std::string op, std::string val, std::shared_ptr<IRule> t_rule,
                    std::shared_ptr<IRule> f_rule)
        : condition_check_rule_(std::move(cond_rule)), op_(std::move(op)), target_val_(std::move(val)),
          true_rule_(std::move(t_rule)), false_rule_(std::move(f_rule))
    {
    }

    std::string apply(const std::string &original_value, const RowContext &context) override
    {
        std::string actual_val = condition_check_rule_->apply(original_value, context);
        bool match = false;

        if (op_ == "EQ")
        {
            match = (actual_val == target_val_);
        }
        else if (op_ == "NEQ")
        {
            match = (actual_val != target_val_);
        }
        else if (op_ == "IN")
        {
            std::string args = target_val_;
            std::stringstream ss(args);
            std::string arg;
            while (std::getline(ss, arg, ','))
            {
                if (trim(arg) == actual_val)
                {
                    match = true;
                    break;
                }
            }
        }

        if (match)
            return true_rule_->apply(original_value, context);
        return false_rule_->apply(original_value, context);
    }

    static std::string trim(const std::string &str)
    {
        size_t first = str.find_first_not_of(" \t");
        if (std::string::npos == first)
            return "";
        size_t last = str.find_last_not_of(" \t");
        return str.substr(first, (last - first + 1));
    }
};

class CompositeRule : public IRule
{
    std::vector<std::shared_ptr<IRule>> sub_rules_;

  public:
    void add_rule(std::shared_ptr<IRule> rule)
    {
        sub_rules_.push_back(std::move(rule));
    }
    std::string apply(const std::string &original_value, const RowContext &context) override
    {
        std::ostringstream result;
        for (const auto &rule : sub_rules_)
            result << rule->apply(original_value, context);
        return result.str();
    }
};

// --- Factory for Parsing ---

class RuleFactory
{
  public:
    /**
     * @brief Parses template strings using a generic brace counter to support nesting.
     */
    static std::shared_ptr<IRule> parse_template(const std::string &raw_template)
    {
        auto composite = std::make_shared<CompositeRule>();
        size_t len = raw_template.length();
        size_t i = 0;
        size_t last_pos = 0;

        while (i < len)
        {
            // Look for start token "{{"
            if (i + 1 < len && raw_template[i] == '{' && raw_template[i + 1] == '{')
            {
                // 1. Capture text before this token
                if (i > last_pos)
                {
                    composite->add_rule(std::make_shared<StaticTextRule>(raw_template.substr(last_pos, i - last_pos)));
                }

                // 2. Find the matching closing "}}" by counting depth
                size_t start_content = i + 2;
                int depth = 2;
                size_t j = start_content;
                bool found_end = false;

                while (j < len)
                {
                    if (raw_template[j] == '{')
                        depth++;
                    else if (raw_template[j] == '}')
                        depth--;

                    if (depth == 0)
                    {
                        std::string content = raw_template.substr(start_content, j - 1 - start_content);
                        composite->add_rule(create_func_rule(content));

                        i = j + 1;
                        last_pos = i;
                        found_end = true;
                        break;
                    }
                    j++;
                }

                if (!found_end)
                {
                    // Error handling for unmatched braces
                    break;
                }
            }
            else
            {
                i++;
            }
        }

        // 3. Add remaining text
        if (last_pos < len)
        {
            composite->add_rule(std::make_shared<StaticTextRule>(raw_template.substr(last_pos)));
        }

        return composite;
    }

  private:
    static std::shared_ptr<IRule> create_func_rule(const std::string &func_def)
    {
        // 1. Extract Name
        size_t paren_start = func_def.find('(');
        std::string name, args_str;

        if (paren_start == std::string::npos)
        {
            name = func_def;
        }
        else
        {
            name = func_def.substr(0, paren_start);
            size_t paren_end = func_def.find_last_of(')');
            if (paren_end != std::string::npos && paren_end > paren_start)
                args_str = func_def.substr(paren_start + 1, paren_end - paren_start - 1);
        }

        name.erase(std::remove_if(name.begin(), name.end(), ::isspace), name.end());

        std::vector<std::string> args = smart_split_args(args_str);

        if (name == "NONE") // NONE rule now takes no arguments and returns NULL marker
        {
            return std::make_shared<NoneRule>();
        }
        else if (name == "RAND" && args.size() == 2)
        {
            try
            {
                return std::make_shared<RandomIntRule>(std::stoi(args[0]), std::stoi(args[1]));
            }
            catch (...)
            {
            }
        }
        else if (name == "HASH" && args.size() == 1)
        {
            unsigned int salt = 0;
            for (char c : args[0])
                salt = (salt * 31) + (unsigned char)c;
            return std::make_shared<HashRule>(salt);
        }
        else if (name == "PICK")
        {
            return std::make_shared<PickRule>(args);
        }
        else if (name == "REGEX" && args.size() >= 2)
        {
            auto replacement_rule = parse_template(args[1]);
            return std::make_shared<RegexRule>(args[0], replacement_rule);
        }
        else if (name == "LITERAL" && !args.empty())
        {
            return std::make_shared<StaticTextRule>(args[0]);
        }
        else if (name == "MATCHES" && args.size() == 2)
        {
            try
            {
                return std::make_shared<MatchesRule>(args[0], args[1]);
            }
            catch (const std::regex_error &e)
            {
                std::cerr << "Regex Error in MATCHES: " << e.what() << " for pattern: " << args[1] << "\n";
            }
            catch (...)
            {
            }
        }
        else if (name == "IF" && args.size() == 5)
        {
            auto condition_rule = parse_template(args[0]);
            auto true_rule = parse_template(args[3]);
            auto false_rule = parse_template(args[4]);
            return std::make_shared<ConditionalRule>(condition_rule, args[1], args[2], true_rule, false_rule);
        }

        std::cerr << "Warning: Unknown function or invalid args: " << name << " (Args count: " << args.size() << ")\n";
        return std::make_shared<StaticTextRule>("");
    }

    static std::vector<std::string> smart_split_args(const std::string &s)
    {
        std::vector<std::string> args;
        std::string current;
        int nesting = 0;

        for (char c : s)
        {
            if (c == '{' || c == '(')
                nesting++;
            else if (c == '}' || c == ')')
                nesting--;

            if (c == ',' && nesting == 0)
            {
                args.push_back(trim(current));
                current.clear();
            }
            else
            {
                current += c;
            }
        }
        args.push_back(trim(current));
        return args;
    }

    static std::string trim(const std::string &str)
    {
        size_t first = str.find_first_not_of(" \t");
        if (std::string::npos == first)
            return "";
        size_t last = str.find_last_not_of(" \t");
        return str.substr(first, (last - first + 1));
    }
};