#pragma once

#include <iostream>
#include <memory>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

// --- Abstract Base Class ---

class IRule
{
  public:
    virtual ~IRule() = default;

    /**
     * @brief Generates a string fragment based on the rule.
     * @param original_value The original data from the SQL dump (needed for REGEX).
     */
    virtual std::string apply(const std::string &original_value) = 0;
};

// --- Concrete Implementations ---

/**
 * @brief Represents static text (the parts of the string OUTSIDE the {{ }} tags).
 */
class StaticTextRule : public IRule
{
    std::string text_;

  public:
    explicit StaticTextRule(std::string text) : text_(std::move(text))
    {
    }
    std::string apply(const std::string &) override
    {
        return text_;
    }
};

/**
 * @brief Generates a random integer. Usage: {{RAND(min, max)}}
 */
class RandomIntRule : public IRule
{
    int min_;
    int max_;
    std::mt19937 rng_;

  public:
    RandomIntRule(int min, int max) : min_(min), max_(max), rng_(std::random_device{}())
    {
    }
    std::string apply(const std::string &) override
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

    std::string apply(const std::string &) override
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
    std::string replacement_;

  public:
    RegexRule(const std::string &pattern, std::string replacement)
        : pattern_(pattern), replacement_(std::move(replacement))
    {
    }

    std::string apply(const std::string &original_value) override
    {
        // Only attempts replacement if the pattern matches, otherwise returns original
        return std::regex_replace(original_value, pattern_, replacement_);
    }
};

/**
 * @brief The Composite Rule. It holds a sequence of other rules and joins their outputs.
 */
class CompositeRule : public IRule
{
    std::vector<std::shared_ptr<IRule>> sub_rules_;

  public:
    void add_rule(std::shared_ptr<IRule> rule)
    {
        sub_rules_.push_back(std::move(rule));
    }

    std::string apply(const std::string &original_value) override
    {
        std::ostringstream result;
        for (const auto &rule : sub_rules_)
        {
            result << rule->apply(original_value);
        }
        return result.str();
    }
};

// --- Factory for Parsing ---

class RuleFactory
{
  public:
    /**
     * @brief Parses a template string like "User-{{RAND(1,9)}}" into a CompositeRule.
     */
    static std::shared_ptr<IRule> parse_template(const std::string &raw_template)
    {
        auto composite = std::make_shared<CompositeRule>();

        // Regex to find {{ ... }} blocks
        // capture group 1 is the content inside brackets
        std::regex tag_pattern(R"(\{\{(.*?)\}\})");

        auto begin = std::sregex_iterator(raw_template.begin(), raw_template.end(), tag_pattern);
        auto end = std::sregex_iterator();

        size_t last_pos = 0;

        for (std::sregex_iterator i = begin; i != end; ++i)
        {
            std::smatch match = *i;

            // 1. Add the static text BEFORE this tag
            if (match.position() > last_pos)
            {
                std::string static_part = raw_template.substr(last_pos, match.position() - last_pos);
                composite->add_rule(std::make_shared<StaticTextRule>(static_part));
            }

            // 2. Parse and add the function INSIDE the tag
            std::string func_def = match[1].str();
            composite->add_rule(create_func_rule(func_def));

            last_pos = match.position() + match.length();
        }

        // 3. Add any remaining static text AFTER the last tag
        if (last_pos < raw_template.length())
        {
            composite->add_rule(std::make_shared<StaticTextRule>(raw_template.substr(last_pos)));
        }

        return composite;
    }

  private:
    static std::shared_ptr<IRule> create_func_rule(const std::string &func_def)
    {
        // Pattern: NAME(ARGS) or just NAME
        static const std::regex func_parser(R"(^\s*([A-Z0-9_]+)(?:\((.*)\))?\s*$)", std::regex::optimize);
        std::smatch matches;

        if (std::regex_match(func_def, matches, func_parser))
        {
            std::string name = matches[1];
            std::string args_str = matches[2];
            std::vector<std::string> args = parse_args(args_str);

            if (name == "RAND" && args.size() == 2)
            {
                try
                {
                    return std::make_shared<RandomIntRule>(std::stoi(args[0]), std::stoi(args[1]));
                }
                catch (...)
                {
                }
            }
            else if (name == "PICK" && !args.empty())
            {
                return std::make_shared<PickRule>(args);
            }
            else if (name == "REGEX" && args.size() >= 2)
            {
                // Re-join args for regex if commas split the pattern incorrectly?
                // For simplicity, we assume standard CSV args here.
                return std::make_shared<RegexRule>(args[0], args[1]);
            }
            else if (name == "LITERAL" && !args.empty())
            {
                return std::make_shared<StaticTextRule>(args[0]);
            }
        }

        // Fallback: if we can't parse the function, return it as static text "((ERROR))"
        // or effectively ignore the tag logic.
        std::cerr << "Warning: Unknown or malformed function: " << func_def << "\n";
        return std::make_shared<StaticTextRule>("");
    }

    static std::vector<std::string> parse_args(const std::string &args_str)
    {
        std::vector<std::string> args;
        std::stringstream ss(args_str);
        std::string segment;
        while (std::getline(ss, segment, ','))
        {
            // Trim whitespace
            size_t first = segment.find_first_not_of(" ");
            size_t last = segment.find_last_not_of(" ");
            if (first != std::string::npos && last != std::string::npos)
                args.push_back(segment.substr(first, (last - first + 1)));
            else if (first != std::string::npos)
                args.push_back(segment.substr(first));
            else if (!segment.empty()) // keep empty strings if they are explicit
                args.push_back("");
        }
        return args;
    }
};