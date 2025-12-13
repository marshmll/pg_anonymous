#include "pg_anonymous/DataProcessor.hpp"

#include <iostream>
#include <map>
#include <string>

// --- Function Prototypes ---
void print_usage(const std::string &program_name);
std::map<std::string, std::string> parse_arguments(int argc, char *argv[]);

// --- Global Constants for Arguments ---
const std::string CONFIG_FLAG = "--config";
const std::string CONFIG_SHORT_FLAG = "-c";
const std::string INPUT_FLAG = "--input";
const std::string INPUT_SHORT_FLAG = "-i";
const std::string OUTPUT_FLAG = "--output";
const std::string OUTPUT_SHORT_FLAG = "-o";
const std::string HELP_FLAG = "--help";
const std::string HELP_SHORT_FLAG = "-h";

/**
 * @brief Prints the correct usage information to stderr.
 * @param program_name The name of the executable (argv[0]).
 */
void print_usage(const std::string &program_name)
{
    std::cerr << "--- PG Anonymous ---\n";
    std::cerr << "An anonymization tool for PostgreSQL plain SQL dump files.\n";
    std::cerr << "Usage: " << program_name << " [OPTIONS]\n\n";
    std::cerr << "Options:\n";
    std::cerr << "  " << CONFIG_SHORT_FLAG << ", " << CONFIG_FLAG
              << "\t<file>  The YAML configuration file with redaction rules (REQUIRED).\n";
    std::cerr << "  " << INPUT_SHORT_FLAG << ", " << INPUT_FLAG
              << "\t<file>  The input PostgreSQL dump file (dump.sql) (REQUIRED).\n";
    std::cerr << "  " << OUTPUT_SHORT_FLAG << ", " << OUTPUT_FLAG
              << "\t<file>  The output file for the sanitized dump (out.sql) (REQUIRED).\n";
    std::cerr << "  " << HELP_SHORT_FLAG << ", " << HELP_FLAG << "\t\tShow this help message.\n";
    std::cerr << "\nExample: " << program_name << " -c config.yaml -i dump.sql -o out.sql\n";
}

/**
 * @brief Parses command-line arguments into a map of flag names to values.
 *
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return std::map<std::string, std::string> Map containing the canonical flag names and their values.
 */
std::map<std::string, std::string> parse_arguments(int argc, char *argv[])
{
    std::map<std::string, std::string> args;

    // Start loop from 1 to skip program name (argv[0])
    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];

        // Handle help flags
        if (arg == HELP_FLAG || arg == HELP_SHORT_FLAG)
        {
            args[HELP_FLAG] = "true";
            return args;
        }

        // Check if the argument is a recognized flag
        std::string canonical_flag;
        if (arg == CONFIG_FLAG || arg == CONFIG_SHORT_FLAG)
            canonical_flag = CONFIG_FLAG;
        else if (arg == INPUT_FLAG || arg == INPUT_SHORT_FLAG)
            canonical_flag = INPUT_FLAG;
        else if (arg == OUTPUT_FLAG || arg == OUTPUT_SHORT_FLAG)
            canonical_flag = OUTPUT_FLAG;
        else
        {
            std::cerr << "Error: Unknown argument or misplaced value: " << arg << "\n";
            args.clear(); // Clear map to signal error
            return args;
        }

        // Ensure there is a value following the flag
        if (i + 1 < argc)
        {
            args[canonical_flag] = argv[i + 1];
            i++; // Skip the next argument as it was the value
        }
        else
        {
            std::cerr << "Error: Flag " << arg << " requires a value.\n";
            args.clear(); // Clear map to signal error
            return args;
        }
    }

    return args;
}

int main(int argc, char *argv[])
{
    std::map<std::string, std::string> params = parse_arguments(argc, argv);
    const std::string program_name = argv[0];

    // Check for help flag first
    if (params.count(HELP_FLAG))
    {
        print_usage(program_name);
        return 0;
    }

    // Check for parsing error (map was cleared in parse_arguments)
    if (params.empty() && argc > 1)
    {
        print_usage(program_name);
        return 1;
    }

    // Check for missing required parameters
    if (!params.count(CONFIG_FLAG) || !params.count(INPUT_FLAG) || !params.count(OUTPUT_FLAG))
    {
        std::cerr << "Error: Missing required arguments. All -c, -i, and -o flags must be provided.\n";
        print_usage(program_name);
        return 1;
    }

    const std::string config_file = params.at(CONFIG_FLAG);
    const std::string input_file = params.at(INPUT_FLAG);
    const std::string output_file = params.at(OUTPUT_FLAG);

    std::cout << "--- PG Anonymous ---\n";
    std::cout << "Config File: " << config_file << "\n";
    std::cout << "Input File:  " << input_file << "\n";
    std::cout << "Output File: " << output_file << "\n\n";

    // 1. Initialize the processor (loads config and rules)
    DataProcessor processor(config_file);

    // 2. Process the dump file
    int result = processor.process_dump(input_file, output_file);

    if (result == 0)
    {
        std::cout << "\nProcessing completed successfully.\n";
    }
    else
    {
        std::cerr << "\nProcessing failed.\n";
    }

    return result;
}