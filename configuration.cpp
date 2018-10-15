/*
 * configuration.cpp
 *
 *  Created on: 24 Jul 2017
 *      Author: Dean De Leo
 */

#include "configuration.hpp"

#include <algorithm>
#include <cstdlib> // exit
#include <iostream>
#include <string>

#include "console_arguments.hpp"
#include "database.hpp"
#include "miscellaneous.hpp"

using namespace std;

#define ERROR(msg) RAISE_EXCEPTION(ConfigurationException, msg)

namespace configuration {

/*****************************************************************************
 *                                                                           *
 *  Singleton                                                                *
 *                                                                           *
 *****************************************************************************/
Configuration Configuration::singleton;

Configuration::Configuration() {

    // Command line arguments
    PARAMETER(string, "database").hint("path")
            .descr("The SQLite3 database where to store the results of the run").set_default("results.sqlite3");
    PARAMETER(int64_t, "initial_size").hint("N >= 0")
            .descr("The initial size of the data structure. The actual semantic depends on the experiment.")
            .validate_fn([](int64_t value){return value >= 0; });
    PARAMETER(int64_t, "num_insertions")["I"].hint("N >= 0").alias("num_inserts").alias("I")
            .descr("The number of insertions to perform in the experiment. The final semantic depends on the experiment.")
            .validate_fn([](int64_t value){ return value >= 0; });
    PARAMETER(int64_t, "num_lookups")["L"].hint("N >= 0").alias("L").set_default(0)
            .descr("The number of lookups to perform. The final semantic depends on the experiment.")
            .validate_fn([](int64_t value){ return value >= 0; });
    PARAMETER(int64_t, "num_scans")["S"].hint("N >= 0").alias("S").set_default(0)
            .descr("The number of scans to perform. The final semantic depends on the experiment.")
            .validate_fn([](int64_t value){ return value >= 0; });
    PARAMETER(bool, "verbose")["v"]
            .descr("Display additional messages to the standard output.");
    m_verbose = dynamic_cast<details::ParameterImpl<bool>*>(m_console_parameters.back());
    auto parameter_git_commit = PARAMETER(string, "git_commit").hint("commit")
            .descr("Record in the database the current git commit. It does not change the execution. By default, it attempts to retrieve the current commit automatically.");
    auto str_git_last_commit = git_last_commit();
    if(!str_git_last_commit.empty()){ parameter_git_commit.set_default(str_git_last_commit); }
    PARAMETER(uint64_t, "seed_lookups").hint("N").set_default(73867)
            .descr("The seed for the experiment lookups");
    PARAMETER(uint64_t, "seed_random_permutation").hint("N").set_default(152981)
            .descr("The seed for the random generator that initialises the order in which elements are inserted in the data structure");
    PARAMETER(string, "hostname").hint().set_default(hostname())
            .descr("Record the hostname where the simulation has been executed.");
    PARAMETER(uint64_t, "memory_pool").hint("N").set_default(67108864)
            .descr("Capacity of the the internal memory pools");
    PARAMETER(bool, "hugetlb")
        .descr("Use huge pages (2Mb) with the algorithms that support memory rewiring");
}

Configuration::~Configuration() {
    for(size_t i = 0; i < m_console_parameters.size(); i++){
        delete m_console_parameters[i]; m_console_parameters[i] = nullptr;
    }
}

void Configuration::parse_command_line_args(int argc, char* argv[]){
    using namespace clara;

    bool show_help_prompt = false;
    string program_name;

    // sort the options
    sort(begin(m_console_parameters), end(m_console_parameters), [](const auto& p1, const auto& p2){
       string v1;
       if(p1->has_short_option())
           v1 = p1->get_short();
       else
           v1 = p1->get_long();
       string v2;
       if(p2->has_short_option())
           v2 = p2->get_short();
       else
           v2 = p2->get_long();

       return v1 < v2;
    });

    auto cli = ExeName(program_name) | Help(show_help_prompt);

    for(decltype(auto) param : m_console_parameters){
        bool has_default = param->is_default();

        Opt option = param->generate_option();

        if(!param->has_short_option() && !param->has_long_option()){
            RAISE_EXCEPTION(ConsoleArgumentError, "The option " << param->name() << " does not have a short option nor a long option associated to it");
        }
        if(param->has_short_option()){
            stringstream ss;
            ss << "-" << param->get_short();
            option[ss.str()];
        }

        if(param->has_long_option()){
            stringstream ss;
            ss << "--" << param->get_long();
            option[ss.str()];
        }

        if(!option.has_description()){
            stringstream ss;
            const string& description = param->description();
            if(!description.empty()){
                ss << description;
                if(has_default){
                    char last_char = description[ description.size() -1 ];
                    if (last_char != '.' && last_char != '\n'){
                        ss << ".";
                    }
                    ss << " ";
                }
            }
            if(has_default){
                ss << "The default value is " << param->to_string() << ".";
            }

            option(ss.str());
        }

        if(param->is_required()){
            option.required();
        }

        cli |= option;
    }

    auto res = cli.parse(Args{argc, argv});

    if(argc <= 1 || show_help_prompt){
        cout << cli << endl;
        exit(EXIT_SUCCESS);
    }

    // Parsing error (...)
    if(!res){ ERROR("Cannot parse the command line parameters: " << res.errorMessage()); }
    if(res.value().type() == ParseResultType::NoMatch){
        auto lexemes = res.value().remainingTokens();
        auto lexeme = *lexemes;

        stringstream sserror;
        if(lexeme.type == clara::detail::TokenType::Option){
            if(lexeme.token.length() >= 2 && lexeme.token[0] == '-' && lexeme.token[1] != '-'){
                sserror << "Invalid short option: " << lexeme.token;

                ++lexemes;
                if(lexemes.operator bool()){
                    stringstream sslongoption;
                    do {

                        auto lexeme_next = *lexemes;
                        string attribute_next = lexeme_next.token;
                        if(lexeme_next.type == clara::detail::TokenType::Option){
                            if(attribute_next.length() == 2 && attribute_next[0] == '-' && attribute_next[1] != '-'){
                                sslongoption << attribute_next[1];
                            }
                        }

                        ++lexemes;
                    } while(lexemes.operator bool());
                    string strlongoption = sslongoption.str();
                    if(!strlongoption.empty()){
                        sserror << ". Perhaps it should be long option -" << lexeme.token << strlongoption << " ?";
                    }
                }
            } else {
                sserror << "Invalid option: " << lexeme.token;
            }
        } else {
            sserror << "Invalid argument: " << lexeme.token;
        }
        ERROR(sserror.str());
    } else if (res.value().remainingTokens().operator bool()){ // some argument was not properly parsed
        ERROR("Console argument not recognised: " << res.value().remainingTokens()->token);
    }

    // check all required options have been set
    for(decltype(auto) p : m_console_parameters){
        if(p->is_required() && !p->is_set()){
            if(p->has_long_option()){
                ERROR("Mandatory parameter --" << p->get_long() << " not set");
            } else {
                ERROR("Mandatory parameter -" << p->get_short() << " not set");
            }
        }
    }
}

void Configuration::initialise_database() {
    if(m_database.get() != nullptr)
        RAISE_EXCEPTION(ConfigurationException, "Already initialised");
    m_database.reset(new database::Database());
}

bool Configuration::verbose() const {
    return m_verbose != nullptr && m_verbose->get();
}

bool use_huge_pages(){
    static bool warning_already_emitted = false;

    try {
        return ARGREF(bool, "hugetlb").get();
    } catch( configuration::ConsoleArgumentError& e ){
        if(!warning_already_emitted){
            cerr << "[use_huge_pages] Warning, configuration not initialised. Huge pages are disabled." << endl;
            warning_already_emitted = true; // emit it only once!
        }
    }

    return false;
}

} // namespace configuration
