/**
 * Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#include "console_arguments.hpp"

#include <algorithm> // transform
#include <cctype> // tolower
#include <cstring>
#include <iostream> // debug only
#include <string>

#include "configuration.hpp"

using namespace std;

namespace configuration {

namespace details {


ParameterBaseImpl::ParameterBaseImpl(const char* name, const char* source, int line) : m_long(name), m_required(false) {
    m_name.set(name, source, line);
}

ParameterBaseImpl::~ParameterBaseImpl(){ }

const char* ParameterBaseImpl::name() const {
    return m_name.m_value;
}

const char*ParameterBaseImpl::source() const {
    return m_name.m_source;
}

int ParameterBaseImpl::line() const {
    return m_name.m_line;
}

const std::string& ParameterBaseImpl::hint() const {
    return m_hint.m_value;
}

const std::string& ParameterBaseImpl::description() const {
    return m_description.m_value;
}

void ParameterBaseImpl::set_hint(const std::string& value, const char* source, int line){
    m_hint.set(value, source, line);
}

void ParameterBaseImpl::set_short(char value, const char* source, int line){
    if(m_short.m_state == State::SET){
        RAISE_EXCEPTION(ConsoleArgumentError, "Short option already set to `" << m_short.m_value << "' from " <<
                m_short.m_source << ":" << m_short.m_line << ". Attempting to reset to `" << value << "' at " << source << ":" << line);
    }
    m_short.set(value, source, line);
}

void ParameterBaseImpl::set_long(const char* value, const char* source, int line){
    if(m_long.m_state == State::SET){
        RAISE_EXCEPTION(ConsoleArgumentError, "Long option already set to `" << m_long.m_value << "' from " <<
                m_long.m_source << ":" << m_long.m_line << ". Attempting to reset to `" << value << "' at " << source << ":" << line);
    }
    m_long.set(value, source, line);
}

void ParameterBaseImpl::set_description(const string& value, const char* source, int line) {
    m_description.set(value, source, line);
}

void ParameterBaseImpl::set_required(const char* source, int line){
    m_required.set(true, source, line);
}

void ParameterBaseImpl::set_record_in_database(bool value, const char* source, int line){
    m_record_in_database.set(value, source, line);
}


bool ParameterBaseImpl::has_short_option() const{
    return m_short.m_state != State::UNSET;
}

bool ParameterBaseImpl::has_long_option() const {
    return m_long.m_state != State::UNSET;
}

bool ParameterBaseImpl::has_hint() const {
    return m_hint.m_state != State::UNSET;
}

bool ParameterBaseImpl::is_required() const {
    return m_required.m_value;
}

bool ParameterBaseImpl::is_recorded() const {
    return m_record_in_database.m_value;
}

char ParameterBaseImpl::get_short() const {
    if(!has_short_option()){
        RAISE_EXCEPTION(ConsoleArgumentError, "Short option does not exist");
    }
    return m_short.m_value;
}

const char* ParameterBaseImpl::get_long() const {
    if(!has_long_option()){
        RAISE_EXCEPTION(ConsoleArgumentError, "Long option does not exist");
    }
    return m_long.m_value.c_str();
}


bool ParameterBaseImpl::has_alias(const char* alias) const{
    auto it = find_if(begin(m_aliases), end(m_aliases), [alias](const auto& property){
        return property.m_value == alias;
    });
    return it != end(m_aliases);
}

void ParameterBaseImpl::add_alias(const char* alias, const char* source, int line) {
    if(!has_alias(alias)){
        ParameterProperty<string> property;
        property.set(alias, source, line);
        m_aliases.push_back(property);
    }
}

ParameterBaseImpl* ParameterBase::find_generic_impl(const char* name){
    auto& console_parameters = config().m_console_parameters;
    auto it = find_if(begin(console_parameters), end(console_parameters), [name](auto& p){
        return strcmp(name, p->name()) == 0 || p->has_alias(name);
    });

    if(it == end(console_parameters)){
        return nullptr;
    } else {
        return *it;
    }
}

void ParameterBase::register_generic_impl(ParameterBaseImpl* impl){
    config().m_console_parameters.push_back(impl);
}

ParameterBase::ParameterBase(const char* source, int line): m_source(source), m_line(line) { }
ParameterBase::~ParameterBase() { }

/*****************************************************************************
 *                                                                           *
 *  Booleans                                                                 *
 *                                                                           *
 *****************************************************************************/

template<>
clara::Opt handle_argument<bool>(ParameterImpl<bool>& impl){
    // if there is no, assume 'false' by default
    if(! impl.is_set()){
        impl.set_default(false, __FILE__, __LINE__);
    }

    clara::Opt result([&impl](bool value){
        impl.set(value, __FILE__, __LINE__);
    });

    return result;
}

template<>
string param_to_string(const ParameterImpl<bool>& impl){
    return impl.get()? "true" : "false";
}


} // namespace details

} // namespace configuration


