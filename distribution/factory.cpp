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

#include "factory.hpp"

#include "distribution.hpp"

using namespace std;

namespace distribution {

Factory Factory::singleton;
Factory& factory(){ return Factory::singleton; }


std::unique_ptr<Distribution> Factory::make(const std::string& name){
    auto it = std::find_if(begin(m_distributions), end(m_distributions), [&name](decltype(m_distributions[0])& impl){
        return impl->name() == name;
    });
    if (it == end(m_distributions)){
        RAISE_EXCEPTION(Exception, "Implementation not found: " << name);
    }

    return (*it)->make();
}

void Factory::sort_list() {
    auto comparator = [](const unique_ptr<Factory::InvokerBase>& v1, const unique_ptr<Factory::InvokerBase>& v2){
        return v1->name() < v2->name();
    };
    sort(begin(m_distributions), end(m_distributions), comparator);
}

Factory::InvokerBase::InvokerBase(const string& name, const string& description, const char* source, int line) :
        m_name(name), m_description(description), m_source(source), m_line(line) { }
Factory::InvokerBase::~InvokerBase(){ };
const string& Factory::InvokerBase::name() const{ return m_name; }
const string& Factory::InvokerBase::description() const{ return m_description; }
const char* Factory::InvokerBase::source() const { return m_source; }
int Factory::InvokerBase::line() const{ return m_line; }

} // namespace distribution
