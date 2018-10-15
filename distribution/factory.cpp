/*
 * factory.cpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
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
