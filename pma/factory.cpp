/*
 * factory.cpp
 *
 *  Created on: 26 Dec 2017
 *      Author: Dean De Leo
 */

#include "factory.hpp"

#include <algorithm>
#include <string>

#include "console_arguments.hpp"
#include "driver.hpp"
#include "experiment.hpp"
#include "interface.hpp"

using namespace pma::factory_details;
using namespace std;

namespace pma {

Factory Factory::singleton;
Factory& factory(){ return Factory::singleton; }

Factory::Factory(){ }
Factory::~Factory() { }


template <typename Interface>
unique_ptr<Interface> Factory::make_algorithm_generic(const string& name){
    auto it = std::find_if(begin(m_pma_implementations), end(m_pma_implementations), [&name](decltype(m_pma_implementations[0])& impl){
        return impl->name() == name && (impl->is_multicolumnar() == std::is_same_v<Interface, pma::InterfaceNR>);
    });
    if (it == end(m_pma_implementations)){
        RAISE_EXCEPTION(Exception, "Implementation not found: " << name);
    }

    auto cast = dynamic_cast<FactoryGenericImpl<Interface>*>((*it).get());
    if(cast != nullptr){
        return cast->make();
    } else {
        RAISE_EXCEPTION(Exception, "The implementation " << name << " cannot be initialised for the given configuration (single/multi columnar)");
    }
}


unique_ptr<Interface> Factory::make_algorithm(const string& name){
    return make_algorithm_generic<Interface>(name);
}


template <typename Interface>
std::unique_ptr<Experiment> Factory::make_experiment_generic(const string& name, shared_ptr<Interface> pmae){
    auto it = std::find_if(begin(m_experiments), end(m_experiments), [&name](decltype(m_experiments[0])& impl){
        return impl->name() == name && (impl->is_multicolumnar() == std::is_same_v<Interface, pma::InterfaceNR>);
    });
    if (it == end(m_experiments)){
        RAISE_EXCEPTION(Exception, "Experiment not found: " << name);
    }

    auto cast = dynamic_cast<FactoryGenericImpl<Interface>*>((*it).get());
    if(cast != nullptr){
        return cast->make(pmae);
    } else {
        RAISE_EXCEPTION(Exception, "The experiment " << name << " cannot be used initialised for the given data structure");
    }
}

unique_ptr<Experiment> Factory::make_experiment(const string& name, shared_ptr<Interface> pma){
    return make_experiment_generic(name, pma);
}

namespace factory_details {
ItemDescription::ItemDescription(const string& name, const string& description, const char* source, int line, bool is_multi) :
        m_name(name), m_description(description), m_source(source), m_line(line), m_is_multicolumnar(is_multi) { }
ItemDescription::~ItemDescription(){ };
const string& ItemDescription::name() const{ return m_name; }
const string& ItemDescription::description() const{ return m_description; }
const char* ItemDescription::source() const { return m_source; }
int ItemDescription::line() const{ return m_line; }
bool ItemDescription::is_multicolumnar() const { return m_is_multicolumnar; }
void ItemDescription::set_display(bool value){ m_display_in_help = value; }
bool ItemDescription::is_display() const { return m_display_in_help; }

} // namespace factory_details

} // namespace pma

