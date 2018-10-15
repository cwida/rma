/*
 * factory.hpp
 *
 *  Created on: 26 Dec 2017
 *      Author: Dean De Leo
 */

#ifndef PMA_FACTORY_HPP
#define PMA_FACTORY_HPP

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "errorhandling.hpp"

namespace pma {

#define REGISTER_PMA(name, description, callable) pma::factory().register_pma_implementation(name, description, callable, __FILE__, __LINE__)
#define REGISTER_EXPERIMENT(name, description, callable) pma::factory().register_experiment(name, description, callable, __FILE__, __LINE__)

// forward decl.
class Experiment;
class Factory;
class Interface;
class InterfaceNR;


Factory& factory();

namespace factory_details {

    class ItemDescription {
    protected:
        std::string m_name;
        std::string m_description;
        const char* m_source;
        int m_line;
        const bool m_is_multicolumnar; // does this instance refer to InterfaceNR (=true) or Interface (=false) ?
        bool m_display_in_help = true; // whether to show this item in the help screen

        ItemDescription(const std::string& name, const std::string& description, const char* source, int line, bool is_multi);

    public:
        const std::string& name() const;

        const std::string& description() const;

        const char* source() const;

        int line() const;

        bool is_multicolumnar() const;

        void set_display(bool value);

        bool is_display() const;

        virtual ~ItemDescription();
    };

    template <typename Interface>
    class FactoryGenericImpl : public ItemDescription {
    protected:
        FactoryGenericImpl(const std::string& name, const std::string& description, const char* source, int line) :
            ItemDescription(name, description, source, line, std::is_same_v<Interface, pma::InterfaceNR>) { }

    public:
        virtual std::unique_ptr<Interface> make() { RAISE_EXCEPTION(Exception, "Not implemented"); }
        virtual std::unique_ptr<Experiment> make(std::shared_ptr<Interface>){ RAISE_EXCEPTION(Exception, "Not implemented"); };
    };

    template <typename Interface, typename Callable>
    class AlgorithmFactoryImpl : public FactoryGenericImpl<Interface> {
    protected:
        Callable m_callable;

    public:
        AlgorithmFactoryImpl(const std::string& name, const std::string& description, Callable callable, const char* source, int line) :
            FactoryGenericImpl<Interface>(name, description, source, line), m_callable(callable) {}

        std::unique_ptr<Interface> make() override{
            return std::invoke(m_callable);
        }
    };

    template <typename Interface, typename Callable>
    class ExperimentFactoryImpl : public FactoryGenericImpl<Interface> {
    protected:
        Callable m_callable;

    public:
        ExperimentFactoryImpl(const std::string& name, const std::string& description, Callable callable, const char* source, int line) :
            FactoryGenericImpl<Interface>(name, description, source, line), m_callable(callable) { }

        std::unique_ptr<Experiment> make(std::shared_ptr<Interface> interface) override{
            return std::invoke(m_callable, interface);
        }
    };
} // namespace factory_details


class Factory {
    friend Factory& factory();
    static Factory singleton;

    using Impl = ::pma::factory_details::ItemDescription;

    std::vector<std::unique_ptr<Impl>> m_pma_implementations;
    std::vector<std::unique_ptr<Impl>> m_experiments;

    template <typename Interface>
    std::unique_ptr<Interface> make_algorithm_generic(const std::string& name);

    template <typename Interface>
    std::unique_ptr<Experiment> make_experiment_generic(const std::string& name, std::shared_ptr<Interface> pma);

    Factory();
public:
    ~Factory();

    const auto& algorithms() const { return m_pma_implementations; }

    const auto& experiments() const { return m_experiments; }

    template<typename Callable>
    void register_pma_implementation(const std::string& name, const std::string& description, Callable callable, const char* source, int line) {
        auto it = std::find_if(begin(m_pma_implementations), end(m_pma_implementations), [name](const std::unique_ptr<Impl>& impl){
            return impl->name() == name && !impl->is_multicolumnar();
        });
        if(it != end(m_pma_implementations)){
            auto& r = *it;
            RAISE_EXCEPTION(Exception, "The data structure '" << name << "' [single] has already been registered from: " << r->source() << ":" << r->line() << ". Attempting to register it again from: " << source << ":" << line);
        }

        m_pma_implementations.push_back(std::unique_ptr<Impl>{ new pma::factory_details::AlgorithmFactoryImpl<Interface, Callable>(name, description, callable, source, line)});
    }

    template<typename Callable>
    void register_experiment(const std::string& name, const std::string& description, Callable callable, const char* source, int line) {
        auto it = std::find_if(begin(m_experiments), end(m_experiments), [name](const std::unique_ptr<Impl>& impl){
            return impl->name() == name && !impl->is_multicolumnar();
        });
        if(it != end(m_experiments)){
            auto& r = *it;
            RAISE_EXCEPTION(Exception, "The experiment '" << name << "' [single] has already been registered from: " << r->source() << ":" << r->line() << ". Attempting to register it again from: " << source << ":" << line);
        }

        m_experiments.push_back(std::unique_ptr<Impl>{
            new pma::factory_details::ExperimentFactoryImpl<Interface, Callable>(name, description, callable, source, line)});
    }

    std::unique_ptr<pma::Interface> make_algorithm(const std::string& name);

    std::unique_ptr<pma::Experiment> make_experiment(const std::string& name, std::shared_ptr<Interface> pma);
};

}
#endif /* PMA_FACTORY_HPP */
