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

#ifndef CONSOLE_ARGUMENTS_HPP_
#define CONSOLE_ARGUMENTS_HPP_

#include <functional> // invoke
#include <iostream> // debug
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "errorhandling.hpp"

#if !defined(CLARA_CONFIG_CONSOLE_WIDTH) // avoid to redefine it, if already defined (by catch)
#define CLARA_CONFIG_CONSOLE_WIDTH 120
#endif
#include "third-party/clara/clara.hpp"

/**
 * Declare and retrieve a console argument, hereafter a parameter, that can be used by a user
 * to set options through the command line.
 */
#define PARAMETER(type, name) configuration::parameter<type>(name, __FILE__, __LINE__)
#define ARGREF(type, name) configuration::argument_ref<type>(name, __FILE__, __LINE__)

namespace configuration {

// Forward declaration
class Configuration;

DEFINE_EXCEPTION(ConsoleArgumentError);

namespace details {

// Whether a parameter is still unset, it has been explicitly set, or it retains a default value
enum class State { UNSET, SET, DEFAULT };

/**
 * A property is a mere container of a typed value, together with the location __FILE__ / __LINE__ where
 * the value has been set and whether it has been set.
 */
template<typename T>
struct ParameterProperty {
    T m_value; // the actual value contained
    State m_state = State::UNSET; // whether this property has been explicitly set
    const char* m_source = nullptr; // the source file where the property has been set
    int m_line = -1; // the source line where the property has been set

    // Create an `unset' property
    ParameterProperty(){ }

    // Create a property with a given default value
    ParameterProperty(const T& value): m_value(value), m_state(State::DEFAULT){ } // default value
    void set(const T& value, const char* source, int line) {
        m_value = value;
        m_state = State::SET;
        m_source = source;
        m_line = line;
    }
};

/**
 * Base class for the registered parameters. Derived classes set the actual type and content of the
 * parameter.
 */
class ParameterBaseImpl {
protected:
    ParameterProperty<const char*> m_name; // the parameter name, used as identifier to search for a parameter and also the id stored in the results database
    ParameterProperty<std::string> m_hint{"value"}; // the tag is showed in the `help prompt'
    ParameterProperty<char> m_short; // short option (if given), e.g. -e
    ParameterProperty<std::string> m_long; // long option, e.g. --experiment
    ParameterProperty<std::string> m_description; // the description showed in the `help prompt'
    ParameterProperty<bool> m_required; // the parameter must be set
    ParameterProperty<bool> m_record_in_database{ true }; // store in the database?
    std::vector<ParameterProperty<std::string>> m_aliases; // aliases for the code

public:

    /**
     * Create the option for clara
     */
    virtual clara::Opt generate_option() = 0;

    /**
     * Instantiate the base class
     */
    ParameterBaseImpl(const char* name, const char* source, int line);

    /**
     * Default destructor.
     */
    virtual ~ParameterBaseImpl();

    /**
     * The name / identifier of this parameter. Used to refer the parameter from the source code
     * and stored in the database
     */
    const char* name() const;

    /**
     * The source file where this parameter has been created
     */
    const char* source() const;

    /**
     * The line where this parameter has been created
     */
    int line() const;

    /**
     * The hint associated to this parameter.
     */
    const std::string& hint() const;

    /**
     * The description associated to this parameter.
     */
    const std::string& description() const;

    /***
     * Set the tag for this parameter. The tag is the identifier showed in the `help prompt'
     *  after the short/long option. For instance the tag 'value' will be showed as
     *  -e --experiment <value>. Setting a tag implies that this parameter requires an associated
     *  value.
     */
    void set_hint(const std::string& value, const char* source, int line);

    /**
     * Set the short option, single character, e.g. 'l' for the short option -l
     */
    void set_short(char value, const char* source, int line);

    /**
     * Set the long option, a string, e.g. "long" for the long option --long
     */
    void set_long(const char* value, const char* source, int line);

    /**
     * Set the description associated to this parameter. This is showed in the `help prompt'
     */
    void set_description(const std::string& value, const char* source, int line);

    /**
     * Set the parameter as mandatory/required.
     */
    void set_required(const char* source, int line);

    /**
     * Set whether to record the property, when set, in the table parameters of the database
     */
    void set_record_in_database(bool value, const char* source, int line);

    /**
     * Check whether this parameter is set, either explicitly or by default.
     */
    virtual bool is_set() const = 0;

    /**
     * Check whether this parameter retains the default value.
     */
    virtual bool is_default() const = 0;

    /**
     * Check whether this parameter is required/mandatory.
     */
    bool is_required() const;

    /**
     * Check whether this parameter needs to be store in the table parameters of the database
     */
    bool is_recorded() const;

    /**
     * Check whether this parameter has a short option attached to it
     */
    bool has_short_option() const;

    /**
     * Check whether this parameter has a long option attached to it
     */
    bool has_long_option() const;

    /**
     * Check whether a hint has been set for this parameter.
     */
    bool has_hint() const;

    /**
     * Get the short option attached to this parameter, or raise an
     * exception if a short option doesn't exist
     */
    char get_short() const;

    /**
     * Get the long option attached to this parameter, or raise an
     * exception if a long option doesn't exist
     */
    const char* get_long() const;

    /**
     * Get a string representation of the current value
     */
    virtual std::string to_string() const = 0;

    /**
     * Check whether this parameter has also the given code alias
     */
    bool has_alias(const char* alias) const;

    /**
     * Register an additional alias for the source code
     */
    void add_alias(const char* alias, const char* source, int line);
};

// Forward declarations
template<typename T>
class ParameterImpl;
template<typename T>
clara::Opt handle_argument(ParameterImpl<T>& impl);
template<typename T>
std::string param_to_string(const ParameterImpl<T>& impl);

template<typename T>
struct ParameterValidateBase {
    virtual bool validate(const T& value) = 0;
    virtual ~ParameterValidateBase() { };
};

template<typename T, typename Callable>
struct ParameterValidate : public ParameterValidateBase<T> {
    Callable callable;

    ParameterValidate(const Callable& callable) : callable(callable) { }

    bool validate(const T& value) override{
        return std::invoke(callable, value);
    }
};


/**
 * Container of the typed value of a parameter.
 */
template<typename T>
class ParameterImpl : public ParameterBaseImpl {
    friend clara::Opt handle_argument<T>(ParameterImpl<T>& impl);
    friend std::string param_to_string<T>(const ParameterImpl<T>& impl);
protected:

    /**
     * The actual value contained
     */
    ParameterProperty<T> m_value;

    /**
     * Validate the value
     */
    std::unique_ptr<ParameterValidateBase<T>> m_validate;
public:

    /**
     * Create a new parameter with the given `name'
     */
    ParameterImpl<T>(const char* name, const char* source, int line) : ParameterBaseImpl(name, source, line)  { }

    ~ParameterImpl<T>() { }

    /**
     * Retrieve the actual value associated to this parameter
     */
    const T& get() const {
        return m_value.m_value;
    }

    /**
     * Set the default value associated to this parameter.
     */
    void set_default(const T& value, const char* source, int line){
        if(is_set()){
            RAISE_EXCEPTION(ConsoleArgumentError, "Argument already set to `" << m_value.m_value << "' from " << m_value.m_source << ":" << m_value.m_line);
        }
        if(m_validate && !m_validate->validate(value)){
            RAISE_EXCEPTION(ConsoleArgumentError, "Invalid value: " << value);
        }

        m_value.set(value, source, line);
        m_value.m_state = State::DEFAULT;
    }

    /**
     * Set the actual value for this parameter
     */
    void set(const T& value, const char* source, int line){
        if(m_validate && !m_validate->validate(value)){
            std::stringstream ss;
            ss << "Invalid value for the argument";
            if(has_short_option()) ss << " -" << get_short();
            if(has_long_option()) ss << " --" << get_long();
            ss << ": " << value;
            RAISE_EXCEPTION(ConsoleArgumentError, ss.str());
        }
        m_value.set(value, source, line);
    }

    bool is_set() const override { return m_value.m_state != State::UNSET; }
    bool is_default() const override { return m_value.m_state == State::DEFAULT; }

    clara::Opt generate_option() override {
        return handle_argument<T>(*this);
    }

    std::string to_string() const override {
        return param_to_string<T>(*this);
    }

    template<typename Callable>
    void set_validate_fn(Callable fn){
        m_validate.reset(new ParameterValidate<T, Callable>(fn));
    }
};

template<typename T>
std::string param_to_string(const ParameterImpl<T>& impl){
    std::stringstream ss;
    ss << impl.get();
    return ss.str();
}

template<typename T>
clara::Opt handle_argument(ParameterImpl<T>& impl){
    if(!impl.has_hint()){
        RAISE_EXCEPTION(ConsoleArgumentError, "Missing hint for the parameter: " << impl.name());
    }

    return clara::Opt([&impl](const T& value){
        impl.set(value, __FILE__, __LINE__);
    }, impl.hint());
}

/**
 * Friend class for the Configuration and base class for the argument wrappers.
 */
class ParameterBase {
protected:
    const char* m_source; // The source file where this wrapper has been instantiated
    const int m_line; // The source line where this wrapper has been instantiated

    /**
     * Instantiate the object keeping track of the source file (__FILE__) and line (__LINE__)
     */
    ParameterBase(const char* source, int line);

    /**
     * Default destructor
     */
    virtual ~ParameterBase();

    /**
     * Find the parameter implementation for the parameter with the given `name'. It returns
     * NULL if the parameter has not been registered.
     */
    static ParameterBaseImpl* find_generic_impl(const char* name);

    /**
     * Register a parameter to the configuration
     */
    static void register_generic_impl(ParameterBaseImpl* impl);
};

/**
 * Read-only reference to a registered parameter. It assumes the parameter has already been
 * registered elsewhere.
 */
template <typename T>
class TypedParameterRef : public ParameterBase {
protected:
    ParameterImpl<T>* m_impl = nullptr; // Base class for the implementation

    TypedParameterRef<T>(ParameterBaseImpl* impl, const char* name, const char* source, int line) :
            ParameterBase(source, line), m_impl(dynamic_cast<ParameterImpl<T>*>(impl)){
        if(m_impl == nullptr){
            if(impl == nullptr){
                RAISE_EXCEPTION(ConsoleArgumentError, "The parameter `" << name << "' does not exist");
            } else {
                RAISE_EXCEPTION(ConsoleArgumentError, "Invalid type for the parameter: " << name);
            }
        }
    }

public:
    /**
     * Instantiante the wrapper for the given parameter. It raises an error, ConsoleArgumentError, if the
     * parameter has not been registered
     */
    TypedParameterRef<T>(const char* name, const char* source, int line) :
        TypedParameterRef<T>(find_generic_impl(name), name, source, line) { }

    /**
     * Retrieve the name of this parameter
     */
    const char* name() const {
        return m_impl->name();
    }

    /**
     * Retrieve the value of this parameter, or raises an error if the parameter has not been set yet.
     */
    T get() const {
         if(!m_impl->is_set()){
             RAISE_EXCEPTION(ConsoleArgumentError, "Argument " << name() << " not set. Requested at: " << m_source << ":" << m_line);
         }
         return m_impl->get();
    }


    operator T() const{ return get(); }

    /**
     * If this parameter has been set, retrieve the value of this parameter and store in the given
     * variable, then return `true'. Otherwise, return `false'.
     */
    bool get(T& variable) const {
        if(m_impl->is_set()){
            variable = m_impl->get();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check whether this parameter still retains a default value
     */
    bool is_default() const { return m_impl->is_default(); }

    /**
     * Check whether this parameter contains a value, either by a default value or explicitly by the user
     */
    bool is_set() const { return m_impl->is_set(); }
};

/**
 * A wrapper to a registered parameter / console argument.
 */
template <typename T>
class TypedParameter : public TypedParameterRef<T> {
protected:
    /**
     * Helper, get or register the parameter with the given name
     */
    static ParameterImpl<T>* ref(const char* name, const char* source, int line){
        ParameterBaseImpl* base_impl = ParameterBase::find_generic_impl(name);
        ParameterImpl<T>* impl = nullptr;
        if(base_impl != nullptr){
            impl = dynamic_cast<ParameterImpl<T>*>(base_impl);
            if(impl == nullptr){
                RAISE_EXCEPTION(ConsoleArgumentError, "Type mismatch for the argument " << base_impl->name() << ", as registered from: " << base_impl->source() << ":" << base_impl->line());
            }
        } else { // parameter not found
            impl = new ParameterImpl<T>(name, source, line);
            ParameterBase::register_generic_impl(impl);
        }
        return impl;
    }

public:
    /**
     * Instantiate the wrapper with the given parameter name
     */
    TypedParameter<T>(const char* name, const char* source, int line) : TypedParameterRef<T>(ref(name, source, line), name, source, line) { }

    /**
     * Set the tag associated to this parameter. The `tag' is the identifier showed in `help prompt'
     * after the short/long options. If the `tag' is set, it implies that a value is required for
     * this parameter.
     */
    decltype(auto) hint(const std::string& value) {
        TypedParameter<T>::m_impl->set_hint(value, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set as hint the keyword `value'.
     */
    decltype(auto) hint(){
        return hint("value");
    }

    /**
     * Set the argument as required.
     */
    decltype(auto) required(){
        TypedParameter<T>::m_impl->set_required(TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set the `short option' for this parameter.
     */
    decltype(auto) option_short(char value){
        TypedParameter<T>::m_impl->set_short(value, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set the `long option' associated to this parameter.
     */
    decltype(auto) option_long(const char* value) {
        TypedParameter<T>::m_impl->set_long(value, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set short/long option, similar interface to clara
     */
    decltype(auto) operator[](const char* value){
        if(value != nullptr && value[0] != '\0' && value[1] == '\0'){
            return option_short(value[0]);
        } else {
            return option_long(value);
        }
    }

    /**
     * Set both the short & long option associated to this parameter.
     */
    decltype(auto) interface(char option_short, const char* option_long){
        TypedParameter<T>::option_short(option_short);
        TypedParameter<T>::option_long(option_long);
        return *this;
    }

    /**
     * Set the short option, the long option and the tag associated to this parameter.
     */
    decltype(auto) interface(char option_short, const char* option_long, const std::string& hint){
        interface(option_short, option_long);
        TypedParameter<T>::hint(hint);
        return *this;
    }

    /**
     * Set the description associated to this parameter. The description is showed in the `help prompt'.
     */
    decltype(auto) description(const std::string& help){
        TypedParameter<T>::m_impl->set_description(help, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }
    decltype(auto) descr(const std::string& help){ return description(help); } // alias

    /**
     * Whether to record the property in the database. By default true.
     */
    decltype(auto) record(bool value = true){
        TypedParameter<T>::m_impl->set_record_in_database(value, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set the default value of this parameter.
     */
    decltype(auto) set_default(const T& value){
        TypedParameter<T>::m_impl->set_default(value, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set the actual value of this parameter.
     */
    decltype(auto) set_forced(const T& value){
        TypedParameter<T>::m_impl->set(value, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Register an alias for this parameter.
     * Aliases are used to refer to the parameter with an alternative name from the source code.
     */
    decltype(auto) alias(const char* name){
        auto param = TypedParameter<T>::find_generic_impl(name);
        if(param != nullptr){
            RAISE_EXCEPTION(ConsoleArgumentError, "Alias `" << name << "' already in use for the parameter: " << param->name() <<
                    ", as set from: " << param->source() << ":" << param->line() << ". Attempting to reset from: " <<
                    TypedParameter<T>::m_source << ":" << TypedParameter<T>::m_line);
        }
        TypedParameter<T>::m_impl->add_alias(name, TypedParameter<T>::m_source, TypedParameter<T>::m_line);
        return *this;
    }

    /**
     * Set the validator function
     */
    template <typename Callable>
    decltype(auto) validate_fn(Callable callable){
        TypedParameter<T>::m_impl->set_validate_fn(callable);
        return *this;
    }
};

} // namespace details


/**
 * Get a read-write wrapper for the given parameter
 */
template<typename T>
configuration::details::TypedParameter<T> parameter(const char* name, const char* source, int line){
    return configuration::details::TypedParameter<T>(name, source, line);
}

/**
 * Get a read-only wrapper for the given parameter.
 */
template<typename T>
configuration::details::TypedParameterRef<T> argument_ref(const char* name, const char* source, int line){
    return configuration::details::TypedParameterRef<T>(name, source, line);
}

} // namespace configuration

#endif /* CONSOLE_ARGUMENTS_HPP_ */
