/*
 * errorhandling.hpp
 *
 *  Created on: Jul 14, 2016
 *      Author: Dean De Leo
 */

#ifndef ERRORHANDLING_HPP_
#define ERRORHANDLING_HPP_

#include <ostream>
#include <sstream>
#include <stdexcept>

/**
 * Base class for any errors
 */
class Exception : public std::runtime_error {
private:
    const std::string exceptionClass; // the class of the exception ("Graph::Exception")
    const std::string file; // source file where the exception has been raised
    int line; // related line where the exception has been raised
    const std::string function; // function causing the exception

public:
    /**
     * Constructor
     * @param exceptionClass the name of the exception class
     * @param message the error message associated to this exception
     * @param file the source file where the exception has been generated
     * @param line the line where the exception has been generated
     * @param function the function where this exception has been raised
     */
    Exception(const std::string& exceptionClass, const std::string& message, const std::string& file, int line, const std::string& function);

    /**
     * Retrieve the source file where the exception has been raised
     * @return the source file where the exception has been raised
     */
    std::string getFile() const;

    /**
     * The line number where the exception has been raised
     * @return the line number where the exception has been raised
     */
    int getLine() const;

    /**
     * Retrieve the function that fired the exception
     * @return the function name where the exception has been raised
     */
    std::string getFunction() const;

    /**
     * Retrieves the name of the exception class
     * @return the name of the exception class
     */
    std::string getExceptionClass() const;

    /**
     * Utility class to create the exception message
     */
    static thread_local std::stringstream utilitystream;
};


/**
 * Overload the operator to print the descriptive content of an ELF Exception
 */
std::ostream& operator<<(std::ostream& out, Exception& e);

// In case of changing the inheritance of the exception and the macro is caught from somewhere else
#if !defined(RAISE_EXCEPTION_CREATE_ARGUMENTS)
/**
 * It prepares the arguments `file', `line', `function', and `what' to be passed to an exception ctor
 * @param msg the message stream to concatenate
 */
#define RAISE_EXCEPTION_CREATE_ARGUMENTS(msg) const char* file = __FILE__; int line = __LINE__; const char* function = __FUNCTION__; \
        auto& stream = Exception::utilitystream; \
        stream.str(""); stream.clear(); \
        stream << msg; \
        std::string what = stream.str(); \
        stream.str(""); stream.clear() /* reset once again */
#endif

/**
 * Raises an exception with the given message
 * @param exception the exception to throw
 * @param msg: an implicit ostream, with arguments concatenated with the symbol <<
 */

#define RAISE_EXCEPTION(exc, msg) { RAISE_EXCEPTION_CREATE_ARGUMENTS(msg); throw exc( #exc, what, file, line, function); }


/**
 * These exception classes are so similar, so define a general macro to create the exception
 */
#define DEFINE_EXCEPTION( exceptionName ) class exceptionName: public Exception { \
        public: exceptionName(const std::string& exceptionClass, const std::string& message, const std::string& file, \
	        int line, const std::string& function) : \
	            Exception(exceptionClass, message, file, line, function) { } \
} /* End of DEFINE_EXCEPTION */

#endif /* ERRORHANDLING_HPP_ */
