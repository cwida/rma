/*
 * errorhandling.cpp
 *
 *  Created on: Jul 14, 2016
 *      Author: Dean De Leo
 */

#include "errorhandling.hpp"

Exception::Exception(const std::string& exceptionClass_, const std::string& message_, const std::string& file_, int line_, const std::string& function_)
    : runtime_error(message_), exceptionClass(exceptionClass_), file(file_), line(line_), function(function_){ }

std::string Exception::getFile() const{ return file; }
int Exception::getLine() const{ return line; }
std::string Exception::getFunction() const{ return function; }
std::string Exception::getExceptionClass() const { return exceptionClass; }

// Definition of the utility stream
thread_local std::stringstream Exception::utilitystream;

std::ostream& operator<<(std::ostream& out, Exception& e){
    out << "[" << e.getExceptionClass() << ": " << e.what() << " - Raised at: `" << e.getFile() << "', "
            "line: " << e.getLine() << ", function: `" << e.getFunction() << "']";
    return out;
}
