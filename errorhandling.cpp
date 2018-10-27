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
