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

#include "database.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <locale>
//#include <limits>
//#include <random>
#include <sstream>
#include <string>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "miscellaneous.hpp"
#include "third-party/sqlite3/sqlite3.h"

using namespace std;

#define ERROR(msg) RAISE_EXCEPTION(DatabaseException, msg)

namespace database {

#define conn reinterpret_cast<sqlite3*>(m_connection_handle)

Database::Database() {
    { // Database path
        const string& db_path = ARGREF(string, "database");
        m_database_path = (const char*) calloc(sizeof(char), db_path.size() + 1);
        strcpy(const_cast<char*>(m_database_path), db_path.c_str());
    }

    int rc (0);
    char* errmsg = nullptr;

    { // Initialise the connection to the database
        LOG_VERBOSE("Connecting to `" << m_database_path << "' ...");
        sqlite3* connection(nullptr);
        rc = sqlite3_open(m_database_path, &connection);
        if(rc != SQLITE_OK) { ERROR("Cannot open a SQLite connection to `" << m_database_path << "'"); }
        assert(connection != nullptr);
        m_connection_handle = connection;
    }

    // Start the transaction
    rc = sqlite3_exec(conn, "BEGIN TRANSACTION", nullptr, nullptr, &errmsg);
    if(rc != SQLITE_OK || errmsg != nullptr){
        string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
        ERROR("Cannot start a transaction: " << error);
    }

    // Create the table executions (if it does not already exist)
    LOG_VERBOSE("Recording the execution ...");
    auto SQL_create_table_executions = ""
            "CREATE TABLE IF NOT EXISTS executions ("
            "   id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
            "   timeStart TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "   timeEnd TIMESTAMP, "
            "   experiment TEXT NOT NULL, "
            "   algorithm TEXT NOT NULL "
            ");";
    rc = sqlite3_exec(conn, SQL_create_table_executions, nullptr, nullptr, &errmsg);
    if(rc != SQLITE_OK || errmsg != nullptr){
        string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
        ERROR("Cannot create the table `Executions': " << error);
    }

    // Append a record for the current execution
    sqlite3_stmt* stmt (nullptr);
    auto SQL_insert_execution =
            "INSERT INTO executions (algorithm, experiment) "
            "VALUES (?1, ?2);";
    rc = sqlite3_prepare_v2(conn, SQL_insert_execution, -1, &stmt, nullptr);
    if(rc != SQLITE_OK || stmt == nullptr){
        ERROR("Cannot prepare the statement to insert an execution: " << sqlite3_errstr(rc));
    }
    auto algorithm_str = ARGREF(string, "algorithm").get();
    rc = sqlite3_bind_text(stmt, 1, algorithm_str.c_str(), /* compute length with strlen */ -1, /* do not free */ SQLITE_STATIC);
    if(rc != SQLITE_OK){ ERROR("SQL Insert -> Executions: cannot bind the parameter #1: " << sqlite3_errstr(rc)); }
    auto experiment_str = ARGREF(string, "experiment").get();
    rc = sqlite3_bind_text(stmt, 2, experiment_str.c_str(), /* compute length with strlen */ -1, /* do not free */ SQLITE_STATIC);
    if(rc != SQLITE_OK){ ERROR("SQL Insert -> Executions: cannot bind the parameter #2: " << sqlite3_errstr(rc)); }

    rc = sqlite3_step(stmt);
    if(rc != SQLITE_DONE){ ERROR("SQL Insert -> Executions: cannot insert the values: " << sqlite3_errstr(rc)); }
    rc = sqlite3_finalize(stmt); stmt = nullptr;
    assert(rc == SQLITE_OK);

    // execution id
    m_exec_id = sqlite3_last_insert_rowid(conn);

    // parameters
    LOG_VERBOSE("Recording the parameters ...");
    auto SQL_create_table_parameters = ""
            "CREATE TABLE IF NOT EXISTS parameters ("
            "   exec_id INTEGER NOT NULL, "
            "   name TEXT NOT NULL, "
            "   value TEXT NOT NULL, "
            "   PRIMARY KEY(exec_id, name), "
            "   FOREIGN KEY(exec_id) REFERENCES executions ON DELETE CASCADE ON UPDATE CASCADE"
            ");";
    rc = sqlite3_exec(conn, SQL_create_table_parameters, nullptr, nullptr, &errmsg);
    if(rc != SQLITE_OK || errmsg != nullptr){
        string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
        ERROR("Cannot create the table `parameters': " << error);
    }
    stmt = nullptr;
    auto SQL_insert_parameters = "INSERT INTO parameters (exec_id, name, value) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(conn, SQL_insert_parameters, -1, &stmt, nullptr);
    if(rc != SQLITE_OK || stmt == nullptr)
        ERROR("Cannot prepare the statement to insert the execution parameters: " << sqlite3_errstr(rc));
    auto& parameters = config().m_console_parameters;
    sort(begin(parameters), end(parameters), [](auto p1, auto p2){
        string v1 = p1->name();
        string v2 = p2->name();
        return v1 < v2;
    });

    for(auto p : parameters){
        if(!p->is_recorded() || !p->is_set()) continue;
        rc = sqlite3_reset(stmt);
        if(rc != SQLITE_OK){ ERROR("SQL Insert -> Parameter [" << p->name() << "]: cannot reset the statement: " << sqlite3_errstr(rc)); }
        rc = sqlite3_bind_int64(stmt, 1, m_exec_id);
        if(rc != SQLITE_OK){ ERROR("SQL Insert -> Parameter [" << p->name() << "]: cannot bind the parameter #1: " << sqlite3_errstr(rc)); }
        rc = sqlite3_bind_text(stmt, 2, p->name(), -1, SQLITE_STATIC);
        if(rc != SQLITE_OK){ ERROR("SQL Insert -> Parameters [" << p->name() << "]: " << sqlite3_errstr(rc)); }
        string value = p->to_string(); // p->to_string().c_str() would refer to a potentially dead temporary
        rc = sqlite3_bind_text(stmt, 3, value.c_str(), -1, SQLITE_STATIC);
        if(rc != SQLITE_OK){ ERROR("SQL Insert -> Parameter [" << p->name() << "]: cannot bind the parameter #3: " << sqlite3_errstr(rc)); }
        rc = sqlite3_step(stmt);
        if(rc != SQLITE_DONE){ ERROR("SQL Insert -> Parameters: cannot insert the values for the parameter " << p->name() << ": " << sqlite3_errstr(rc)); }
    }
    rc = sqlite3_finalize(stmt); stmt = nullptr;
    assert(rc == SQLITE_OK);

    // Complete the transaction
    rc = sqlite3_exec(conn, "COMMIT", nullptr, nullptr, &errmsg);
    if(rc != SQLITE_OK || errmsg != nullptr){
        string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
        ERROR("Cannot complete a transaction: " << error);
    }

//    rc = sqlite3_close(conn);
//    assert(rc == SQLITE_OK);

    LOG_VERBOSE("Database initialised");
}

Database::~Database() {
    int rc (0);
    sqlite3_stmt* stmt (nullptr);
    auto SQL_update_execution = "UPDATE executions SET timeEnd = CURRENT_TIMESTAMP WHERE id = ?";

    if(conn == nullptr) { // there is no connection active
        goto next;
    }

    // Check there is no transaction active
    rc = sqlite3_get_autocommit(conn);
    if(rc == 0){ // Ignore the result, just attempt to rollback at this point
        sqlite3_exec(conn, "ROLLBACK", nullptr, nullptr, nullptr);
    }

    rc = sqlite3_prepare_v2(conn, SQL_update_execution, -1, &stmt, nullptr);
    if(rc != SQLITE_OK || stmt == nullptr){
        cerr << "[SaveResults::dtor] ERROR: Cannot prepare the statement to insert an execution: " << sqlite3_errstr(rc) << endl;
        goto next;
    }
    rc = sqlite3_bind_int64(stmt, 1, id());
    if(rc != SQLITE_OK){
        cerr << "[SaveResults::dtor] ERROR: SQL Insert -> Executions: cannot bind the parameter #1: " << sqlite3_errstr(rc) << endl;
        goto next;
    }
    rc = sqlite3_step(stmt);
    if(rc != SQLITE_DONE){
        cerr << "[SaveResults::dtor] ERROR: SQL Insert -> Executions: cannot insert the values: " << sqlite3_errstr(rc) << endl;
        goto next;
    }
    rc = sqlite3_finalize(stmt); stmt = nullptr;
    assert(rc == SQLITE_OK);

next:
    if(conn != nullptr){
        sqlite3_close(conn);
        m_connection_handle = nullptr;
    }

    free(const_cast<char*>(m_database_path)); m_database_path = nullptr;
}

/*****************************************************************************
 *                                                                           *
 *  Store the experiment results                                             *
 *                                                                           *
 *****************************************************************************/
Database::AbstractField::AbstractField(const string& key, FieldType type) : key(key), type(type) {
    // check the key is not id or exec_id
    locale loc;
    stringstream lstrb;
    for(auto e : key){ lstrb << tolower(e, loc); }
    string lstr = lstrb.str();
    if(lstr == "id" || lstr == "exec_id"){
        ERROR("[AbstractField::ctor] Invalid attribute name: `" << key << "'. This name is reserved.");
    }
}

Database::AbstractField::~AbstractField() { }
Database::TextField::TextField(const string& key, const string& value) : AbstractField{ key, TYPE_TEXT }, value(value) { }
Database::IntegerField::IntegerField(const string& key, int64_t value) : AbstractField{ key, TYPE_INTEGER }, value(value) { }
Database::RealField::RealField(const string& key, double value) : AbstractField{ key, TYPE_REAL }, value(value) { }
Database::ResultsBuilder::ResultsBuilder(Database* instance, const std::string& tableName) : instance(instance), tableName(tableName) {
    if(instance == nullptr)
        ERROR("[ResultsBuilder::ctor] Null pointer for the Database instance parameter");
}
Database::ResultsBuilder::~ResultsBuilder() { save(); }
Database::ResultsBuilder& Database::ResultsBuilder::operator()(const string& key, const string& value){
    unique_ptr<AbstractField> ptr(new TextField(key, value));
    fields.push_back(move(ptr));
    return *this;
}
Database::ResultsBuilder& Database::ResultsBuilder::operator()(const string& key, int64_t value){
    unique_ptr<AbstractField> ptr(new IntegerField(key, value));
    fields.push_back(move(ptr));
    return *this;
}
Database::ResultsBuilder& Database::ResultsBuilder::operator()(const string& key, double value){
    unique_ptr<AbstractField> ptr(new RealField(key, value));
    fields.push_back(move(ptr));
    return *this;
}
void Database::ResultsBuilder::save() {
    sqlite3* connection = reinterpret_cast<sqlite3*>(instance->m_connection_handle);
    if(connection == nullptr) {
        ERROR("No active connection to `" << instance->db_path() << "'");
    }
    try {
        save(connection);
    } catch (DatabaseException& e){
        cerr << "[Database::ResultsBuilder::save] " << *this << endl;
        throw; // propagate the exception
    }
    // Do not close the connection
}
void Database::ResultsBuilder::save(void* handle) {
    assert(handle != nullptr);

    int rc = 0;
    sqlite3* connection = reinterpret_cast<sqlite3*>(handle);
    char* errmsg = nullptr;

    // Start the transaction
    rc = sqlite3_exec(connection, "BEGIN TRANSACTION", nullptr, nullptr, &errmsg);
    if(rc != SQLITE_OK || errmsg != nullptr){
        string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
        ERROR("Cannot start the transaction: " << error);
    }

    { // first check whether the table exists
        const char* SQL_check_table_exists = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;";
        sqlite3_stmt* stmt (nullptr);
        rc = sqlite3_prepare_v2(connection, SQL_check_table_exists, -1, &stmt, nullptr);
        if(rc != SQLITE_OK || stmt == nullptr)
            ERROR("Cannot prepare the statement to insert an execution: " << sqlite3_errstr(rc));
        rc = sqlite3_bind_text(stmt, 1, tableName.c_str(), /* compute length with strlen */ -1, /* do not free */ SQLITE_STATIC);
        if(rc != SQLITE_OK){ ERROR("Cannot bind the parameter: " << sqlite3_errstr(rc)); }
        rc = sqlite3_step(stmt);
        if(rc != SQLITE_DONE && rc != SQLITE_ROW){
            ERROR("Cannot execute the statement: " << sqlite3_errstr(rc));
        }
        bool table_exists = rc != SQLITE_DONE;
        rc = sqlite3_finalize(stmt); stmt = nullptr;
        assert(rc == SQLITE_OK);

        // the table `tableName' does not exist
        if(!table_exists){
            stringstream sqlcc;
            sqlcc << "CREATE TABLE " << tableName << "( ";
            sqlcc << "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, ";
            sqlcc << "exec_id INTEGER NOT NULL, ";
            for(auto& e : fields){
                sqlcc << e->key;
                switch(e->type){
                case TYPE_TEXT:
                    sqlcc << " TEXT NOT NULL, "; break;
                case TYPE_INTEGER:
                    sqlcc << " INTEGER NOT NULL, "; break;
                case TYPE_REAL:
                    sqlcc << " REAL NOT NULL, "; break;
                default:
                    ERROR("[Database::ResultsBuilder::save] Invalid type: " << (int) e->type);
                }
            }
            sqlcc << "FOREIGN KEY(exec_id) REFERENCES executions ON DELETE CASCADE ON UPDATE CASCADE";
            sqlcc << ")";
            auto SQL_create_table = sqlcc.str();
            rc = sqlite3_exec(connection, SQL_create_table.c_str(), nullptr, nullptr, &errmsg);
            if(rc != SQLITE_OK || errmsg != nullptr){
                string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
                ERROR("Cannot create the table `" << tableName << "': " << error);
            }
        }
    } // does the table exist?

    { // Insert the results
        stringstream sqlcc;
        sqlcc << "INSERT INTO " << tableName << " ( exec_id";
        for(size_t i = 0; i < fields.size(); i++ ){
            sqlcc << ", " << fields[i]->key;
        }
        sqlcc << " ) VALUES ( ?";
        for(size_t i = 0; i < fields.size(); i++){
            sqlcc << ", ?";
        }
        sqlcc << ")";
        auto sqlccs = sqlcc.str();

        sqlite3_stmt* stmt (nullptr);
        rc = sqlite3_prepare_v2(connection, sqlccs.c_str(), -1, &stmt, nullptr);
        if(rc != SQLITE_OK || stmt == nullptr){
            ERROR("Cannot prepare the statement to insert a result: " << sqlite3_errstr(rc));
        }
        rc = sqlite3_bind_int64(stmt, 1, instance->id());
        if(rc != SQLITE_OK){ ERROR("SQL Insert -> Results: cannot bind the parameter #1: " << sqlite3_errstr(rc)); }
        int index = 2;
        for(auto& e : fields){
            switch(e->type){
            case TYPE_TEXT:
                rc = sqlite3_bind_text(stmt, index, dynamic_cast<TextField*>(e.get())->value.c_str(), /* compute length with strlen */ -1, /* do not free */ SQLITE_STATIC);
                break;
            case TYPE_INTEGER:
                rc = sqlite3_bind_int64(stmt, index, dynamic_cast<IntegerField*>(e.get())->value);
                break;
            case TYPE_REAL:
                rc = sqlite3_bind_double(stmt, index, dynamic_cast<RealField*>(e.get())->value);
                break;
            default:
                ERROR("[Database::ResultsBuilder::save] Invalid type: " << (int) e->type);
            }
            if(rc != SQLITE_OK){ ERROR("SQL Insert -> Results: cannot bind the parameter " << index << ": " << sqlite3_errstr(rc)); }
            index++;
        }
        rc = sqlite3_step(stmt);
        if(rc != SQLITE_DONE){ ERROR("SQL Insert -> Results: cannot insert the values: " << sqlite3_errstr(rc) << ". SQL Statement: " << sqlccs); }
        rc = sqlite3_finalize(stmt); stmt = nullptr;
        assert(rc == SQLITE_OK);
    }

    // Close the transaction
    rc = sqlite3_exec(connection, "COMMIT", nullptr, nullptr, &errmsg);
    if(rc != SQLITE_OK || errmsg != nullptr){
        string error = errmsg; sqlite3_free(errmsg); errmsg = nullptr;
        ERROR("Cannot commit the transaction: " << error);
    }
}


Database::ResultsBuilder Database::add(const std::string& tableName){
    return ResultsBuilder(this, tableName);
}

void Database::ResultsBuilder::dump(ostream& out) const {
    out << "table: " << tableName << ", # fields: " << fields.size() << "\n";
    for(size_t i = 0; i < fields.size(); i++){
        auto& e = fields[i];
        out << "[" << (i+1) << "] name: " << e->key << ", type: ";
        switch(e->type){
        case TYPE_TEXT:
            out << "text, value: \"" << dynamic_cast<TextField*>(e.get())->value << "\"";
            break;
        case TYPE_INTEGER:
            out << "int, value: " << dynamic_cast<IntegerField*>(e.get())->value;
            break;
        case TYPE_REAL:
            out << "real, value: " << dynamic_cast<RealField*>(e.get())->value;
            break;
        default:
            out << "unknown (" << e->type << ")";
        }
        out << "\n";
    }
}

ostream& operator <<(ostream& out, const Database::ResultsBuilder& instance){
    instance.dump(out);
    return out;
}

} // namespace database
