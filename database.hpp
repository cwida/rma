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

#ifndef DATABASE_HPP_
#define DATABASE_HPP_

#include <cinttypes>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "errorhandling.hpp"

namespace database {

DEFINE_EXCEPTION(DatabaseException);

class Database {
private:
    const char* m_database_path;
    void* m_connection_handle; // actual type: sqlite3*
    uint64_t m_exec_id;

    // ResultsBuilder
public:
    enum FieldType { TYPE_TEXT, TYPE_INTEGER, TYPE_REAL };

    struct AbstractField {
        std::string key;
        FieldType type;

        AbstractField(const std::string& key, FieldType type);
        virtual ~AbstractField();
    };

    struct TextField : public AbstractField {
        std::string value;

        TextField(const std::string& key, const std::string& value);
    };

    struct IntegerField : public AbstractField {
        int64_t value;

        IntegerField(const std::string& key, int64_t value);
    };

    struct RealField : public AbstractField {
        double value;

        RealField(const std::string& key, double value);
    };

    class ResultsBuilder {
        friend class Database;
        friend std::ostream& operator<<(std::ostream& outstream, const Database::ResultsBuilder& instance);

        Database* instance;
        const std::string tableName;
        std::vector<std::unique_ptr<AbstractField>> fields;
        void save();
        void save(void* handle);
        void dump(std::ostream& out) const;

    public:
        ResultsBuilder(Database* instance, const std::string& tableName);
        ~ResultsBuilder();
        ResultsBuilder& operator()(const std::string& key, const std::string& value);
        ResultsBuilder& operator()(const std::string& key, int64_t value);
        ResultsBuilder& operator()(const std::string& key, double value);

        // Treat all integer types as int64_t
        template <typename T>
        std::enable_if_t<std::is_integral_v<T>, ResultsBuilder&>
        operator()(const std::string& key, T value){
            return operator()(key, static_cast<int64_t>(value));
        }
    };
    friend class ResultsBuilder;

public:
    Database();
    virtual ~Database();

    /**
     * The path to the sqlite3 database
     */
    const char* db_path() const { return m_database_path; }

    /**
     * The generated execution ID to identify the experiment results
     * in the database
     */
    uint64_t id() const { return m_exec_id; }

    /**
     * Add the given experiment results in the table `tableName'. The table
     * is created if it does not already exist.
     */
    ResultsBuilder add(const std::string& tableName);
};

std::ostream& operator<<(std::ostream& outstream, const Database::ResultsBuilder& instance);

}

#endif /* DATABASE_HPP_ */
