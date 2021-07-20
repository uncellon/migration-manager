#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <cstring>
#include <set>
#include <regex>
#include <nlohmann/json.hpp>

/*
    Include directly the different
    headers from cppconn/ and mysql_driver.h + mysql_util.h
    (and mysql_connection.h). This will reduce your build time!
*/
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

/*******************************************************************************
 * Defines
 ******************************************************************************/

#define SETTINGS_FILENAME "settings.json"
#define SQL_CONNECTION_TIMEOUT 10

/*******************************************************************************
 * Globals
 ******************************************************************************/

sql::Driver *SQL_DRIVER = nullptr;
sql::Connection *SQL_CONNECTION = nullptr;
sql::Statement *SQL_STMT = nullptr;
std::string path;

/*******************************************************************************
 * Prototypes
 ******************************************************************************/

void createMigration();
void migrateUp(unsigned int count = 0);
void migrateDown(unsigned int count = 1);

void dbSet(const char *host, const char *user, const char *password, const char* database);
void printHelp();
bool dbInit();
void dbDestroy();
bool confirm(const std::string &text);

/*******************************************************************************
 * Main
 ******************************************************************************/

int main(int argc, char *argv[]) {
    path = std::string( std::filesystem::current_path() ) + "/";

    // Initialize "migrations" folder
    if (!std::filesystem::exists(path + "migrations")) {
        std::filesystem::create_directory(path + "migrations");
    } else if (!std::filesystem::is_directory(path + "migrations")) {
        std::cout << "Cannot create \"migrations\" directory because file with the same name already exists!" << std::endl;
        return EXIT_FAILURE;
    }

    // Print help
    if (argc < 2) {
        printHelp();
        return EXIT_SUCCESS;
    }

    if (strcmp("create", argv[1]) == 0) {
        createMigration();
    } else if (strcmp("up", argv[1]) == 0) {
        unsigned int count = 0;
        if (argc >= 3) {
            try {
                count = std::stoi(argv[2]);
            } catch (std::invalid_argument &e) {
                std::cout << "The number of migrations must be an integer!" << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        migrateUp(count);
    } else if (strcmp("down", argv[1]) == 0) {
        unsigned int count = 1;
        if (argc >= 3) {
            try {
                count = std::stoi(argv[2]);
            } catch (std::invalid_argument &e) {
                std::cout << "The number of migrations must be an integer!" << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        migrateDown(count);
    } else if (strcmp("db-set", argv[1]) == 0) {
        if (argc < 4) {
            std::cout << "Invalid argument count" << std::endl;
            return EXIT_FAILURE;
        }
        dbSet(argv[2], argv[3], argv[4], argv[5]);
    } else if (strcmp("help", argv[1]) == 0) {
        printHelp();
    }

    return EXIT_SUCCESS;
}

void createMigration() {
    // Get current date and time
    time_t now = time(0);
    tm *ltm = localtime(&now);

    // Format filename
    std::string filename = "m";
    char month[3], day[3], hour[3], min[3], sec[3]; // Two digits and null-terminator
    sprintf(month, "%02d", ltm->tm_mon + 1);
    sprintf(day, "%02d", ltm->tm_mday);
    sprintf(hour, "%02d", ltm->tm_hour);
    sprintf(min, "%02d", ltm->tm_min);
    sprintf(sec, "%02d", ltm->tm_sec);
    filename += std::to_string(1900 + ltm->tm_year) + month + day + "_" + hour + min + sec + ".sql";

    // Try to create file
    std::ofstream file;
    file.open("migrations/" + filename);

    file << "-- UP\n\n";
    file << "-- DOWN\n\n";

    file.close();

    std::cout << filename << " created!" << std::endl;
}

void migrateUp(unsigned int count) {
    if (!dbInit()) {
        std::cout << "Failed to initialize database connection!\n";
        return;
    }

    // Get migrations count and last migration id
    std::string lastMigrationId = "";
    auto result = SQL_STMT->executeQuery("SELECT COUNT(*) FROM `migrations`;");
    result->next();
    unsigned int appliedMigrations = result->getUInt("COUNT(*)");
    delete result;
    if (appliedMigrations != 0) {
        result = SQL_STMT->executeQuery("SELECT * FROM `migrations` ORDER BY `id` DESC LIMIT 1;");            
        result->next();
        lastMigrationId = result->getString("id");
        delete result;
    }

    // Print info about migrations
    bool filtered = false;
    if (lastMigrationId != "") {
        std::cout << "Last applied migration: " << lastMigrationId << std::endl;
    } else {
        std::cout << "No applied migrations found\n";
        filtered = true;
    }

    // Get ordered filenames
    std::set<std::filesystem::path> migrations;
    for (const auto &entry : std::filesystem::directory_iterator("migrations")) {
        migrations.insert(entry.path());
    }

    // If the counter is not set, we take it as the number of migration files 
    if (count == 0) {
        count = migrations.size();
    }

    // Filter migrations set
    auto iter = migrations.begin();
    for (unsigned int i = 0; i < migrations.size(); ++i) {
        if (filtered && --count == 0) {
            migrations.erase(++iter, migrations.end());
            break;
        }

        // Get migration id
        std::string migrationId = iter->filename();
        migrationId = migrationId.substr(0, migrationId.find("."));

        if (!filtered) {
            if (lastMigrationId == migrationId) {
                filtered = true;
                migrations.erase(migrations.begin(), ++iter);
                continue;
            }
        }
        ++iter;
    }  
    if (migrations.size() == 0) {
        std::cout << "No migrations available\n";
        dbDestroy();
        return;
    }

    // Check user response
    std::cout << "\nApplicable migrations (" + std::to_string(migrations.size()) + " pcs.):\n";
    for (const auto &migration : migrations) {
        // Form migration id (name)
        std::string migrationId = migration.filename();
        migrationId = migrationId.substr(0, migrationId.find("."));
        
        std::cout << migrationId << std::endl;
    }
    if (!confirm("\nApply above migrations?")) {
        dbDestroy();
        return;
    }


    // Apply migrations
    std::cout << "\nApplying migrations:" << std::endl;
    std::string delimiter = ";";
    for (const auto &migration : migrations) {
        // Form migration id (name)
        std::string migrationId = migration.filename();
        migrationId = migrationId.substr(0, migrationId.find("."));
        std::cout << migrationId << "...";

        // Open file
        std::ifstream file(migration);
        if (!file) {
            std::cout << "failed to open file!" << std::endl;
            dbDestroy();
            return;
        }

        std::string line;

        // Search for "-- UP"
        while (!file.eof()) {
            std::getline(file, line);
            if (line.find("-- UP") != std::string::npos) {
                break;
            }
        }
        if (file.eof()) {
            std::cout << "up block not found" << std::endl;
            dbDestroy();
            return;
        }

        // Prepare sql
        std::list<std::string> sqlQueries;
        sqlQueries.push_back("INSERT INTO `migrations` VALUES (\"" + migrationId + "\");");
        std::string sqlQuery;

        while (!file.eof()) {
            std::getline(file, line);

            // Find migration file separator
            if (line.find("-- DOWN") != std::string::npos) {
                break;
            }

            // Skip empty lines
            if (line == "") {
                continue;
            }

            // Check delimiter changed
            std::cmatch cmatch;
            std::regex_search(line.c_str(), cmatch, std::regex("[D|d][E|e][L|l][I|i][M|m][I|i][T|t][E|e][R|r] (.*)$"));
            if (cmatch.size() == 2) {
                delimiter = cmatch[1].str();
                continue;
            }

            sqlQuery += line + "\n";

            if (line.find(delimiter) != std::string::npos) {
                // Replace custom delimiter to ";"
                size_t startPos = 0;
                while ((startPos = sqlQuery.find(delimiter, startPos)) != std::string::npos) {
                    sqlQuery.replace(startPos, delimiter.length(), ";");
                    startPos += 1;
                }
                
                sqlQueries.push_back(sqlQuery);
                sqlQuery.clear();
            }
        }

        // Apply migration
        for (const auto &sqlQuery : sqlQueries) {
            try {
                SQL_STMT->execute(sqlQuery);
            } catch (sql::SQLException &e) {
                std::cout << "failed!";
                std::cout << "\n==============================" << std::endl;
                // std::cout << "Migration " << migrationId << " apply failed!\n\n";
                std::cout << "Query:\n" << sqlQuery << "\n\n";
                std::cout << "Error:\n" << e.what() << std::endl;
                std::cout << "==============================\n" << std::endl;
                dbDestroy();
                return;
            }
        }
        // std::cout << migrationId << " applied!" << std::endl;
        std::cout << "done!\n";
    }
    std::cout << "\nAll migrations applied!\n";
    dbDestroy();
}

void migrateDown(unsigned int count) {
    if (!dbInit()) {
        std::cout << "Failed to initialize database connection!\n";
        return;
    }

    // Get migrations
    std::list<std::string> migrations;
    auto result = SQL_STMT->executeQuery("SELECT * FROM `migrations` ORDER BY `id` DESC LIMIT " + std::to_string(count));
    while (result->next()) {
        migrations.push_back("migrations/" + result->getString("id") + ".sql");
    }
    delete result;

    // Check migrations count
    if (!migrations.size()) {
        std::cout << "No migrations find to revert\n";
        dbDestroy();
        return;
    }

    // Get migrations total
    result = SQL_STMT->executeQuery("SELECT COUNT(*) FROM `migrations`");
    result->next();
    unsigned int totalMigrations = result->getUInt("COUNT(*)");
    delete result;
    if (count > totalMigrations) {
        count = totalMigrations;
    }

    if (!confirm(std::to_string(totalMigrations) + " migration(s) applied. Revert " + std::to_string(count) + " migration(s)?")) {
        dbDestroy();
        return;
    }

    // Revert migrations
    std::cout << "\nReverting migrations:\n";
    for (const auto &migration : migrations) {
        std::string migrationId = migration.substr(migration.find("/") + 1, migration.find(".") - migration.find("/") - 1);
        std::cout << migrationId << "...";

        // Try to open file
        std::ifstream file(migration);
        if (!file) {
            std::cout << "failed to open file!\n";
            dbDestroy();
            return;
        }

        // Search for "-- DOWN" block
        std::string tmp;
        while (!file.eof()) {
            std::getline(file, tmp);
            if (tmp.find("-- DOWN") != std::string::npos) {
                break;
            }
        }
        if (file.eof()) {
            std::cout << "down block not found!\n";
            dbDestroy();
            return;
        }

        // Prepare sql
        std::list<std::string> sqlQueries;
        std::string sqlQuery;
        while (!file.eof()) {
            std::getline(file, tmp);

            sqlQuery += tmp + "\n";

            if (tmp.find(";") != std::string::npos) {
                sqlQueries.push_back(sqlQuery);
                sqlQuery.clear();
            }
        }
        sqlQueries.push_back("DELETE FROM `migrations` WHERE `id` = \"" + migrationId + "\";");

        // Revert migration
        for (const auto &sqlQuery : sqlQueries) {
            try {
                SQL_STMT->execute(sqlQuery);
            } catch (sql::SQLException &e) {
                std::cout << "failed!\n";
                std::cout << "\n==============================" << std::endl;
                // std::cout << "Migration " << migrationId << " revert failed!\n\n";
                std::cout << "Query:\n" << sqlQuery << "\n\n";
                std::cout << "Error:\n" << e.what() << std::endl;
                std::cout << "==============================\n" << std::endl;
                dbDestroy();
                return;
            }
        }
        std::cout << "done!\n";
    }
    std::cout << "\nAll migrations reverted!\n";
    dbDestroy();
}

void dbSet(const char *host, const char *user, const char *password, const char* schema) {
    // Try to connect
    sql::Driver *driver = get_driver_instance();
    sql::ConnectOptionsMap connectionProperties;
    connectionProperties["hostName"] = host;
    connectionProperties["userName"] = user;
    connectionProperties["password"] = password;
    connectionProperties["schema"] = schema;
    connectionProperties["OPT_CONNECT_TIMEOUT"] = SQL_CONNECTION_TIMEOUT;
    try {
        sql::Connection *connection = driver->connect(connectionProperties);
        delete connection;
    } catch (sql::SQLException &e) {
        std::cout << "Error: " << e.what() << "; Code: " << e.getErrorCode() << std::endl;
        return;
    }

    // Create JSON
    nlohmann::json json;
    json["host"] = host;
    json["user"] = user;
    json["password"] = password;
    json["schema"] = schema;

    // Write config
    std::ofstream file;
    file.open(path + SETTINGS_FILENAME);
    if (!file.is_open()) {
        std::cout << "Failed to open \"" << SETTINGS_FILENAME << "\"";
    }
    file << "/* THIS FILE GENERATED AUTOMATICALLY. USER DEFINED CHANGED MAY NOT BE SAVED AFTER REGENERATION! */" << std::endl;
    file << json.dump(4);
    file.close();

    std::cout << "\"" << SETTINGS_FILENAME << "\" generated" << std::endl;
}

void printHelp() {
    auto message = 
R"(Usage:
    migration-manager <action>
Actions:
    create - create new migration template;
    up <count> - apply migrations. Without specifying the quantity, all migrations will be applied;
    down <count> - undo migrations. Without specifying the quantity, one migration will be canceled;
    db-set <host> <user> <password> <schema> - set database connection;
    help - print this message.)";

    std::cout << message << std::endl;
}

bool dbInit() {
    // Check file exists
    if (!std::filesystem::exists(path + SETTINGS_FILENAME)) {
        std::cout << "\"" << SETTINGS_FILENAME << "\" not found" << std::endl;
        return false;
    }

    // Read config
    nlohmann::json json;
    try {
        std::ifstream file;
        file.open(path + SETTINGS_FILENAME);
        json = nlohmann::json::parse(file, nullptr, true, true);
        file.close();
    } catch (...) {
        std::cout << "Invalid file format \"" << SETTINGS_FILENAME << "\"" << std::endl;
        return false;
    }

    // Check fields
    if (!json.contains("host")) {
        std::cout << "\"" << SETTINGS_FILENAME << "\" doesn't contain \"host\" field" << std::endl;
        return false;
    } else if (!json.contains("user")) {
        std::cout << "\"" << SETTINGS_FILENAME << "\" doesn't contain \"user\" field" << std::endl;
        return false;
    } else if (!json.contains("password")) {
        std::cout << "\"" << SETTINGS_FILENAME << "\" doesn't contain \"password\" field" << std::endl;
        return false;
    } else if (!json.contains("schema")) {
        std::cout << "\"" << SETTINGS_FILENAME << "\" doesn't contain \"schema\" field" << std::endl;
        return false;
    }

    // Try to connect
    SQL_DRIVER = get_driver_instance();
    sql::ConnectOptionsMap connectionProperties;
    connectionProperties["hostName"] = std::string(json["host"]);
    connectionProperties["userName"] = std::string(json["user"]);
    connectionProperties["password"] = std::string(json["password"]);
    connectionProperties["schema"] = std::string(json["schema"]);
    connectionProperties["OPT_CONNECT_TIMEOUT"] = SQL_CONNECTION_TIMEOUT;
    if (SQL_CONNECTION) {
        delete SQL_CONNECTION;
    }
    try {
        SQL_CONNECTION = SQL_DRIVER->connect(connectionProperties);
    } catch (sql::SQLException &e) {
        std::cout << "Error: " << e.what() << "; Code: " << e.getErrorCode() << std::endl;
        return false;
    }

    // Check database "migrations" table
    if (SQL_STMT) {
        delete SQL_STMT;
    }
    SQL_STMT = SQL_CONNECTION->createStatement();
    SQL_STMT->execute(
R"(CREATE TABLE IF NOT EXISTS `migrations` 
(`id` varchar(64) NOT NULL PRIMARY KEY))");
    return true;
}

void dbDestroy() {
    if (SQL_STMT) {
        SQL_STMT->close();
    }
    delete SQL_STMT;
    SQL_STMT = nullptr;

    if (SQL_CONNECTION) {
        SQL_CONNECTION->close();
    }
    delete SQL_CONNECTION;
    SQL_CONNECTION = nullptr;

    SQL_DRIVER = nullptr;
}

bool confirm(const std::string &text) {
    std::cout << text << " (y/n): ";
    char answer;
    while (true) {
        std::cin >> answer;
        answer = std::tolower(answer);
        if (answer == 'y') {
            return true;
        } else if (answer == 'n') {
            return false;
        } else {
            std::cout << "Please enter 'y' to confirm or 'n' to cancel: ";
        }
    }
}