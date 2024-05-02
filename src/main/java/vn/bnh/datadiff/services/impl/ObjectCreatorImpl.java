package vn.bnh.datadiff.services.impl;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.ObjectCreator;

import java.util.Properties;

/**
 * Creates a database object based on the connection string, username, password, and database name.
 * The object is created using the ObjectCreatorImpl class.
 *
 * @params properties, dbType
 */
public class ObjectCreatorImpl implements ObjectCreator {

    Logger log4j = LogManager.getLogger(ObjectCreatorImpl.class);
    static Logger log4jStatic = LogManager.getLogger(ObjectCreatorImpl.class);

    public DBObject create(String connectionString, String username, String password, String dbname) {
        log4j.info("Creating {} object", dbname);

        return new DBObject(connectionString, username, password, dbname);
    }

    public DBObject create(Properties properties, String dbType) {
        String connectionString = properties.getProperty(dbType + "_connection_string");
        String username = properties.getProperty(dbType + "_username");
        String password = properties.getProperty(dbType + "_password");
        String dbname = getDbTypeFromConnectionString(connectionString);
        return create(connectionString, username, password, dbname);
    }


    private static String getDbTypeFromConnectionString(String connectionString) {
        log4jStatic.info("Getting database type from connection string");
        // Assuming the database type is the substring between "jdbc:" and the first ":"
        int startIndex = connectionString.indexOf(":") + 1;
        int endIndex = connectionString.indexOf(":", startIndex);
        // Extracting the substring
        String dbType = connectionString.substring(startIndex, endIndex);
        log4jStatic.info("Database type: {}", dbType);
        return dbType;
    }

}