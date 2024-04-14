package vn.bnh.datadiff.service;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.log4j.Log4j;
import vn.bnh.datadiff.dto.DBObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.logging.Logger;

public class QueryService {

    Logger log4j = Logger.getLogger(QueryService.class.getName());


    public ArrayList<String> getTableList(Statement statement, String[] databaseSchema, Cache cache, String query, String database) throws SQLException {
        DBObject object = (DBObject) cache.getIfPresent(database);
        log4j.info("Getting table list from database " + object.getDatabase().toUpperCase(Locale.ROOT) + " with schema: " + Arrays.toString(databaseSchema));
        log4j.info("Executing query: " + query + "'" + databaseSchema[0].toUpperCase(Locale.ROOT) + "'");
        ResultSet resultSet = statement.executeQuery(query + "'" + databaseSchema[0].toUpperCase(Locale.ROOT) + "'");

        log4j.info("Executing query: " + query + "'" + databaseSchema[0] + "'");
        ArrayList<String> tableList = new ArrayList<>();
        while (resultSet.next()) {
            String tableName = resultSet.getString("table_name");
            tableList.add(tableName);
        }
        return tableList;
    }
}
