package vn.bnh.datadiff.controller;

import com.github.benmanes.caffeine.cache.Cache;
import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.QueryService;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Objects;

public class QueryController {
    QueryService queryService = new QueryService();

    public ArrayList<String> getTableList(Statement statement, String[] schemaList, Cache cache, String query, String database) throws SQLException {
        return queryService.getTableList(statement, schemaList, cache, query, database);
    }

}
