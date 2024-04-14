package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.service.QueryService;

import java.sql.Statement;
import java.util.ArrayList;

public class QueryController {
    QueryService queryService = new QueryService();

    public ArrayList<String> getTableList(Statement statement, String[] schemaList) {
        return queryService.getTableList(statement, schemaList);
    }

}
