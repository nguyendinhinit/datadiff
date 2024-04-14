package vn.bnh.datadiff.service;

import lombok.extern.log4j.Log4j;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;

public class QueryService {

    Logger log4j = Logger.getLogger(QueryService.class.getName());
    public ArrayList<String> getTableList(Statement statement, String [] databaseSchema) {
        log4j.info("Getting table list from database with schema: " + Arrays.toString(databaseSchema));
        return null;
    }
}
