package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.DatabaseService;

import java.sql.Statement;

public class DatabaseController {

    DatabaseService databaseService = new DatabaseService();

    public Statement connectToDatabase(DBObject dbObject) {
       return databaseService.connectToDatabase(dbObject);
    }


}
