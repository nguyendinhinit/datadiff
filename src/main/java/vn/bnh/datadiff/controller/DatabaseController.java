package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.Impl.DatabaseServiceImpl;

import java.sql.Statement;

public class DatabaseController {

    DatabaseServiceImpl databaseServiceImpl = new DatabaseServiceImpl();

    public Statement connectToDatabase(String connectionString, String userName, String passWord) {
       return databaseServiceImpl.connectToDatabase(connectionString,userName,passWord);
    }


}
