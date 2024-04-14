package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.service.DatabaseService;

import java.sql.Statement;

public class DatabaseController {

    DatabaseService databaseService = new DatabaseService();

    public Statement connectMysql(DBObject mysqlObject) {
       return databaseService.connectMysql(mysqlObject);
    }

    public Statement connectOracle(DBObject oracleObject) {
      return   databaseService.connectOracle(oracleObject);
    }

}
