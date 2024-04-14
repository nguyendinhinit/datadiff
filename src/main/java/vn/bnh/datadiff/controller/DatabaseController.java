package vn.bnh.datadiff.controller;

import vn.bnh.datadiff.dto.MysqlObject;
import vn.bnh.datadiff.dto.OracleObject;
import vn.bnh.datadiff.service.DatabaseService;

import java.sql.Statement;

public class DatabaseController {

    DatabaseService databaseService = new DatabaseService();

    public Statement connectMysql(MysqlObject mysqlObject) {
       return databaseService.connectMysql(mysqlObject);
    }

    public Statement connectOracle(OracleObject oracleObject) {
      return   databaseService.connectOracle(oracleObject);
    }

}
