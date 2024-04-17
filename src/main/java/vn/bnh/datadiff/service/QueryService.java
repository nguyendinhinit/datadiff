package vn.bnh.datadiff.service;


import java.sql.Statement;
import java.util.ArrayList;

public interface QueryService {

    public void getAllSchema(String query, String connectionString, String databaseType,String userName,String passWord);

    public ArrayList<String> getTableList(String connectionString, String database, String username, String password, String schema);

    //Use for oracle
    public ArrayList<String> findPk(String tableName, String tableSchema, String connectionString, String username, String password);
}
