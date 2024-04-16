package vn.bnh.datadiff.service;


import java.util.ArrayList;

public interface QueryService {

    public void getAllSchema(String query, String connectionString, String databaseType,String userName,String passWord);

    public ArrayList<String> getTableList(String connectionString, String database, String username, String password, String schema);
}
