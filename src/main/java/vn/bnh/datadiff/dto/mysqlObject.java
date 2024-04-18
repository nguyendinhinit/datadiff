package vn.bnh.datadiff.dto;

public class mysqlObject extends DbObject {

    public mysqlObject(String connectionString, String dbname, String username, String password) {
        super(connectionString, dbname, username, password);
    }
    
    private final String schemaQuery = "SHOW DATABASES";
}