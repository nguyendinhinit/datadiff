package vn.bnh.datadiff.dto;
import lombok.Getter;

@Getter
public class oracleObject extends DbObject {

    public oracleObject(String connectionString, String dbname, String username, String password) {
        super(connectionString, dbname, username, password);
    }
    private final String schemaQuery = "SELECT username from dba_users";
}