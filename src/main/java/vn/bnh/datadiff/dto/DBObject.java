package vn.bnh.datadiff.dto;

import lombok.Getter;
import lombok.Setter;

public class DBObject {

    @Getter
    @Setter
    private String database;
    @Getter
    @Setter
    private String connectionString;

    @Getter
    @Setter
    private String userName;

    @Getter
    @Setter
    private String password;

    @Getter
    @Setter
    private String[] schemaList;

}
