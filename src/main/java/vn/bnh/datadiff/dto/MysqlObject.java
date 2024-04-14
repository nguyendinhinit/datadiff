package vn.bnh.datadiff.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;

public class MysqlObject {
    @Getter
    @Setter
    private String mysqlConnection;

    @Getter
    @Setter
    private String mysqlUser;

    @Getter
    @Setter
    private String mysqlPassword;

    @Getter
    @Setter
    private String[] schemaList;

}
