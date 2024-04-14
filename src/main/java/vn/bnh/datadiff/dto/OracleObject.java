package vn.bnh.datadiff.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;

public class OracleObject {
    @Getter
    @Setter
    private String oracleConnection;

    @Getter
    @Setter
    private String oracleUser;

    @Getter
    @Setter
    private String oraclePassword;

    @Getter
    @Setter
    private String[] schemaList;

}
