package vn.bnh.datadiff.services.impl;

import vn.bnh.datadiff.services.PrepareStatementService;

import java.sql.PreparedStatement;

public class PrepareStatementServiceImpl implements PrepareStatementService {

    @Override
    public String prepareStatement(String dbType, String query) {
        switch (dbType) {
            case "mysql":
                switch (query) {
                    case "column":
                        return "SELECT COLUMN_NAME as CL, IS_NULLABLE as DN,DATA_TYPE as DT,CHARACTER_MAXIMUM_LENGTH as DL,NUMERIC_PRECISION as DP,NUMERIC_SCALE as DS,COLUMN_DEFAULT as DD, DATETIME_PRECISION as DDP FROM information_schema.columns WHERE TABLE_SCHEMA = ? and TABLE_NAME= ? order by COLUMN_NAME collate utf8_bin";
                }
            case "oracle":
                switch (query) {
                    case "column":
                        return "SELECT COLUMN_NAME as CL, DATA_TYPE as DT,DATA_LENGTH AS DL, DATA_PRECISION AS DP, DATA_SCALE as DS, NULLABLE as DN, DATA_DEFAULT as DD, null as DDP FROM all_tab_columns where OWNER = ? and TABLE_NAME = ? order by COLUMN_NAME";

                }
            case "sqlserver":
                return null;
            case "postgresql":
                return null;
            default:
                return null;
        }
    }
}
