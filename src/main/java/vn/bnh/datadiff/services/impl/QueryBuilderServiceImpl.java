package vn.bnh.datadiff.services.impl;

import vn.bnh.datadiff.dto.DBObject;
import vn.bnh.datadiff.services.QueryBuilderService;

public class QueryBuilderServiceImpl implements QueryBuilderService {
    @Override
    public String buildQuery(DBObject dbObject, String type) {
        String dbName = dbObject.getDbname();

        switch (dbName) {
            case "oracle":
                switch (type) {
                    case "schema":
                        return "SELECT username from dba_users where username not in (SELECT distinct(OWNER) FROM sys.dba_tab_privs WHERE grantee='PUBLIC')";
                    case "table":
                        return "SELECT table_name FROM all_tables WHERE owner = '%s'";
                    case "column":
                        return "SELECT COLUMN_NAME as CL, DATA_TYPE as DT,DATA_LENGTH AS DL, DATA_PRECISION AS DP, DATA_SCALE as DS, NULLABLE as DN, DATA_DEFAULT as DD, NULL as DDP FROM all_tab_columns where OWNER = '%s' and TABLE_NAME = '%s'  order by COLUMN_NAME";
                    case "foreignKey":
                        return "SELECT acc.COLUMN_NAME FROM ALL_CONS_COLUMNS acc INNER JOIN ALL_CONSTRAINTS ac ON ( acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME ) WHERE ac.OWNER = '%s' AND ac.TABLE_NAME = '%s' AND ac.CONSTRAINT_TYPE = 'R'";
                    case "primaryKey":
                        return "SELECT cols.column_name as PK FROM all_constraints cons, all_cons_columns cols WHERE cons.owner in ('%s') and cols.table_name = ('%s') AND cons.constraint_type in ('P','U') AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name";
                    case "increment":
                        return "SELECT COLUMN_NAME FROM all_tab_columns WHERE IDENTITY_COLUMN = 'YES' and OWNER = '%s' and TABLE_NAME = '%s'";
                    case "indexes":
                        return "SELECT INDEX_NAME, COLUMN_NAME FROM all_ind_columns where table_owner = '%s' AND table_name = '%s'";
                    case "constraints":
                        return "SELECT acc.constraint_name, ac.TABLE_NAME, acc.COLUMN_NAME FROM ALL_CONS_COLUMNS acc INNER JOIN ALL_CONSTRAINTS ac ON ( acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME ) WHERE ac.OWNER = '%s' AND ac.TABLE_NAME   = '%s' AND    ac.CONSTRAINT_TYPE IN ( 'U', 'P' )";
                }
                break;
            case "mysql":
                switch (type) {
                    case "schema":
                        return "SELECT schema_name FROM information_schema.schemata";
                    case "table":
                        return "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'";
                    case "column":
                        return "SELECT COLUMN_NAME as CL, IS_NULLABLE as DN,DATA_TYPE as DT,CHARACTER_MAXIMUM_LENGTH as DL,NUMERIC_PRECISION as DP,NUMERIC_SCALE as DS,COLUMN_DEFAULT as DD, DATETIME_PRECISION as DDP FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' and TABLE_NAME='%s' order by COLUMN_NAME collate utf8_bin";
                    case "foreignKey":
                        return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA = '%s' AND REFERENCED_TABLE_NAME = '%s'";
                    case "primaryKey":
                        return "SELECT k.column_name as PK FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY'   AND t.table_schema='%s'   AND t.table_name='%s'";
                    case "increment":
                        return "SELECT COLUMN_NAME FROM information_schema.columns WHERE EXTRA = 'auto_increment' and TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'";
                    case "indexes":
                        return "SELECT DISTINCT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' and TABLE_NAME='%s'";
                    case "constraints":
                        return "SELECT CONSTRAINT_NAME FROM information_schema.table_constraints WHERE  TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'";
                }
                break;
        }

        return null;
    }
}
