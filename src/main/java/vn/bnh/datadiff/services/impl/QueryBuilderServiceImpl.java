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
                        return "SELECT COLUMN_NAME as CL, DATA_TYPE as DT,DATA_LENGTH AS DL, DATA_PRECISION AS DP, DATA_SCALE as DS, NULLABLE as DN, DATA_DEFAULT as DD, null as DDP FROM all_tab_columns where OWNER = '%s' and TABLE_NAME = '%s'  order by COLUMN_NAME";
                    case "foreignKey":
                        return "SELECT acc.COLUMN_NAME FROM ALL_CONS_COLUMNS acc INNER JOIN ALL_CONSTRAINTS ac ON ( acc.CONSTRAINT_NAME = ac.CONSTRAINT_NAME ) WHERE ac.OWNER = '%s' AND ac.TABLE_NAME = '%s' AND ac.CONSTRAINT_TYPE = 'R'";
                    case "primaryKey":
                        return "SELECT cols.column_name as PK FROM all_constraints cons, all_cons_columns cols WHERE cons.owner = '%s' AND cols.table_name = '%s'  AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position";
                    case "increment":
                        return "SELECT COLUMN_NAME FROM all_tab_columns WHERE IDENTITY_COLUMN = 'YES' and OWNER = '%s' and TABLE_NAME = '%s'";
                    case "indexes":
                        return "SELECT count(*) as INDEXES FROM all_ind_columns where TABLE_OWNER = '%s' and index_name not like 'BIN$%%'";
                    case "constraints":
                        return "select count(*) as CONSTRAINS from all_constraints where OWNER = '%s' and table_name not like 'BIN$%%'";
                    case "tablelist":
                        return "SELECT table_name FROM all_tables where OWNER='%s'";
                    case "partition":
                        return "select count(*) as P from all_tab_partitions where table_owner = '%s' and table_name = '%s' ";
                    case "index":
                        return "SELECT COUNT(*) as I FROM all_indexes WHERE table_owner='%s' and TABLE_NAME='%s'";
                    case "constraint":
                        return "select count(*) as CONSTRAINS from all_constraints where OWNER = '%s' and table_name = '%s'";
                    case "columnCount":
                        return "select count(*) as C from all_tab_columns where owner = '%s' and table_name = '%s'";
                    case "triggerCount":
                        return "select count(*) as T from all_triggers where owner = '%s' and table_name = '%s'";
                    case "sequenceCount":
                        return "select count(*) as SC from all_sequences where sequence_owner = '%s' and sequence_name = '%s'";
                    case "plSQLCount":
                        return "select count(*) from all_source where owner = '%s' and name = '%s'";
                    case "schedulerCount":
                        return "select count(*) from all_scheduler_jobs where owner = '%s' and job_name = '%s'";
                    case "viewCount":
                        return "select count(*) from all_views where owner = '%s'";
                    case "constraintCount":
                        return "select count(*) as C from all_constraints where OWNER = '%s' and table_name = '%s'";
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
                        return "SELECT count(*) as INDEXES FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s'";
                    case "constraints":
                        return "(SELECT TABLE_SCHEMA, sum(numb) AS CONSTRAINS FROM( (SELECT TABLE_SCHEMA,TABLE_NAME, COUNT(*) AS numb FROM (SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME,COLUMN_NAME FROM information_schema.COLUMNS WHERE IS_NULLABLE='NO' AND TABLE_SCHEMA NOT IN ('mysql','information_schema', 'performance_schema', 'sys','awsdms_control')) AS tbl_01 GROUP BY TABLE_SCHEMA, TABLE_NAME) UNION ALL (SELECT TABLE_SCHEMA,TABLE_NAME, COUNT(*) AS numb FROM (SELECT distinct TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME FROM information_schema.table_constraints WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema', 'performance_schema', 'sys','awsdms_control')) AS tbl_02 GROUP BY TABLE_SCHEMA, TABLE_NAME) ) AS tbl_03 where table_schema = '%s' GROUP BY TABLE_SCHEMA)";
                    case "tablelist":
                        return "SELECT table_name FROM information_schema.tables where TABLE_SCHEMA='%s'";
                    case "partition":
                        return "SELECT max(PARTITION_ORDINAL_POSITION) as P FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";
                    case "index":
                        return "select count(distinct INDEX_NAME) as I from INFORMATION_SCHEMA.STATISTICS where table_schema = '%s' and table_name = '%s'";
                    case "columnCount":
                        return "SELECT count(COLUMN_NAME) as C FROM information_schema.COLUMNS where table_schema = '%s' and table_name = '%s'";
                    case "triggerCount":
                        return "SELECT count(COLUMN_NAME) as T FROM information_schema.COLUMNS where table_schema = '%s' and table_name = '%s' ";
                    case "sequenceCount":
                        return "SELECT count(column_name) as SC FROM `information_schema`.`COLUMNS` WHERE `EXTRA` = 'auto_increment' AND `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'";
                    case "constraintCount":
                        return "SELECT count(constraint_name) as C FROM    information_schema.table_constraints where table_schema = '%s' and table_name = '%s'";
                }
                break;
        }

        return null;
    }
}
