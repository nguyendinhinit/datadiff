package org.example;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        ReadSchemaMysql.main(args);
        ReadSchemaOracle.main(args);
        Validate.main(args);
    }
}