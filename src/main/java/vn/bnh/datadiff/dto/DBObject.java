package vn.bnh.datadiff.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
public class DBObject {
    private String connectionString;
    private String username;
    private String password;
    private String dbname;
}