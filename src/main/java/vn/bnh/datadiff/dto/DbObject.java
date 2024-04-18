package vn.bnh.datadiff.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
public class DbObject {
    @Getter
    @Setter
    private String connectionString;
    @Getter
    @Setter
    private String dbname;
    @Getter
    @Setter
    private String username;
    @Getter
    @Setter
    private String password;
}