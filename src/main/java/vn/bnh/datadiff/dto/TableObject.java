package vn.bnh.datadiff.dto;

import lombok.*;

import java.util.ArrayList;

@AllArgsConstructor
@NoArgsConstructor
@RequiredArgsConstructor
public class TableObject {
    @Getter
    @Setter
    @NonNull
    private String schemaName;
    @Getter
    @Setter
    @NonNull
    private String tableName;
    @Getter
    @Setter
    @NonNull
    private String columnName;
    @Getter
    @Setter
    @NonNull
    private String dataType;
    @Getter
    @Setter
    @NonNull
    private String dataLength;
    @Getter
    @Setter
    @NonNull
    private String dataPrecision;
    @Getter
    @Setter
    @NonNull
    private String dataScale;
    @Getter
    @Setter
    @NonNull
    private String nullable;
    @Getter
    @Setter
    @NonNull
    private String dataDefault;
    @Getter
    @Setter
    private ArrayList<String> pK;
}
