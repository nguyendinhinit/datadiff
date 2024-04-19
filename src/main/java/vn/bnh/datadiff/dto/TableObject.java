package vn.bnh.datadiff.dto;

import lombok.*;

import java.util.ArrayList;

@Getter
@Setter
@RequiredArgsConstructor
@AllArgsConstructor
@NoArgsConstructor
public class TableObject {
    @NonNull
    String schemaName;

    String tableName;

    String columnName;

    String dataType;

    String dataLength;

    String dataPrecision;

    String dataScale;

    String nullable;

    String dataDefault;

    String dateTimePrecision;

    String pKs;

}
