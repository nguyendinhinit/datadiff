package vn.bnh.datadiff.dto;

import lombok.*;

@Getter
@Setter
@RequiredArgsConstructor
@AllArgsConstructor
@NoArgsConstructor
public class ColumnObject {
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

    String primaryKey;

    String incremental;

    String constraint;

    String index;

}
