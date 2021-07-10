package norensa.parquet.io;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;

public class ColumnIdentifier {

    private int index;
    private SchemaElement schemaElement;
    private boolean nullable;

    ColumnIdentifier(int index, SchemaElement schemaElement) {
        this.index = index;
        this.schemaElement = schemaElement;
        nullable = false;
    }

    public int getIndex() {
        return index;
    }

    public Type getType() {
        return schemaElement.type;
    }

    public ConvertedType getConvertedType() {
        return schemaElement.converted_type;
    }

    public String getName() {
        return schemaElement.name;
    }

    public int getPrecision () {
        return schemaElement.precision;
    }

    public int getScale() {
        return schemaElement.scale;
    }

    int getElementLength() {
        return schemaElement.type_length;
    }

    void setNullable() {
        this.nullable = true;
    }

    public boolean isNullable() {
        return nullable;
    }

    FieldRepetitionType getRepetitionType() {
        return schemaElement.repetition_type;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ColumnIdentifier) {
            ColumnIdentifier that = (ColumnIdentifier)obj;

            return (index == that.index && schemaElement.equals(that.schemaElement));
        }
        else {
            return false;
        }
    }
}
