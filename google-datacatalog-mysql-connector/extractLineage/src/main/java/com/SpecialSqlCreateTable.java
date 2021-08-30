package com;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SpecialSqlCreateTable extends SqlCreate {
    public final SqlIdentifier name;
    @Nullable
    public final SqlNodeList columnList;
    @Nullable
    public final SqlNode query;
    private static final SqlOperator OPERATOR;

    public SpecialSqlCreateTable(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, @Nullable SqlNodeList columnList, @Nullable SqlNode query) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = (SqlIdentifier)Objects.requireNonNull(name, "name");
        this.columnList = columnList;
        this.query = query;
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(this.name, this.columnList, this.query);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (this.ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        this.name.unparse(writer, leftPrec, rightPrec);
        if (this.columnList != null) {
            Frame frame = writer.startList("(", ")");
            Iterator var5 = this.columnList.iterator();

            while(var5.hasNext()) {
                SqlNode c = (SqlNode)var5.next();
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }

            writer.endList(frame);
        }

        if (this.query != null) {
            writer.keyword("AS");
            writer.newlineAndIndent();
            this.query.unparse(writer, 0, 0);
        }

    }

    static {
        OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);
    }
}
