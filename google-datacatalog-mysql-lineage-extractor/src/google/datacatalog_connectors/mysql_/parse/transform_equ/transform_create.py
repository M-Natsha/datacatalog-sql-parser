import re

from google.datacatalog_connectors.mysql_.parse.transform_equ.commons \
    import Commons


class TransformCreate:

    def can_transform(self, query) -> bool:
        pattern = re.compile("\\s*CREATE\\s+TABLE\\s+((?s).*)",
                             re.RegexFlag.IGNORECASE)
        return pattern.match(query)

    def transform(self, query) -> str:
        start = None
        end = None
        table_def_start = Commons.findSqlKeyword(query, "TABLE") + 5
        table_def_end = Commons.findSqlKeyword(query, "AS")

        counter = 0

        for i in range(table_def_start, table_def_end):
            ch = query[i]
            if ch == '(':
                counter += 1
                start = i if start is None else start
            elif ch == ')':
                counter -= 1
                if counter == 0:
                    end = i
                    break

        if start is not None and end is not None:
            columnDef: str = query[start + 1:end]

            if columnDef.strip() != "":
                columns = columnDef.split(",")

                newColDef: str = ""
                for col in columns:
                    if col.strip() != "" and newColDef != "":
                        newColDef += ","

                    attributes = col.strip().split(" ", 2)

                    if attributes[0] != ")":
                        newColDef += attributes[0]

                return query.replace(query[start + 1:end], newColDef, 1)

        return query

    def post_parse_transform(self, query):
        return query
