import re
from typing import Sequence
from google.datacatalog_connectors.mysql_.parse.transform_equ.commons import Commons
from google.datacatalog_connectors.mysql_.parse.transform_equ.transform_create import TransformCreate
from google.datacatalog_connectors.mysql_.parse.transform_equ.transform_update import TransformUpdate


class TransformDelete:
    _post_transformer = None
    
    def can_transform(self, query: str) -> str:
        delete_from_regex = "\\s*DELETE\\s+((?s).*)"
        pattern = re.compile(delete_from_regex, re.IGNORECASE) 
        matched = pattern.match(query)
        if not matched: 
            return False
                
        delete_body = pattern.search(query).group(1)

        from_index = Commons.findSqlKeyword(delete_body, "FROM")

        if from_index is None:
            return False
        

        target_table = delete_body[0:from_index].strip()
        if target_table == "":
            return False
        
        return True
    
    def transform(self, query: str) -> str:
        delete_regex = "\\s*DELETE\\s+((?s).*)"
        pattern = re.compile(delete_regex,re.IGNORECASE)
        # find Matching patterns
        delete_body = pattern.search(query).group(1)
        
        from_start = Commons.findSqlKeyword(delete_body, "FROM")
        target_table = delete_body[0:from_start].strip()
        subQuery = delete_body[from_start:]
        equivalentQuery = f"UPDATE {target_table} \n SET _ = (SELECT * {subQuery}  )"

        updateTransform = TransformUpdate()
        
        if (updateTransform.can_transform(equivalentQuery)):
            equivalentQuery = updateTransform.transform(equivalentQuery)
            self._post_transformer = updateTransform
        
        return  equivalentQuery

    def post_parse_transform(self, node):
        if self._post_transformer is not None:
            return self._post_transformer.post_parse_transform(node)
        return node