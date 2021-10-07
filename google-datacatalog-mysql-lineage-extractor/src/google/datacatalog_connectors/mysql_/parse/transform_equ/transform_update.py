import re
import types
import uuid
from typing import List, Match, Sequence
from google.datacatalog_connectors.mysql_.parse.transform_equ.commons import Commons

class TransformUpdate:
    _alias_table = {}
    def random_name(self,x:Match):
        code = "uuid" + str(uuid.uuid1()).replace('-','')
        self._alias_table[code] = x.group().replace('=','').strip()
        return f" {code} = "
        
    def rename_set_targets(self, query: str) -> str:
        setBody: str = ""

        setPos = Commons.findSqlKeyword(query, "SET")
        wherePos = Commons.findSqlKeyword(query, "WHERE")

        if setPos is not None:
            if wherePos is not None: 
                setBody = query[setPos + 5:wherePos]
            else:
                setBody = query[setPos:]
        else: 
            return query
        
        varRegex: str = "[A-Za-z0-9_]+\\.[A-Za-z0-9_]+\\s*="
        rgx = re.compile(varRegex, re.IGNORECASE)
        
        newBody: str = rgx.sub(self.random_name, setBody)
                
        query = query.replace(setBody,newBody)
        return query
    

    def can_transform(self, query: str) -> bool:        
        updateRegex: str = "\\s*UPDATE\\s+((?s).*)SET((?s).*)"
        pattern = re.compile(updateRegex,re.IGNORECASE)
        return pattern.match(query)
    

    def _is_select_as_query(self, query: str) -> bool:
        query = query.strip()

        updateRegex: str = "\\(\\s*SELECT\\s+(?s).*as\\s+[a-z0-9_]*"
        pattern = re.compile(updateRegex, re.IGNORECASE)
        return pattern.match(query)

    def get_col_list(self, query: str) -> Sequence[str]:
        updateRegex: str = "\\s*update\\s+((?s).*)SET((?s).*)"
        pattern = re.compile(updateRegex, re.IGNORECASE)
        # find Matching patterns
        colListString = pattern.search(query).group(1)

        return colListString.split(",")

    def _get_set_query(self, query:str) -> str:
        updateRegex: str = "\\s*update\\s+((?s).*)SET((?s).*)"
        pattern = re.compile(updateRegex, re.IGNORECASE)
        # find Matching patterns
        return pattern.search(query).group(2)



    def _get_targets(self, colList: Sequence[str]) -> Sequence[str]:
        return filter(lambda col: not self._is_select_as_query(col), colList)
    
    def transform(self, query: str) -> str:
        # Filtering and removing unparsable information
        query = self.rename_set_targets(query)

        sources: Sequence[str] = self.get_col_list(query)
        targets: Sequence[str] =  self._get_targets(sources)

        combined_sources: str = " JOIN ".join(sources)
        set_clousre: str = self._get_set_query(query)

        equivalentQuery = ""
        for target in targets :
            if target.strip == "":
                continue

            equivalentQuery += f"UPDATE  {target} SET _ = ( SELECT * FROM {combined_sources} ),{set_clousre};" 
        
        return equivalentQuery


    def post_parse_transform(self, node):
        self.post_parse_transform_helper(node)
        return node
    
    
    def post_parse_transform_helper(self, node):
        
        if isinstance(node, List):
            for element in node:
                self.post_parse_transform_helper(element)
                
        elif isinstance(node, types.SimpleNamespace):
            if hasattr(node, "names"):
                value = getattr(node, "names")
                if len(value) > 0 and value[0] in self._alias_table:
                    new_names = self._alias_table[value[0]].split(".")
                    setattr(node, "names", new_names)
            else:  
                for _, value in node.__dict__.items():
                    self.post_parse_transform_helper(value)
                    
                    
        