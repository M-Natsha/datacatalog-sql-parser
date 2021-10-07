import re

class Commons:
    @staticmethod
    def _balanced_text(text: str) -> str:
        c1 = text.count('\'')
        c2 = text.count('\"')
        
        if c1%2 != 0 or c2%2 != 0:
            return False
        
        stack = []
        
        for ch in text:
            if ch == '(' or ch == '[':
                stack.append(ch) 
            elif ch == ')':
                if len(stack) > 0 and stack[-1] == '(':
                    stack.pop()
                else:
                    return False
                
            elif ch == ']':
                if len(stack) > 0 and stack[-1] == '[':
                    stack.pop()
                else:
                    return False
            
        if len(stack) != 0:
            return False

        return True
    
    @staticmethod
    def findSqlKeyword(text: str,target: str):
        target = '\\w*(?<![a-z0-9_])' + target + '[\\s\\(]';
        regex = re.compile(target, re.IGNORECASE)
        
        for m in regex.finditer(text):
            matchPos = m.start()
            if(Commons._balanced_text(text[0:matchPos])):
                return matchPos
            
        return None