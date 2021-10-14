DELETE messages , usersmessages  FROM messages  INNER JOIN usersmessages  
WHERE messages.messageid= usersmessages.messageid and messages.messageid = '1'