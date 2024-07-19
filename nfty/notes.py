import logging
from typing import List
import nfty.db as db
import nfty.constants as constants


logger = logging.getLogger("AppLogger")


qry = {
    "new": """ 
    INSERT INTO 
        cache.logs (clientid, userid, origin, comment) 
    VALUES 
        (%s,%s,%s,%s)  
    RETURNING 
        id 
""",
    "client_notes": """ 
    SELECT 
        id, userid, origin, comment, created 
    FROM 
        cache.logs 
    WHERE 
        clientid = %s
""",
    "delete": """ 
    DELETE FROM 
        cache.logs 
    WHERE 
        id = %s  
""",
}


class Notes:
    @staticmethod
    def new(clientid: str, userid: str, origin: str, note: str) -> str:
        return db.fetchreturn(qry["new"], clientid, userid, origin, note)

    @staticmethod
    def get(clientid: str) -> List:
        return db.fetchall(qry["client_notes"], clientid)

    @staticmethod
    def delete(id: str) -> str:
        db.execute(qry["delete"], id)
        return id

    @staticmethod
    def search(note):
        """
        Placeholder function designed to, eventually, perform a search operation to find a given `Note`.
        """
        pass