import nfty.db as db

qry = {
    "chats": "SELECT id, meta, sent FROM users.chat WHERE chat_id = %s ORDER BY id",
    "chats-limit": "SELECT id, meta, sent FROM users.chat WHERE chat_id = %s LIMIT {} OFFSET {} ORDER BY id",
    "add": "INSERT INTO users.chat (id, chat_id, meta) VALUES (%s, %s, %s) RETURNING id, sent",
    "update" : "UPDATE users.chat SET meta = %s WHERE id = %s RETURNING meta"
}
class Chat:
    user2 = None
    user1 = None
    chat_id = None

    @classmethod
    def by_chat_id(cls, chat_id):
        cls.chat_id = chat_id
        cls.user1, cls.user2 = chat_id.split('.')
        return cls

    @classmethod
    def by_user_id(cls, user1, user2):
        if user1 > user2: #ORDER USERS BY ID
            cls.user1 = user2
            cls.user2 = user1
        else:
            cls.user1 = user1
            cls.user2 = user2
        cls.chat_id = f'{cls.user1}.{cls.user2}'
        return cls

    def get(self):
        return db.fetchone(qry["chats"], self.chat_id)

    def get_limit(self, meta):
        limit = meta.get('limit', 10)
        offset = meta.get('offset', 0)
        return db.fetchall(qry["chats-limit"].format(int(limit), int(offset)), self.id)

    def add(self, meta):
        if self.chat_id:
            return db.fetchone(qry["add"], self.chat_id, meta)

    def update(self, id, meta):
        return db.fetchreturn(qry["update"], id, meta)