import nfty.db as db

qry = {
    "questions": "SELECT id, meta, category FROM cache.questions ORDER BY category",
    "questions_in_category": "SELECT id, meta, sent FROM cache.questions WHERE category = %s LIMIT {} OFFSET {} ORDER BY id",
    "categories": "SELECT DISTINCT(category) FROM cache.questions",
    "random" : "SELECT * FROM cache.questions ORDER BY RANDOM() LIMIT 1;",
    "random_category": "SELECT * FROM cache.questions WHERE category = %s ORDER BY RANDOM() LIMIT 1;"
}
class Questions:
    def get(self):
        return db.fetchone(qry["questions"], self.chat_id)

    def categories(self, filter=None):
        if filter:
            return db.fetchall(qry["questions_in_category"], filter)
        else:
            return db.fetchall(qry["categories"])

    def random(self, category=None):
        if category:
            return db.fetchone(qry["random_category"], category)
        return db.fetchone(qry["random"])