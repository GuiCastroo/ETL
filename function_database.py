import sqlite3


class Queries(object):
    def __enter__(self):
        self.con = sqlite3.connect('database.db')
        return self.con

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.con.close()

    @staticmethod
    def commit_query(query):
        with Queries() as sql_conn:
            sql_conn.execute(query)
            sql_conn.commit()

    @staticmethod
    def select(query, fetch=all):
        with Queries() as sql_conn:
            cur = sql_conn.execute(query)
            if fetch == all:
                result = cur.fetchall()
            else:
                result = cur.fetchone()
        return result


if __name__ == '__main__':
    Queries.commit_query("""create table agriculturalProduction(
        id TEXT PRIMARY  KEY    NOT NULL,
        state           TEXT    NOT NULL,
        time            INT     NOT NULL,
        area_ha         INT     NOT NULL,
        production_t    INT     NOT NULL
    )""")

    Queries.commit_query("""create table PIB(
            id TEXT PRIMARY   KEY    NOT NULL,
            year           INT      NOT NULL,
            segment        TEXT      NOT NULL,
            category       TEXT      NOT NULL,
            pibIcome       FLOAT     NOT NULL,
            pib            FLOAT     NOT NULL
    )""")
