import json
import mysql.connector
import logging

class MySQLStore:
    def __init__(self, host, port, user, password, database, table):
        self.config = dict(host=host, port=port, user=user, password=password, autocommit=True)
        self.database = database
        self.table = table
        self.conn = None
        self._connect()
        self._ensure_table()

    def _connect(self):
        self.conn = mysql.connector.connect(**self.config)
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
        cursor.close()
        self.conn.database = self.database

    def _ensure_table(self):
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS `{self.table}` ("
            "id INT AUTO_INCREMENT PRIMARY KEY,"
            "source_url VARCHAR(2048),"
            "title VARCHAR(1024),"
            "price VARCHAR(255),"
            "description TEXT,"
            "extra JSON,
            "scraped_at DATETIME,
            "UNIQUE KEY unique_record (source_url, title(255))"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )
        cursor = self.conn.cursor()
        cursor.execute(create_sql)
        cursor.close()

    def upsert(self, record: dict):
        cursor = self.conn.cursor()
        sql = (
            f"INSERT INTO `{self.table}` (source_url, title, price, description, extra, scraped_at) "
            "VALUES (%s,%s,%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE price=VALUES(price), description=VALUES(description), extra=VALUES(extra), scraped_at=VALUES(scraped_at)"
        )
        params = (
            record.get('source_url'),
            record.get('title'),
            record.get('price'),
            record.get('description'),
            json.dumps(record.get('extra') or {}),
            record.get('scraped_at')
        )
        try:
            cursor.execute(sql, params)
            cursor.close()
            return True
        except mysql.connector.Error as e:
            logging.error("MySQL upsert error: %s", e)
            cursor.close()
            return False

    def close(self):
        if self.conn:
            self.conn.close()