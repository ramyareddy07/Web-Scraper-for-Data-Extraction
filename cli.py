import argparse
import logging
import sys
import getpass
from scraper.config import ScraperConfig
from scraper.csv_store import CSVStore
from scraper.mysql_store import MySQLStore
from scraper.scraper import WebScraper

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start-url", required=True)
    p.add_argument("--item-selector", required=True)
    p.add_argument("--field", action="append", required=True)
    p.add_argument("--next-selector", default=None)
    p.add_argument("--max-pages", type=int, default=0)
    p.add_argument("--delay", type=float, default=1.0)
    p.add_argument("--csv", default="output.csv")
    p.add_argument("--db-host", default=None)
    p.add_argument("--db-port", type=int, default=3306)
    p.add_argument("--db-user", default=None)
    p.add_argument("--db-password", default=None)
    p.add_argument("--db-name", default="scraper_db")
    p.add_argument("--db-table", default="scraped_data")
    p.add_argument("--user-agent", default="Mozilla/5.0 (compatible; DataScraper/1.0)")
    p.add_argument("--log", default="INFO")
    return p.parse_args()

def build_fields(field_args):
    fields = {}
    for f in field_args:
        if "=" not in f:
            continue
        k, v = f.split("=", 1)
        fields[k.strip()] = v.strip()
    return fields

if __name__ == "__main__":
    args = parse_args()
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level, format="%(asctime)s [%(levelname)s] %(message)s")
    fields = build_fields(args.field)
    if not fields:
        logging.error("No fields provided")
        sys.exit(1)
    config = ScraperConfig(
        start_url=args.start_url,
        item_selector=args.item_selector,
        fields=fields,
        next_selector=args.next_selector,
        max_pages=args.max_pages,
        delay=args.delay,
        user_agent=args.user_agent
    )
    csv_fields = list(fields.keys()) + ["source_url", "scraped_at"]
    csv_store = CSVStore(args.csv, csv_fields)
    mysql_store = None
    if args.db_host and args.db_user:
        if args.db_password is None:
            args.db_password = getpass.getpass("MySQL password: ")
        mysql_store = MySQLStore(args.db_host, args.db_port, args.db_user, args.db_password, args.db_name, args.db_table)
    scraper = WebScraper(config, csv_store, mysql_store)
    try:
        scraper.run()
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    finally:
        if mysql_store:
            mysql_store.close()