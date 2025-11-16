import csv

class CSVStore:
    def __init__(self, path, fieldnames):
        self.path = path
        self.fieldnames = fieldnames
        self._ensure_file()

    def _ensure_file(self):
        try:
            with open(self.path, 'r', newline='', encoding='utf-8'):
                pass
        except FileNotFoundError:
            with open(self.path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def write(self, record: dict):
        with open(self.path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow({k: record.get(k, '') for k in self.fieldnames})