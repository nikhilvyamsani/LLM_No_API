import sqlite3

def init_db():
    print("Initializing DB")
    conn = sqlite3.connect("audit_reports.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS audit_reports (
            audit_id INTEGER PRIMARY KEY,
            site_id TEXT,
            subsite_id TEXT,
            anomaly_id TEXT,
            user_id TEXT,
            date_audited DATE
        )
    ''')

    cursor.execute("DELETE FROM audit_reports")
    sample_data = [
        (1, "SITE001", "SUB001", "ANOM001", "USR001", "2024-01-15"),
        (2, "SITE001", "SUB002", "ANOM002", "USR002", "2024-01-20"),
        (3, "SITE002", "SUB003", "ANOM003", "USR003", "2024-02-01"),
        (4, "SITE001", "SUB001", "ANOM004", "USR001", "2024-02-10"),
        (5, "SITE002", "SUB004", "ANOM005", "USR002", "2024-03-05"),
        (6, "SITE001", "SUB004", "ANOM006", "USR002", "2024-03-05"),
        (7, "SITE001", "SUB004", "ANOM007", "USR002", "2024-03-05")
    ]
    cursor.executemany('''
        INSERT INTO audit_reports (audit_id, site_id, subsite_id, anomaly_id, user_id, date_audited)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', sample_data)
    conn.commit()
    conn.close()
if __name__ == "__main__":
    init_db()