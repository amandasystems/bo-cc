CREATE TABLE IF NOT EXISTS urls (
        id INTEGER PRIMARY KEY,
        page_url TEXT UNIQUE,
        from_archive_id INTEGER,
        nr_forms_total INTEGER,
        FOREIGN KEY(from_archive_id) REFERENCES archives(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS forms (
        id INTEGER PRIMARY KEY,
        form BLOB,
        from_url INTEGER,
        FOREIGN KEY(from_url) REFERENCES urls(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS archives (
        id INTEGER PRIMARY KEY,
        record_url TEXT UNIQUE,
        nr_urls INTEGER DEFAULT 0,
        nr_forms INTEGER DEFAULT 0,
        nr_unknown_encoding INTEGER DEFAULT 0,
        all_records_submitted_for_analysis INTEGER DEFAULT 0
);

DELETE FROM archives WHERE all_records_submitted_for_analysis = false;
