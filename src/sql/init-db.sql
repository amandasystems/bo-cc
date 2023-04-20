pragma temp_store = memory;
pragma mmap_size = 30000000000;
pragma page_size = 4096;

CREATE TABLE IF NOT EXISTS archives (
        id INTEGER PRIMARY KEY,
        record_url TEXT UNIQUE,
        nr_urls INTEGER DEFAULT 0,
        nr_forms INTEGER DEFAULT 0,
        nr_unknown_encoding INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS urls (
        id INTEGER PRIMARY KEY,
        page_url TEXT,
        from_archive_id INTEGER,
        FOREIGN KEY(from_archive_id) REFERENCES archives(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS forms (
        id INTEGER PRIMARY KEY,
        form BLOB,
        from_url INTEGER,
        FOREIGN KEY(from_url) REFERENCES urls(id) ON DELETE CASCADE
);
