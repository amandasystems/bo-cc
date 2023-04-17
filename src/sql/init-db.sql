CREATE TABLE IF NOT EXISTS archives (
        id SERIAL PRIMARY KEY,
        record_url TEXT UNIQUE,
        nr_urls INTEGER DEFAULT 0,
        nr_forms INTEGER DEFAULT 0,
        nr_unknown_encoding INTEGER DEFAULT 0,
        all_records_submitted_for_analysis BOOL DEFAULT false
);

CREATE TABLE IF NOT EXISTS urls (
        id SERIAL PRIMARY KEY,
        page_url TEXT UNIQUE,
        from_archive_id INTEGER,
        nr_forms_total INTEGER,
        FOREIGN KEY(from_archive_id) REFERENCES archives(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS forms (
        id SERIAL PRIMARY KEY,
        form BYTEA,
        from_url INTEGER,
        FOREIGN KEY(from_url) REFERENCES urls(id) ON DELETE CASCADE
);

DELETE FROM archives WHERE all_records_submitted_for_analysis = false;
