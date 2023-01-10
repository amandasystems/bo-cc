CREATE TABLE IF NOT EXISTS urls
                (id INTEGER PRIMARY KEY,
                page_url TEXT,
                from_archive_id INTEGER,
                nr_forms_total INTEGER,
                FOREIGN KEY(from_archive_id) REFERENCES archives(id));

CREATE TABLE IF NOT EXISTS forms
        (id INTEGER PRIMARY KEY,
        form BLOB,
        from_url INTEGER,
        has_pattern INTEGER,
        FOREIGN KEY(from_url) REFERENCES urls(id));

CREATE TABLE IF NOT EXISTS archives
                (id INTEGER PRIMARY KEY,
                 record_url TEXT,
                 nr_urls INTEGER,
                 nr_forms INTEGER,
                 all_records_submitted_for_analysis INTEGER DEFAULT 0,
                 nr_interesting_forms INTEGER);