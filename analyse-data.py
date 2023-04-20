#!/usr/bin/env python3
import lzma
import sqlite3
import sys

OUTPUT_DB_FILE = "form_validation.db"
RO_SQLITE_URI  = f"file:{OUTPUT_DB_FILE}?mode=ro"

def count_forms(cur):
    cur.execute(
        """
    SELECT COUNT(DISTINCT record_url), sum(nr_forms) FROM archives
    """
    )
    nr_records, nr_forms = cur.fetchone()

    cur.execute("""
            SELECT count(forms.id) as nr_forms_with_patterns FROM forms
            JOIN urls ON urls.id = forms.from_url
            JOIN archives ON urls.from_archive_id = archives.id;
            """)
    (nr_interesting, ) =  cur.fetchone()
    print(
        f"Saw {nr_records:n} WARCs with {nr_forms:n} forms: {nr_forms/nr_records:n} forms/WARC."
    )
    print(
        f"{nr_interesting:n} forms with validation ({nr_interesting/nr_forms*100:.2n} %)."
    )

    cur.execute("SELECT sum(nr_urls) from archives")
    (nr_urls,) = cur.fetchone()
    print(f"Saw {nr_urls} urls: {nr_forms/nr_urls:.2n} form(s)/URL.")


def forms_with_validation(cur):
    cur.execute("SELECT form from forms")
    for (form,) in cur:
        yield lzma.decompress(form).decode("utf-8")


def main():
    OK_MODES = ["stats", "with-validation"]
    try:
        _, mode = sys.argv
        mode = mode.lower()
        if mode not in OK_MODES:
            raise ValueError(f"Invalid command {mode}")
    except ValueError as e:
        print(e)
        print(f"Usage {sys.argv[0]}: {' | '.join(OK_MODES)}")
        sys.exit(1)

    with sqlite3.connect(RO_SQLITE_URI, uri=True) as con:
        cur = con.cursor()
        if mode == "stats":
            count_forms(cur)
        elif mode == "with-validation":
            for form in forms_with_validation(cur):
                print(form.replace("\n", "").replace("\r", ""))

if __name__ == "__main__":
    main()
