INSERT OR IGNORE INTO archives(record_url) VALUES($1);
SELECT id, all_records_submitted_for_analysis FROM archives WHERE record_url = $1;