UPDATE archives SET nr_urls = nr_urls + 1, nr_unknown_encoding = nr_unknown_encoding + 1 WHERE id = $1;
