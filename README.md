This is a project to do streaming extraction of HTML input elements with validation patterns from the CommonCrawl dataset. It is highly experimental.


Selective logging highly suggested when gathering data:

```
$ cargo build --release
$ RUST_LOG="info,sqlx::query=warn" ./target/release/bo-cc
```

Post-analysis:
```
$ python3 -m venv .venv
$ ./.venv/bin/pip3 install selectolax
$ ./analyse-data.py with-validation | ./extract-patterns.py > patterns.txt

```
