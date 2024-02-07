This is a project to do streaming extraction of HTML input elements with validation patterns from the CommonCrawl dataset. It is highly experimental.


Selective logging highly suggested when gathering data:

```
$ cargo build --release
$ ./target/release/bo-cc CC-MAIN-2023-40
```

Post-analysis:
```
$ ./target/release/bo-analyse patterns
<a long list of patterns>
$ ./target/release/bo-analyse summary
```
