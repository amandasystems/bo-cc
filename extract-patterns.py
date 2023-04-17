#!.venv/bin/python3
import sys
from selectolax.parser import HTMLParser
from collections import Counter

PATTERN_ATTRIBUTES = ["pattern", "data-val-regex-pattern", "ng-pattern"]
PATTERN_SELECTOR = ",".join(f"input[{a}]" for a in PATTERN_ATTRIBUTES)


def get_interesting_property(e):
    for attribute in PATTERN_ATTRIBUTES:
        if v := e.attributes.get(attribute, None):
            return v
    return "" # The property existed, but was empty!


if __name__ == "__main__":
    seen = Counter()
    for txt_form in sys.stdin:
        form = HTMLParser(txt_form)
        for elem in form.css(PATTERN_SELECTOR):
            seen[get_interesting_property(elem)] += 1
    for pattern, count in seen.most_common():
        print(f"{pattern}\t{count}")
