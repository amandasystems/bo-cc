#!.venv/bin/python3
import sys
from selectolax.parser import HTMLParser

PATTERN_ATTRIBUTES = ["pattern", "data-val-regex-pattern", "ng-pattern"]
PATTERN_SELECTOR = ",".join(f"input[{a}]" for a in PATTERN_ATTRIBUTES)


def get_interesting_property(e):
    for attribute in PATTERN_ATTRIBUTES:
        if v := e.attributes.get(attribute, None):
            return v
    return "" # The property existed, but was empty!


if __name__ == "__main__":
    for txt_form in sys.stdin:
        form = HTMLParser(txt_form)
        for elem in form.css(PATTERN_SELECTOR):
            print(get_interesting_property(elem))
