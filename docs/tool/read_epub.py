#!/usr/bin/env python3
"""Extract text content from the epub book."""

import zipfile
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
import re


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ('style', 'script'):
            self.skip = True

    def handle_endtag(self, tag):
        if tag in ('style', 'script'):
            self.skip = False
        if tag in ('p', 'div', 'h1', 'h2', 'h3', 'h4', 'br', 'li', 'tr'):
            self.text.append('\n')

    def handle_data(self, data):
        if not self.skip:
            stripped = data.strip()
            if stripped:
                self.text.append(stripped)


def extract_toc(z):
    """Extract table of contents."""
    toc_data = z.read('OEBPS/toc.ncx').decode('utf-8')
    root = ET.fromstring(toc_data)
    ns = 'http://www.daisy.org/z3986/2005/ncx/'

    print("=" * 60)
    print("TABLE OF CONTENTS")
    print("=" * 60)

    for nav in root.iter(f'{{{ns}}}navPoint'):
        label_el = nav.find(f'{{{ns}}}navLabel/{{{ns}}}text')
        content_el = nav.find(f'{{{ns}}}content')
        if label_el is not None and label_el.text:
            src = content_el.get('src', '') if content_el is not None else ''
            print(f"  {label_el.text}  [{src}]")


def extract_html_text(html_content):
    """Extract plain text from HTML."""
    parser = TextExtractor()
    parser.feed(html_content)
    text = ' '.join(parser.text)
    # Clean up whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    return text.strip()


def main():
    epub_file = '量化投资技术分析实战：解码股票与期货交易模型 (濮元恺).epub'

    with zipfile.ZipFile(epub_file, 'r') as z:
        # Extract TOC
        extract_toc(z)
        print()

        # Extract all text chapters
        html_files = sorted([f for f in z.namelist() if f.endswith('.html')])

        for html_file in html_files:
            html_content = z.read(html_file).decode('utf-8')
            text = extract_html_text(html_content)
            if text and len(text) > 50:  # Skip near-empty files
                print("=" * 60)
                print(f"FILE: {html_file}")
                print("=" * 60)
                print(text[:8000])  # Print first 8000 chars per chapter
                if len(text) > 8000:
                    print(f"\n... [TRUNCATED, total {len(text)} chars] ...")
                print()


if __name__ == '__main__':
    main()
