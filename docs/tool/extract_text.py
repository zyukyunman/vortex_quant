from html.parser import HTMLParser
import glob

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.texts = []
        self.skip = False
        self.skip_tags = {'script', 'style', 'noscript'}
        self.in_article = False
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        cls = attrs_dict.get('class', '')
        if 'article__bd__detail' in cls:
            self.in_article = True
        if tag in self.skip_tags:
            self.skip = True
    
    def handle_endtag(self, tag):
        if tag in self.skip_tags:
            self.skip = False
    
    def handle_data(self, data):
        if self.in_article and not self.skip:
            text = data.strip()
            if text:
                self.texts.append(text)

files = sorted(glob.glob('*.html'))
for f in files:
    print('=' * 80)
    print(f'FILE: {f[:80]}')
    print('=' * 80)
    with open(f, 'r', encoding='utf-8') as fh:
        content = fh.read()
    parser = TextExtractor()
    parser.feed(content)
    print('\n'.join(parser.texts))
    print()
