from bs4 import BeautifulSoup


DEFAULT_HTML_NAMES_CHANGED = '/home/rafael/Temp/scielo/wos-jcr-names-changed.html'
DEFAULT_TSV_NAMES_CHANGED = 'wos-jcr-names-changed.csv'

if __name__ == '__main__':
    file_names_changed = open(DEFAULT_HTML_NAMES_CHANGED)
    soup_html = BeautifulSoup(file_names_changed.read(), 'html.parser')

    file_results = open(DEFAULT_TSV_NAMES_CHANGED, 'w')
    filtered_lines = []
    for i in soup_html.find_all('tr'):
        splitted_line = i.text.strip().split('\n')
        cleaned_line = [e.strip() for e in splitted_line if e != '']
        file_results.write('\t'.join(cleaned_line) + '\n')
