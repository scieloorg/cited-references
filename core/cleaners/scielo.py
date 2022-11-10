import argparse
import csv
import logging


from scielo_scholarly_data import standardizer


def get_scielo_issn_dict(path):
	data = {}

	with open(path) as fin:
		cr = csv.DictReader(fin, delimiter='|')

		for row in cr:
			sissn = row['scielo_issn']
			eissn = row['electronic_issn']
			pissn = row['print_issn']
			if sissn not in data:
				data[sissn] = row
			if eissn not in data:
				data[eissn] = row
			if pissn not in data:
				data[pissn] = row
			else:
				...

	return data


def get_year_volume(path, scielo2journal):
	data = []
	missing_issns = set()

	with open(path) as fin:
		cr = csv.DictReader(fin, delimiter='|')
		
		for row in cr:
			std_date = str(standardizer.document_publication_date(row['publication_date'], only_year=True))
			std_vol = row['volume']
			scielo_issn = row['scielo_issn']

			try:
				row_journal_data = scielo2journal[scielo_issn]
			except KeyError:
				if scielo_issn not in missing_issns:
					missing_issns.add(scielo_issn)
					print(scielo_issn)
				continue

			issns = [row_journal_data.get(k) for k in [
				'scielo_issn', 
				'print_issn', 
				'electronic_issn'
			] if row_journal_data.get(k) is not None]

			titles = [row_journal_data.get(k) for k in  [
				'title', 
				'abbreviated_iso_title', 
				'abbreviated_title', 
				'acronym', 
				'title_nlm', 
				'subtitle', 
				'previous_title', 
				'next_title', 
				'other_titles'
			] if row_journal_data.get(k) is not None]

			for i in issns:
				for t in titles:
					std_title = standardizer.journal_title_for_deduplication(t).upper()

					if i and std_title:
						line = [
							i,
							std_title,
							std_date,
							std_vol,
						]
						data.append(line)
	return data


def save(output_path, data):
	with open(output_path, 'w') as fout:
		for d in data:
			fout.write('|'.join(d) + '\n')


def main():
	parser = argparse.ArgumentParser()

	parser.add_argument('--path_scielo_journals', required=True)
	parser.add_argument('--path_scielo_issues', required=True)
	parser.add_argument('--path_output', required=True)

	args = parser.parse_args()

	logging.info('Lendo dados de periódicos...')
	issn2jids = get_scielo_issn_dict(args.path_scielo_journals)

	logging.info('Lendo dados de fascículos e gerando variações com dados de periódicos')
	year_volume_data = get_year_volume(args.path_scielo_issues, issn2jids)

	logging.info(f'Gravando dados em {args.path_output}')
	save(args.path_output, year_volume_data)


if __name__ == '__main__':
	main()
