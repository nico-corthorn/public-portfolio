
import json
import webScraper
import dataProcessing


def initialize_crawlers(params):
	""" Initialization is done avoiding unnecessary processing. """
	crawlers = {}
	if params['scrapers']['tiingo']['activate']:
		crawlers['tiingo'] = webScraper.TiingoScraper(params['scrapers']['tiingo'], params['db'])
	if params['scrapers']['iex']['activate']:
		crawlers['iex'] = webScraper.IexScraper(params['scrapers']['iex'], params['db'])
	if params['scrapers']['sec']['activate']:
		crawlers['sec'] = webScraper.SecScraper(params['scrapers']['sec'], params['db'])
	return crawlers


def main():

	# Config
	with open('config.json') as json_file:
		params = json.load(json_file)

	# Scrapers
	crawlers = initialize_crawlers(params)
	for crawler in crawlers.values():
		crawler.build()
		crawler.compute()

	# Data processing
	if params['data_processing']['activate']:
		processor = dataProcessing.DataProcessing()
		if params['data_processing']['compute_db']:
			processor.compute_pb()

	# Outliers

	# Regressions

	# Forecast alpha

	# Forecast risk

	# Optimizers


if __name__ == "__main__":
	main()










