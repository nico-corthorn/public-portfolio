
import webScraper


def main():

	crawlers = {
		'iex': webScraper.IexScraper(),
		'tiingo': webScraper.TiingoScraper(),
		'sec': webScraper.SecScraper()}

	# Nothing fancy yet for this part
	crawler = crawlers['tiingo']
	crawler.build()
	crawler.compute()

	crawler = crawlers['sec']
	crawler.build()
	crawler.compute()

	# Outliers

	# Regressions

	# Forecast alpha

	# Forecast risk

	# Optimizers


if __name__ == "__main__":
	main()










