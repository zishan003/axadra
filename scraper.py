from lib import scraperEngine

def main():
	scrape = scraperEngine(config)
	scrape.importIntoDb(scrape.movie_scraper())

if __name__ == '__main__':
	config= {
				"host": "127.0.0.1",
				"user": "root",
				"database": "test",
				"password":"",
				"url": "https://www.themoviedb.org/movie"
			}
	main()
