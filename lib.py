from bs4 import BeautifulSoup
import requests, pymysql
from lxml import etree

class databaseEngine():

	def __init__(self, config_dict):
		self.config_dict = config_dict
		self.conn = self.connectDatabse()
		self.url = config_dict['url']

	def connectDatabse(self):
		try:
			conn = pymysql.connect(
			    host = self.config_dict['host'],
			    user = self.config_dict['user'],
			    database = self.config_dict['database'],
			    password = self.config_dict['password']
				)
			
			return conn
		except Exception as e:
			print("Conn Error:- ", e)
			pass

	def runQuery(self, sql):
		try:
			cursor = self.conn.cursor()
			cursor.execute(sql)
			self.conn.commit()
			# print("executed successfully")
		except Exception as e:
			print("runQuery ", e)
			pass

	def fetchAll(self, sql):
		try:
			my_database = self.conn.cursor(pymysql.cursors.DictCursor)
			my_database.execute(sql)
			output = my_database.fetchall()
			return output
		except Exception as e:
			print("Fetch:- ", e)
	
	def creatTable(self):
		# drop table first
		self.runQuery("Drop table if exists movies")
		self.runQuery("Drop table if exists movie_details")

		sql=''' 
				CREATE TABLE IF NOT EXISTS movies 
				(
				  `id` int(11) NOT NULL,
				  `movie_id` int(11) DEFAULT NULL,
				  `title` text DEFAULT NULL,
				  `scrape_date` text DEFAULT NULL,
				   `created_at` datetime NOT NULL DEFAULT current_timestamp()
				)
				'''
		self.runQuery(sql)
		# add primay key
		self.runQuery('''ALTER TABLE `movies` ADD PRIMARY KEY (`id`);''')

		# set auto increment
		self.runQuery('''ALTER TABLE `movies` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;''')


		# details table
		sql=''' 
				CREATE TABLE IF NOT EXISTS movie_details 
				(
				  `id` int(11) NOT NULL,
				  `movie_id` int(11) DEFAULT NULL,
				  `user_score` int(11) DEFAULT NULL,
				  `overview` text DEFAULT NULL,
				  `status` varchar(255) DEFAULT NULL,
				  `keyword` text DEFAULT NULL,
				  `facebook_url` text DEFAULT NULL,
				  `twitter_url` text DEFAULT NULL,
				  `instagram_url` text DEFAULT NULL,
				  `website_url` text DEFAULT NULL
				);

				'''
		self.runQuery(sql)

		# add primay key
		self.runQuery('''ALTER TABLE `movie_details` ADD PRIMARY KEY (`id`);''')

		# set auto increment
		self.runQuery('''ALTER TABLE `movie_details` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;''')

	def importIntoDb(self, data):
		try:
			self.creatTable()


			# insert into movies table
			sql ='''Insert into movies (movie_id, title, scrape_date) values (%s, %s, %s); '''
			query_list = []
			for sub_data in data:
				query_list.append(
					(
						sub_data['movie_id'],
						sub_data['title'],
						sub_data['date'],
					)
				)

			cursor = self.conn.cursor()
			cursor.executemany(sql, query_list)
			self.conn.commit()

			# insert into details
			sql ='''Insert into movie_details (movie_id, user_score, overview, status, 
												keyword, facebook_url, twitter_url,
												instagram_url, website_url
											) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
					;
				'''
			query_list = []
			for sub_data in data:
				query_list.append(
					(
						sub_data['movie_id'],
						sub_data['user_score'],
						sub_data['overview'],
						sub_data['status'],
						sub_data['keyword'],
						sub_data['facebook'],
						sub_data['twitter'],
						sub_data['instagram'],
						sub_data['website'],
					)
				)

			cursor = self.conn.cursor()
			cursor.executemany(sql, query_list)
			self.conn.commit()
			print("Data imported into db Successfully ", len(data))

		except Exception as e:
			print("Import Error: ", e)
			pass

class scraperEngine(databaseEngine):

	def __init__(self, config_dict):
		super(scraperEngine, self).__init__(config_dict)
		self.movie_limit = 20

	def hit_url(self, url):
		try:
			headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36', 'Accept-Encoding': 'gzip, deflate', 'Accept': '*/*', 'Connection': 'keep-alive'}
			resp_ = requests.get(url, headers=headers)
			if resp_.status_code == 200:
				return resp_.content
		except Exception as e:
			print("error at hit_url:- ", e, " check conection once")

	def movie_scraper(self):
		soup = BeautifulSoup(self.hit_url(self.url), 'html.parser')
		dom = etree.HTML(str(soup))

		result = []
		for count in range(1, self.movie_limit+1):
			try:
				data = {}
				data['movie_id'] = dom.xpath(f"/html/body/div[1]/main/section/div/div/div/div[2]/div[2]/div/section/div/div/div[{count}]/div[1]/div[2]")[0].attrib['data-id']
				data['title'] = dom.xpath(
					f'/html/body/div[1]/main/section/div/div/div/div[2]/div[2]/div/section/div/div/div[{count}]/div[2]/h2/a')[
					0].attrib['title']

				data['date'] = dom.xpath(f'/html/body/div[1]/main/section/div/div/div/div[2]/div[2]/div/section/div/div/div[{count}]/div[2]/p')[
					0].text

				data = {**data, **self.movie_details(f"{self.url}/{data['movie_id']}")}
				result.append(data)
			except Exception as e:
				print(e)
				pass
		return result

	def movie_details(self, url):

		# Parsing the HTML
		soup = BeautifulSoup(self.hit_url(url), 'html.parser')
		dom = etree.HTML(str(soup))
		movie_name = dom.xpath('/html/body/div[1]/main/section/div[2]/div/div/section/div[2]/section/div[1]/h2/a')[0].text

		# user_score
		user_score = dom.xpath('/html/body/div[1]/main/section/div[2]/div/div/section/div[2]/section/ul/li[1]/div[1]/div/div/div/span')[0].attrib['class']
		user_score = user_score.split("-")[-1][1:]

		# overview
		overview = dom.xpath('/html/body/div[1]/main/section/div[2]/div/div/section/div[2]/section/div[2]/div/p')[0].text

		# status
		status = " ".join(dom.xpath('/html/body/div[1]/main/section/div[3]/div/div/div[2]/div/section/div[1]/div/section[1]/p[1]/text()')).strip()

		# keyword
		keyword =[]
		try:
			keyword_list = dom.xpath('/html/body/div[1]/main/section/div[3]/div/div/div[2]/div/section/div[1]/div/section[2]/ul/li')
			for key in range(len(keyword_list)):
				key += 1
				keyword.append(dom.xpath(
				f'/html/body/div[1]/main/section/div[3]/div/div/div[2]/div/section/div[1]/div/section[2]/ul/li[{key}]/a')[
				0].text)
		except Exception as e:
			pass


		# socal link
		facebook, twitter, instagram, website_url = '', '', '', ''
		for link in soup.find_all(attrs={"class" : "social_link"}):
			try:
				if 'facebook' in link.attrs['href'].lower():
					facebook = link.attrs['href']
				
				if 'twitter' in link.attrs['href'].lower():
					twitter = link.attrs['href']
				
				if 'instagram' in link.attrs['href'].lower():
					instagram = link.attrs['href']
				
				if 'homepage' in link.attrs['title'].lower():
					website_url = link.attrs['href']

			except Exception as e:
				print("xpath_ ",e)
				pass


		details = {"movie_name":movie_name, "user_score": user_score, "overview":overview,
				"status": status,  "keyword":"|".join(keyword), "facebook":facebook,
				"twitter":twitter, "instagram":instagram, "website":website_url }
		return details
