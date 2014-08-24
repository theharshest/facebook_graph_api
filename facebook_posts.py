'''
This code downloads data from Facebook pages' feeds within a range of dates using Facebook Graph API
'''
import facebook
import pymysql
from collections import defaultdict
import datetime
import time
import sys
import os
import requests
from facepy import utils

reload(sys)
sys.setdefaultencoding('utf-8')

app_id = 0
app_secret = 0 

def get_likes_count(id, graph, err_log):
	'''
	Gets likes count for an object
	'''
	count = 0
	
	while True:
		try:
			feed = graph.get_object('/' + id + '/likes', limit = 99)
			break
		except Exception,e:
			err_log.write(str(e))
			print "Below is the error -"
			print str(e)
			print "Waiting for 5 mins and then retry.."
			time.sleep(time_to_wait)
			continue
	
	while 'data' in feed and len(feed['data'])>0:
		count = count + len(feed['data'])
		if 'paging' in feed and 'next' in feed['paging']:
			url = feed['paging']['next']
			id = url.split('graph.facebook.com')[1].split('/likes')[0]
			after_val = feed['paging']['cursors']['after']
			while True:
				try:
					feed = graph.get_object(id + '/likes', limit = 99, after=after_val)
					break
				except Exception,e:
					err_log.write(str(e))
					print "Below is the error -"
					print str(e)
					print "Waiting for 5 mins and then retry.."
					time.sleep(time_to_wait)
					continue
		else:
			break
	
	return count

def get_token(access_token):
        global app_id, app_secret
        try:
                access_token, oath_access_token_expres_when = utils.get_extended_access_token(access_token, app_id, app_secret)
        except Exception, e:
                print "Error while extending access token: ", str(e)
                exit()

        return access_token

if __name__ == "__main__":
        #Taking inputs
        app_id =  raw_input("Enter App ID:\n")
        app_secret = raw_input("Enter App Secret:\n")
        access_token = raw_input("Enter User Access Token:\n")

	# Log file
	err_log = open('error.log', 'w')

	# Time to look for new token periodically
	time_token = 900
	# Time to wait after getting an error while requesting info from Graph API
	time_to_wait = 300
	
	# Time to look for new token periodically, set to 15 mins
	time1 = time.time() + time_token
	
	# Initializing Graph API
	graph = facebook.GraphAPI(access_token)
	
	# Inputs
	url = raw_input("Enter page url (avoid trailing /):\n")
	start_date = raw_input("Enter start date(yyyy-mm-dd):\n")
	end_date = raw_input("Enter end date(yyyy-mm-dd):\n")

	d1 = datetime.datetime.now()
	
	# Verifying inputs
	start_date = datetime.datetime.strptime(start_date , '%Y-%m-%d')
	end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
	
	if start_date > end_date:
		print "ERROR: End date should be greater than start date. Exiting.."
		sys.exit()
	
	if 'http:' in url or 'https:' in url:
		r = requests.get(url)
	else:
		r = requests.get('http://' + url)
	
	if r.status_code == 404:
		print "ERROR: URL is invalid. Exiting.."
		sys.exit()
	
	# Adding 1 day offset to keep data in sync with input dates
	end_date = end_date + datetime.timedelta(days=1)
	end_date = str(end_date).split()[0]
	
	# Getting the page name from the url
	page_name = url.split('/')[-1]
	
	while True:
		try:
			page = graph.get_object('/' + page_name)
			break
		except Exception,e:
			err_log.write(str(e))
			print "Below is the error -"
			print str(e)
			print "Waiting for 5 mins and then retry.."
			time.sleep(time_to_wait)
			continue
	
	while True:
		try:
			# Getting the feed for the page, 99 posts from end_date
			feed = graph.get_object('/' + page_name + '/' + 'feed', until=end_date, limit=99)
			break
		except Exception,e:
			err_log.write(str(e))
			print "Below is the error -"
			print str(e)
			print "Waiting for 5 mins and then retry.."
			time.sleep(time_to_wait)
			continue
	
	posts = []
	
	# Storing paging info in paging_info, it is used later to switch to next page
	for k,v in feed.items():
		if k == 'paging':
			paging_info = v
		else:
			posts = v
	
	# Connecting to database server
	conn = pymysql.connect(host='db-srv.cla.umn.edu', user='username', passwd='password', db='db_name', use_unicode=True, charset='utf8')
	cur = conn.cursor()

	# Initializing values to be inserted in page info table
	p_id = '-'; name = '-'; picture = '-'; link = '-'; likes = '-'; category = '-'; website = '-'; username = '-'; founded = '-'; company_overview = '-'; products = '-'; about = '-'; phone = '-'; can_post = '-'; checkins = '-'; talking_about_count = '-'; p_type = '-'; street = '-'; city = '-'; state = '-'; country = '-'; zip = '-'; longitude = '-'; latitude = '-';

	# Count of number of posts and comments added
	num_posts = 0
	num_comments = 0
	
	# Converting start and end date to unix timestamps
	
	end_time = datetime.datetime.strptime(end_date , '%Y-%m-%d')
	
	# Getting values from the page JSON object
	for k, v in page.items():
		if k == 'id':
			p_id = v
		elif k == 'name':
			name = v
		elif k == 'picture':
			picture = v
		elif k == 'link':
			link = v
		elif k == 'likes':
			likes = v
		elif k == 'category':
			category = v
		elif k == 'website':
			website = v
		elif k == 'username':
			username = v
		elif k == 'founded':
			founded = v
		elif k == 'company_overview':
			company_overview = v
		elif k == 'products':
			products = v
		elif k == 'about':
			about = v
		elif k == 'phone':
			phone = v
		elif k == 'can_post':
			can_post = v
		elif k == 'checkins':
			checkins = v
		elif k == 'talking_about_count':
			talking_about_count = v
		elif k == 'type':
			p_type = v
		elif k =='location':
			for k1, v1 in v.items():
				if k1 == 'street':
					street = v1
				elif k1 == 'city':
					city = v1
				elif k1 == 'state':
					state = v1
				elif k1 == 'country':
					country = v1
				elif k1 == 'zip':
					zip = v1
				elif k1 == 'latitude':
					latitude = v1
				elif k1 == 'longitude':
					longitude = v1
	
	# Current timestamp to inserted in the table for each entry
	inserted_time = str(datetime.datetime.now())
	
	# Populating the page table
	cur.execute("INSERT INTO public_wave2 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [p_id, name, picture, link, likes, category, website, username, founded, company_overview, products, about, phone, can_post, checkins, talking_about_count, p_type, street, city, state, country, zip, longitude, latitude, inserted_time])
	
	# Flag to break loop when post date goes past the start_date
	flag = 1
	
	while posts:
		# Update token if time expires
		if time.time() > time1:
			time1 = time.time() + time_token
			access_token = get_token(access_token)
			graph = facebook.GraphAPI(access_token)
			
		for post in posts:
			# Update token if time expires
			if time.time() > time1:
				time1 = time.time() + time_token
				access_token = get_token(access_token)
				graph = facebook.GraphAPI(access_token)
			
			# Initializing values to be inserted in posts table
			post_id = '-'; from_name = '-'; from_category = '-'; from_id = '-'; message = '-'; post_picture = '-'; post_link = '-'; post_name = '-'; post_caption = '-'; post_description = '-'; post_icon = '-'; created_time = '-'; updated_time = '-'; shares_count = '-'; likes_count = '-'; comments_count = 0; post_type = '-'; post_source = '-';
			
			# Getting values from the post JSON object
			for k, v in post.items():
				if k == 'id':
					post_id = v
				elif k == 'from':
					for k1, v1 in v.items():
						if k1 == 'name':
							from_name = v1
						elif k1 == 'category':
							from_category = v1
						elif k1 == 'id':
							from_id = v1
				elif k == 'message':
					message = v
				elif k == 'picture':
					post_picture = v
				elif k == 'link':
					post_link = v
				elif k == 'name':
					post_name = v
				elif k == 'caption':
					post_caption = v
				elif k == 'description':
					post_description = v
				elif k == 'icon':
					post_icon = v
				elif k == 'created_time':
					created_time = v
				elif k == 'updated_time':
					updated_time = v
				elif k == 'type':
					post_type = v
				elif k == 'source':
					post_source = v
				elif k == 'shares':
					for k1, v1 in v.items():
						if k1 == 'count':
							shares_count = v1
				elif k == 'likes':
					for k1, v1 in v.items():
						if k1 == 'count':
							likes_count = v1
				elif k == 'comments':
					for k1, v1 in v.items():
						if k1 == 'count':
							comments_count = v1
			
			# Getting post time in unix timestamp
			post_date = datetime.datetime.strptime(created_time[:10] , '%Y-%m-%d')
			
			# Getting likes count
			likes_count = get_likes_count(post_id, graph, err_log)
			
			# Break if post created_time is past start_date
			if post_date < start_date:
				flag = 0
				break
			
			num_posts = num_posts + 1
			inserted_time = str(datetime.datetime.now())
			
			while True:
				if time.time() > time1:
					time1 = time.time() + time_token
					access_token = get_token(access_token)
					graph = facebook.GraphAPI(access_token)
				try:
					comments = graph.get_object('/' + post_id + '/comments', limit=99)
					break
				except Exception,e:
					err_log.write(str(e))
					print "Below is the error -"
					print str(e)
					print "Waiting for 5 mins and then retry.."
					time.sleep(time_to_wait)
					continue
					
			while True:
				if time.time() > time1:
					time1 = time.time() + time_token
					access_token = get_token(access_token)
					graph = facebook.GraphAPI(access_token)
				if 'data' in comments:
					for comment in comments['data']:
						comment_id = '-'; comment_name = '-'; comment_category = '-'; comment_from_id = '-'; comment_message = '-'; comment_created = '-'; comment_likes = '-'; comment_type = '-';
						for k, v in comment.items():
							if k == 'id':
								comment_id = v
							elif k == 'from':
								for k1, v1 in v.items():
									if k1 == 'name':
										comment_name = v1
									elif k1 == 'category':
										comment_category = v1
									elif k1 == 'id':
										comment_from_id = v1
							elif k == 'message':
								comment_message = v
							elif k == 'created_time':
								comment_created = v
							elif k == 'likes':
								comment_likes = v
								
						num_comments = num_comments + 1
						inserted_time = str(datetime.datetime.now())
						
						comments_count = comments_count + 1
						
						# Getting likes count
						comment_likes = get_likes_count(comment_id, graph, err_log)
						
						cur.execute("INSERT INTO posts_comments_wave2 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ",[comment_id, post_id, comment_name, comment_category, comment_from_id, comment_message, comment_created, comment_likes, page_name, comment_type, inserted_time])
			
				if 'paging' in comments and 'next' in comments['paging']:
					while True:
						try:
							# Getting the feed for the comments, 99 comments per batch
							comments = graph.get_object('/' + post_id + '/comments', after=comments['paging']['cursors']['after'], limit=99)
							break
						except Exception,e:
							err_log.write(str(e))
							print "Below is the error -"
							print str(e)
							print "Waiting for 5 mins and then retry.."
							time.sleep(time_to_wait)
							continue
				else:
					break
			
			# Populating the posts table
			cur.execute("INSERT INTO posts_wave2 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [post_id, from_name, from_category, from_id, message, post_picture, post_link, post_name, post_caption, post_description, post_icon, created_time, updated_time, shares_count, likes_count, comments_count, p_id, post_type, post_source, inserted_time])
			
		conn.commit()
		print num_posts, 'posts added till now'
		print num_comments, 'comments added till now'
		print '-----------------------------------\n'
	
		# Break if post created_time is past start_date
		if not flag:
			break
		
		# Checking if next page is available or this is the last page
		if 'next' in paging_info:
			until_val = paging_info['next'].split('until=')[1]
		
			while True:
				try:
					# Getting eed of the next page
					feed = graph.get_object('/' + page_name + '/' + 'feed', until=until_val, limit=99)
					break
				except Exception,e:
					err_log.write(str(e))
					print "Below is the error -"
					print str(e)
					print "Waiting for 5 mins and then retry.."
					time.sleep(time_to_wait)
					continue

			# Storing paging info in paging_info, it is used later to switch to next page
			for k,v in feed.items():
				if k == 'paging':
					paging_info = v
				else:
					posts = v
		
	print 'Total', num_posts, 'posts added'
	print 'Total', num_comments, 'comments added'
	
	d2 = datetime.datetime.now()
	
	print 'Time taken', (d2-d1)
	
	cur.close()
	conn.close()
