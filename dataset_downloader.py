import os
import e621py_wrapper as e621
import requests
import sqlite3
import argparse
import time
import pprint
pp = pprint.PrettyPrinter(indent=4)
# Arguments

pwd = os.getcwd()

parser = argparse.ArgumentParser(description="Downloader for e6 based on tag for machine learning models")
parser.add_argument("query", help="The tag(s) you wish to download.")
parser.add_argument("--out", help="Where the downloaded images and tags reside. Separate from the DB.", required=True)
parser.add_argument("--unique", help="How many new (not duplicate) images should we download", type=int, required=True)
parser.add_argument("--db", help="Location of the DB to track already downloaded images.", required=False, default=pwd + "/db/downloaded.db")
parser.add_argument("--list", help="Path to the list in which finished tags are added to.", required=False, default=pwd + "/downloaded_tags.txt")
args = parser.parse_args()
print(args)

if args.query == "":
	Exception("No tags supplied, stopping.")

if args.out == "" or args.out == None:
	Exception("No directory supplied, stopping.")

# Strip any directory names ending with a slash
if args.out[len(args.out)-1] == "/" or args.out[len(args.out)-1] == "\\":
	args.out = args.out[:-1]

if not os.path.isdir(args.out):
	os.mkdir(args.out)

# Create an empty file to track downloading states

def download_status(status):
	with open("downloader_status.conf", "w") as file:
		file.write(status)

# Database stuff
def load_db():
	__con = sqlite3.connect(args.db)
	__cur = __con.cursor()
	return __con, __cur

_con, _cur = load_db()

try:
	_cur.execute("CREATE TABLE posts (id integer PRIMARY KEY UNIQUE)")
	_con.commit()
except:
	pass
_con.close()

# The downloader portion

e6_token = ""
e6_username = ""

# Load the token and username
if not os.path.isfile(pwd + "/downloader.conf"):
	with open("downloader.conf") as file:
		file.write("")
		raise Exception("Downloader token and username are missing")

with open("downloader.conf", "r") as conf:
	conf_lines = conf.readlines()
	i = 1
	for line in conf_lines:
		if i == 1:
			e6_token = line.strip()
		else:
			e6_username = line.strip()
			break
		i += 1

	if e6_token == "":
		with open("downloader.conf") as file:
			file.write("")
			raise Exception("e6 token is missing")
	if e6_username == "":
		with open("downloader.conf") as file:
			file.write("")
			raise Exception("e6 username is missing")

# Basic tag pruning lists
if not os.path.isfile(pwd + "/tag_meta_whitelist.conf"):
	with open(pwd + "/tag_meta_whitelist.conf") as file:
		file.write("")

meta_whitelist = []
with open(pwd + "/tag_meta_whitelist.conf", "r", encoding="utf-8") as meta_file:
	tags = meta_file.readlines()
	for tag in tags:
		new_tag = tag.strip().lower()
		if new_tag not in meta_whitelist:
			meta_whitelist.append(new_tag)

if not os.path.isfile(pwd + "/tag_species_blacklist.conf"):
	with open(pwd + "/tag_species_blacklist.conf") as file:
		file.write("")

species_blacklist = []
with open(pwd + "/tag_species_blacklist.conf", "r", encoding="utf-8") as species_file:
	tags = species_file.readlines()
	for tag in tags:
		new_tag = tag.strip().lower()
		if new_tag not in species_blacklist:
			species_blacklist.append(new_tag)

e6 = e621.client()
e6.login(e6_username, e6_token)

query_negatives = " order:score -animation -what -where_is_your_god_now -low_res -censored -alpha_channel -meme"
con, cur = load_db()
downloads_this_run = 0
download_status("Downloading: " + args.query.split(" ")[0])
try:
	print("Grabbing posts for: " + args.query+query_negatives)
	posts = e6.posts.search(tags=args.query+query_negatives, blacklist="", limit=15000, page=1, ignorepage=True)
	if len(posts) < args.unique:
		raise Exception("Not enough tags in search to fill the unique count. Stopping.")

	print(f"Total number of items in search: {len(posts)} / 15000")
	for post in posts:
		# Stop downloading posts once we reach the number of unique posts downloaded
		if downloads_this_run == args.unique:
			break

		# Check if post exists in DB, otherwise skip
		db_query = f"SELECT COUNT(1) FROM posts WHERE id = {post['id']}"
		perform_download = False
		for row in cur.execute(db_query):
			if not row[0]:
				perform_download = True
		
		if perform_download:
			print(f"Downloading post id: {post['id']}, {downloads_this_run} / {args.unique}")
			# Should networking failover, don't download and add to DB
			# Download the image and save it
			img = requests.get(post["file"]["url"]).content
			img_filename = f"{post['id']}.{post['file']['ext']}"
			with open(args.out + "/" + img_filename, "wb") as file:
				file.write(img)

			# Parse tags and remove certain tags that influence tag bleeding
			tag_filename = f"{post['id']}.txt"
			all_tags = []

			if post["rating"] == "e":
				all_tags.append("explicit")
			elif post["rating"] == "q":
				all_tags.append("questionable")
			elif post["rating"] == "s":
				all_tags.append("safe")

			for tag in post["tags"]["artist"]:
				if tag != "conditional_dnp":
					all_tags.append(str(tag))

			for tag in post["tags"]["character"]:
				all_tags.append(str(tag))

			for tag in post["tags"]["general"]:
				all_tags.append(str(tag))

			for tag in post["tags"]["meta"]:
				if tag in meta_whitelist:
					all_tags.append(str(tag))
			
			for tag in post["tags"]["species"]:
				if tag not in species_blacklist:
					all_tags.append(str(tag))

			all_tags.sort(key=str.lower)
			cs_tags = ""
			pos = 1
			for tag in all_tags:
				if pos == len(all_tags):
					cs_tags += tag
				else:
					cs_tags += tag + ", "
				pos += 1

			with open(args.out + "/" + tag_filename, "w", encoding="utf-8") as file:
				file.write(cs_tags)

			# Add post into DB to avoid redownloading this image and sleep a short while to avoid tripping abuse detection.
			downloads_this_run += 1
			db_query = f"INSERT OR IGNORE INTO posts (id) VALUES ({post['id']})"
			cur.execute(db_query)
			time.sleep(0.75)
		else:
			print(f"Skipping download for post id: {post['id']}, {downloads_this_run} / {args.unique}")

	initial_tag = args.query.split(" ")[0]
	if downloads_this_run == args.unique:
		print("Done downloading specified number of unique tags.")
		download_status("Completed downloading: " + initial_tag)
		with open(args.list, "a+") as file:
			file.seek(0)
			contents = file.read(100)
			if len(contents) > 0:
				file.write("\n")
			file.write(initial_tag)
	else:
		print("Something interrupted downloading or not enough pagination.")
		print(f"Status: {downloads_this_run} / {args.unique} ({args.unique-downloads_this_run} remaining.)")
		download_status("Something interrupted downloading or not enough pagination: " + initial_tag)

	con.commit()
	con.close()

except Exception as e:
	con.commit()
	con.close()
	download_status("Failed downloading: " + args.query.split(" ")[0] + "\n"+e)
	print(f"Status: {downloads_this_run} / {args.unique} ({args.unique-downloads_this_run} remaining.)")
	print("Something went wrong, stopping. What:")
	print(e)
