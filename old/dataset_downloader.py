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
parser.add_argument("--total", help="Target to reach with unique files.", required=True)
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

def total_files_downloaded():
	con, cur = load_db()
	cur.execute("SELECT * FROM posts")
	res = cur.fetchall()
	lres = len(res)
	print(f"Total Files: {lres} / {args.total} ({(lres/int(args.total))*100:.2f}%)")
	del res
	con.close()

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

def progress_bar(current, total, bar_length=20):
    fraction = current / total

    arrow = int(fraction * bar_length) * "█"
    padding = int(bar_length - len(arrow)) * " "

    ending = '\n' if current == total else '\r'

    return f'{int(fraction*100)}%|{arrow}{padding}|', ending

def get_time(seconds):
	if seconds >= 60*60:
		return str(time.strftime("%H:%M:%S", time.gmtime(seconds)))
	else:
		return str(time.strftime("%M:%S", time.gmtime(seconds)))

def get_list_avg(list):
	return sum(list) / len(list)

e6 = e621.client()
e6.login(e6_username, e6_token)

total_files_downloaded()
query_negatives = " order:score -young -animation -what -where_is_your_god_now -low_res -censored -alpha_channel -meme"
con, cur = load_db()
downloads_this_run = 0
download_status("Downloading: " + args.query.split(" ")[0])
down_start = time.perf_counter()
down_after = time.perf_counter()
print("Grabbing posts for: " + args.query+query_negatives)
time.sleep(10)
page_limit = 20
posts = e6.posts.search(tags=args.query+query_negatives, blacklist="", limit=args.unique * page_limit, page=1, ignorepage=True)
down_after = time.perf_counter()
print(f"Total number of items in search, Got: {len(posts)}, Requested: {args.unique * page_limit} in {get_time(down_after-down_start)}.")
try:
	if len(posts) < args.unique:
		raise Exception("Not enough tags in search to fill the unique count. Stopping.")

	console_len = ""
	iters = 1
	avg_times = []
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
			loop_start = time.perf_counter()
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
			# Console progress
			down_after = time.perf_counter()
			old_len = len(console_len)
			tot_time = down_after - down_start
			loop_time = down_after - loop_start
			avg_times.append(loop_time)
			true_avg = get_list_avg(avg_times)
			bar, end = progress_bar(downloads_this_run, args.unique, bar_length=40)
			console_len = f" {bar} D:{downloads_this_run}/{args.unique}, S:{iters-downloads_this_run}/{len(posts)}, I:{iters}/{len(posts)} [{get_time(tot_time)}<{get_time((true_avg)*(args.unique-downloads_this_run))}, {loop_time:.2f}s/img]"
			new_len = len(console_len)
			if old_len > new_len:
				console_len = console_len + (old_len - new_len) * " "
			
			if downloads_this_run != args.unique:
				end = "\r"
			else:
				end = "\n"
			print(console_len, end=end)

			# Sleep if we're going too fast
			if down_after - loop_start <= 0.5:
				time.sleep(0.5)
		else:
			pass
			#print(f"Skipping download for post id: {post['id']}, {downloads_this_run} / {args.unique} Uniques downloaded; {iters - downloads_this_run} Skipped; {iters} / {len(posts)} Posts")

		iters += 1

	initial_tag = args.query.split(" ")[0]
	if downloads_this_run == args.unique or args.unique == iters - 1 or iters - 1 == len(posts):
		print("\nDone downloading specified number of unique images.")
		download_status("Completed downloading: " + initial_tag)

		# Prune duplicates from the downloaded list
		completed_runs = []
		with open(args.list, "r") as file:
			tags = file.readlines()
			for tag in tags:
				tag = tag.strip()
				if tag not in completed_runs:
					completed_runs.append(tag)

		if initial_tag not in completed_runs:
			completed_runs.append(initial_tag)

		with open(args.list, "w") as file:
			out = ""
			for tag in completed_runs:
				out += tag + "\n"
			file.write(out)
	else:
		print("\nSomething interrupted downloading or not enough pagination.")
		print(f"Status: {downloads_this_run} / {args.unique} ({args.unique-downloads_this_run} remaining.)")
		download_status("Something interrupted downloading or not enough pagination: " + initial_tag)

	con.commit()
	con.close()
except Exception as e:
	print("\n")
	con.commit()
	con.close()
	download_status("\nFailed downloading: " + args.query.split(" ")[0] + "\n")
	print(f"Status: {downloads_this_run} / {args.unique} ({args.unique-downloads_this_run} remaining.)")
	print("Something went wrong, stopping. What:")
	print(e)