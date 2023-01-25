import os
import sqlite3
import csv
import time
from tqdm import tqdm

csv.field_size_limit(1000000)
pwd = os.getcwd()

con = sqlite3.connect(pwd + "/db/e6.db")
cur = con.cursor()

try:
	cur.execute("CREATE TABLE tags (tag text PRIMARY KEY UNIQUE, category integer, count integer)")
	cur.execute("CREATE TABLE alias (old text, new text)")
	cur.execute("CREATE TABLE implicate (tag text, new text)")
	cur.execute("CREATE TABLE implicate_lut (tag text PRIMARY KEY UNIQUE, new text)")
	cur.execute("CREATE TABLE posts (post integer PRIMARY KEY UNIQUE, hash text, rating text, tags text, ext text, score integer)")
	con.commit()
except:
	pass

# Process the tags.csv
with open(pwd + "/db/tags.csv", "r", encoding="utf-8") as tagscsv:
	dr = csv.DictReader(tagscsv)
	for i in tqdm(dr, desc="tags"):
		tag = str(i["name"])
		cat = int(i["category"])
		cnt = int(i["post_count"])
		if cnt > 0:
			cur.execute(f"INSERT OR IGNORE INTO tags (tag, category, count) VALUES (?, ?, ?)", (tag, cat, cnt,))
	
con.commit()

# Process the tag_aliases.csv
with open(pwd + "/db/tag_aliases.csv", "r", encoding="utf-8") as aliascsv:
	dr = csv.DictReader(aliascsv)
	for i in tqdm(dr, desc="alias"):
		old = str(i["antecedent_name"])
		new = str(i["consequent_name"])
		state = str(i["status"])
		if state in ["active", "pending"]:
			cur.execute(f"INSERT INTO alias (old, new) VALUES (?, ?)", (old, new,))

con.commit()

# Process the tag_implications.csv
with open(pwd + "/db/tag_implications.csv", "r", encoding="utf-8") as implicsv:
	dr = csv.DictReader(implicsv)
	for i in tqdm(dr, desc="implication"):
		old = str(i["antecedent_name"])
		add = str(i["consequent_name"])
		state = str(i["status"])
		if state in ["active", "pending"]:
			cur.execute(f"INSERT INTO implicate (tag, new) VALUES (?, ?)", (old, add,))

con.commit()

# Things
cur.execute("SELECT * FROM tags")
tags = cur.fetchall()
lres = len(tags)
init_time = time.perf_counter()
avg_times = []
console_len = ""

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

# Scan for an existing alias
def get_tag_alias(cur, _tag):
	query = f'SELECT COUNT(1) FROM alias WHERE old=?'
	for row in cur.execute(query, (_tag,)):
		if not row[0]:
			return _tag

	# If there is an alias, use that
	query = f'SELECT * FROM alias WHERE old=?'
	for row in cur.execute(query, (_tag,)):
		return row[1]

# And also construct a simple lookup table:
numimg = 0
for row in tags:
	loop_start = time.perf_counter()
	tag = get_tag_alias(cur, row[0])
	
	processed_tags = [tag]
	query = f'SELECT * FROM implicate WHERE tag=?'
	for row in cur.execute(query, (tag,)):
		processed_tags.append(row[1])

	implicates = processed_tags
	for itag in processed_tags:
		# Append any and all tags
		for row in cur.execute(query, (itag,)):
			implicates.append(row[1])
	
	# Deduplicate
	implicates = list(dict.fromkeys(implicates))
	implicates.sort(key=str.lower)

	tag_out = ""
	for itag in implicates:
		# Only add implicates, not the root tag
		if tag != itag:
			tag_out = tag_out + f"{itag} "
	
	tag_out = tag_out.strip()

	query = "INSERT OR IGNORE INTO implicate_lut (tag, new) VALUES (?, ?)"
	if tag_out != "":
		cur.execute(query, (tag, tag_out))

	loop_end = time.perf_counter()
	old_len = len(console_len)
	tot_time = loop_end - init_time
	loop_time = loop_end - loop_start
	avg_times.append(loop_time)
	true_avg = get_list_avg(avg_times)
	bar, end = progress_bar(numimg, lres, bar_length=30)
	console_len = f" {bar} D:{numimg}/{lres} [{get_time(tot_time)}<{get_time((true_avg)*(lres-numimg))}, {loop_time:.2f}s/implicate_lut]"
	new_len = len(console_len)
	if old_len > new_len:
		console_len = console_len + (old_len - new_len) * " "
		
	if numimg != lres:
		end = "\r"
	else:
		end = "\n"
	print(console_len, end=end)
	numimg += 1

print("\n")
con.commit()

# Process the posts.csv
with open(pwd + "/db/posts.csv", "r", encoding="utf-8") as postscsv:
	dr = csv.DictReader(postscsv)
	for i in tqdm(dr, desc="posts"):
		id = int(i["id"])
		md5 = str(i["md5"])
		rating = str(i["rating"])
		tags = str(i["tag_string"])
		ext = str(i["file_ext"])
		status = str(i["is_deleted"])
		score = int(i["score"])

		if status == "f":
			cur.execute(f"INSERT OR IGNORE INTO posts (post, hash, rating, tags, ext, score) VALUES (?, ?, ?, ?, ?, ?)", (id, md5, rating, tags, ext, score))

con.commit()