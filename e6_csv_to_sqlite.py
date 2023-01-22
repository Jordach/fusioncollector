import os
import sqlite3
import csv
from tqdm import tqdm

csv.field_size_limit(1000000)
pwd = os.getcwd()

con = sqlite3.connect(pwd + "/db/e6.db")
cur = con.cursor()

try:
	cur.execute("CREATE TABLE tags (tag text PRIMARY KEY UNIQUE, category integer, count integer)")
	cur.execute("CREATE TABLE alias (old text, new text)")
	cur.execute("CREATE TABLE implicate (tag text, new text)")
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