import os
import sqlite3
import csv
import time
from tqdm import tqdm

csv.field_size_limit(1000000)
pwd = os.getcwd()

con = sqlite3.connect(":memory:")
cur = con.cursor()

cur.execute("CREATE TABLE tags (tag text PRIMARY KEY UNIQUE, category integer, count integer)")
cur.execute("CREATE TABLE alias (old text, new text)")
cur.execute("CREATE TABLE implicate (tag text, new text)")
cur.execute("CREATE TABLE implicate_lut (tag text PRIMARY KEY UNIQUE, new text)")
con.commit()

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
		if state in ["active", "approved"]:
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

def get_tag_alias(cur, _tag):
	# Scan for an existing alias
	query = f"SELECT COUNT(1) FROM alias WHERE old LIKE '{_tag}' ESCAPE '\\'"
	for row in cur.execute(query):
		if not row[0]:
			return _tag

	# If there is an alias, use that
	query = f"SELECT * FROM alias WHERE old LIKE '{_tag}' ESCAPE '\\'"
	for row in cur.execute(query):
		return row[1]

def recursive_implicate_walk(_cur, _tag, tag_dict):
	esc_tag = _tag.replace("_", "\\_")
	query = f"SELECT COUNT(1) FROM implicate WHERE tag LIKE '{esc_tag}' ESCAPE '\\'"
	for row in _cur.execute(query):
		if not row[0]:
			return tag_dict

	# Let's look for a tag:
	query = f"SELECT * FROM implicate WHERE tag LIKE '{esc_tag}' ESCAPE '\\'"
	_cur.execute(query)
	tags = _cur.fetchall()
	for row in tags:
		al_row = row[1]
		if al_row in tag_dict:
			continue
		print(f"{_tag} -> {row[1]}")
		# Since we found a tag, let's check if that contains a implicate
		tag_dict[al_row] = True
		new_dict = recursive_implicate_walk(_cur, al_row, tag_dict)
		tag_dict = tag_dict | new_dict
	
	# Once we've finished walking this leaf of the tree, finish
	return tag_dict

def create_implicate_list(_tag):
	tag = get_tag_alias(cur, _tag)

	tdict = recursive_implicate_walk(cur, tag, {})
	print("")
	implicates = list(dict.fromkeys(tdict))
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


# And also construct a simple lookup table:
test_all = False
if test_all:
	numimg = 0
	for row in tags:
		loop_start = time.perf_counter()
		create_implicate_list(row[0])
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
		#print(console_len, end=end)
		numimg += 1

	print("\n")
	con.commit()

create_implicate_list("rainbow_fingerless_elbow_gloves")
create_implicate_list("penis_in_pussy")
create_implicate_list("penile_penetration")