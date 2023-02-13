from operator import itemgetter
import os
import argparse
import sqlite3
from tqdm import tqdm

pwd = os.getcwd()

parser = argparse.ArgumentParser(description="Gets a CSV list of tags for a given folder.")

parser.add_argument("--dir", help="Where the directory ofn image and caption pairs are.", required=True)
parser.add_argument("--e6", help="Location of the e6 DB generated by e6_export_to_sqlite.py", required=False, default=pwd + "/db/e6.db")
args = parser.parse_args()

# Strip any directory names ending with a slash
if args.dir[len(args.dir)-1] == "/" or args.dir[len(args.dir)-1] == "\\":
	args.dir = args.dir[:-1]

def load_db():
	__con = sqlite3.connect(args.e6)
	__cur = __con.cursor()
	return __con, __cur

con, cur = load_db()

if len(os.listdir(args.dir)) < 1:
	raise Exception("Target directory contains no images or files, stopping.")

all_tags = {}
dbg = []
all_tags["total_images"] = len(os.listdir(args.dir)) // 2
# Grab a list of image filenames
for filename in tqdm(os.listdir(args.dir), desc="Loading and collating all tags"):
	current = os.path.join(args.dir, filename)
	if os.path.isfile(current):
		ext = os.path.splitext(filename)[1].lower()
		if ext in ['.txt']:
			if os.path.isfile(current):
				with open(current, "r", encoding="utf-8") as file:
					lines = file.readlines()
					for line in lines:
						tags = line.strip().lower()

						# Split the comma separated tags into singular tags
						#if "urethra" in tags:
						#	dbg.append(filename + " " + tags + "\n")
						one_line_tag = tags.split(", ")
						one_line_tag = list(dict.fromkeys(one_line_tag))
						for split_tags in one_line_tag:
							new_tag = split_tags.strip()
							new_tag = new_tag.replace(" ", "_")
							new_tag = new_tag.replace('"', "")
							new_tag = new_tag.replace("'", "")

							# If the tag isn't known, give it a single count
							# Otherwise, make it add one to the total known.
							if all_tags.get(new_tag) == None:
								all_tags[new_tag] = 1
							else:
								all_tags[new_tag] += 1
						break

#bad_files = open("db/bad_files.txt", "w", encoding="utf-8")
#bad_files.writelines(dbg)
#bad_files.close()

all_tag_tup = []
for tag in all_tags.keys():
	query = f'SELECT * FROM tags WHERE tag =?'
	categ = ""
	for row in cur.execute(query, (tag,)):
		# Store the count, category and post processed name
		all_tag_tup.append((tag, row[1], all_tags[tag]))
		break

all_tag_tup = sorted(all_tag_tup, key=itemgetter(2), reverse=True)

# Get undesired tags from the bot's list.
undesired_tags = []
if os.path.isfile(os.getcwd() + "/undesired_tags.conf"):
	with open("undesired_tags.conf", "r") as ut:
		lines = ut.readlines()
		for line in lines:
			undesired_tag = line.strip()
			# Deduplicate and ensure that the string has content
			if undesired_tag not in undesired_tags and undesired_tag != "":
				if undesired_tag.startswith("#"):
					continue
				else:
					undesired_tags.append(undesired_tag)

csv_data = ""
csv_data_undesired = ""
for tag_pair in all_tag_tup:


	out = f'{str(tag_pair[0])},{str(tag_pair[1])},{str(tag_pair[2])}\n'
	if tag_pair[0] not in undesired_tags and tag_pair[0] != "total_images":
		csv_data_undesired += out
	csv_data += out

with open("db/tags_true.csv", "w", encoding="utf-8") as true_file:
	true_file.write(csv_data)

with open("db/tags.csv", "w", encoding="utf-8") as csv_file:
	csv_file.write(csv_data_undesired)