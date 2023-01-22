from operator import itemgetter
import os
import argparse
import sqlite3
from tqdm import tqdm

pwd = os.getcwd()

parser = argparse.ArgumentParser(description="The folder to rebuild the sqlite db from files.")
parser.add_argument("--db", help="Location of the DB to track already downloaded images.", required=False, default=pwd + "/db/regenerated_downloaded.db")
parser.add_argument("--dir", help="Where the directory of image and caption pairs are.", required=True)
args = parser.parse_args()

# Strip any directory names ending with a slash
if args.dir[len(args.dir)-1] == "/" or args.dir[len(args.dir)-1] == "\\":
	args.dir = args.dir[:-1]

if len(os.listdir(args.dir)) < 1:
	raise Exception("Target directory contains no images or files, stopping.")

known_ids = []

# Grab a list of image filenames
for filename in tqdm(os.listdir(args.dir), desc="Loading and collating all known tagged images"):
	current = os.path.join(args.dir, filename)
	if os.path.isfile(current):
		ext = os.path.splitext(filename)[1].lower()
		if ext in ['.txt']:
			try:
				file = int(os.path.splitext(filename)[0].lower())
				if file not in known_ids:
					known_ids.append(file)
			except:
				pass

# Database stuff
def load_db():
	__con = sqlite3.connect(args.db)
	__cur = __con.cursor()
	return __con, __cur

con, cur = load_db()
try:
	cur.execute("CREATE TABLE posts (id integer PRIMARY KEY UNIQUE)")
	con.commit()
except:
	pass

for id in known_ids:
	db_query = f"INSERT OR IGNORE INTO posts (id) VALUES ({id})"
	cur.execute(db_query)

con.commit()
con.close()