from operator import itemgetter
import os
import argparse
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Gets a CSV list of tags for a given folder.")

parser.add_argument("--dir", help="Where the directory ofn image and caption pairs are.", required=True)
args = parser.parse_args()

# Strip any directory names ending with a slash
if args.dir[len(args.dir)-1] == "/" or args.dir[len(args.dir)-1] == "\\":
	args.dir = args.dir[:-1]

if len(os.listdir(args.dir)) < 1:
	raise Exception("Target directory contains no images or files, stopping.")

all_tags = {}

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
						one_line_tag = tags.split(",")
						for split_tags in one_line_tag:
							new_tag = split_tags.strip()
							new_tag = new_tag.replace('"', "")
							new_tag = new_tag.replace("'", "")

							# If the tag isn't known, give it a single count
							# Otherwise, make it add one to the total known.
							if new_tag not in all_tags:
								all_tags[new_tag] = 1
							elif new_tag in all_tags:
								all_tags[new_tag] += 1

all_tag_tup = []
for tag in all_tags.keys():
	all_tag_tup.append((tag, all_tags[tag]))
all_tag_tup = sorted(all_tag_tup, key=itemgetter(1), reverse=True)

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
	out = f'{str(tag_pair[0])},0,{str(tag_pair[1])}\n'
	if tag_pair[0] not in undesired_tags and tag_pair[0] != "total_images":
		csv_data_undesired += out
	csv_data += out

with open("tags_true.csv", "w", encoding="utf-8") as true_file:
	true_file.write(csv_data)

with open("tags.csv", "w", encoding="utf-8") as csv_file:
	csv_file.write(csv_data_undesired)