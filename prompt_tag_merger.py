import os
import sys
from concurrent.futures import ProcessPoolExecutor

source_dir = ""

# Handle likely exceptions from bad arguments
if __name__ == "__main__":
	if len(sys.argv) > 2:
		raise Exception("Path to directory must be enclosed with quotes, stopping.")

	if len(sys.argv) == 1:
		raise Exception("No directory supplied, stopping.")

	source_dir = sys.argv[1]
	# Strip any directory names ending with a slash
	if source_dir[len(source_dir)-1] == "/" or source_dir[len(source_dir)-1] == "\\":
		source_dir = source_dir[:-1]

	if source_dir == "":
		raise Exception("No directory supplied, stopping.")

	if not os.path.isdir(source_dir):
		raise Exception("Target directory containing both webui textfiles and Hydrus DB export files does not exist, stopping.")

	if len(os.listdir(source_dir)) < 2:
		raise Exception("Target directory contains no images or files, stopping.")

	# Don't make a mess in the parent folder
	if not os.path.isdir(source_dir+"/output"):
		os.mkdir(source_dir+"/output")

# Use list of image filenames to find relevant exported text files containing prompts, tokens and tags
def process_tags(imagename, sdir):
	filename = os.path.splitext(imagename)[0]
	#print(filename, sdir)

	source_tags = []
	path = os.path.join(sdir, imagename + ".txt")
	if os.path.isfile(path):
		with open(path, "r", encoding="utf-8") as file:
			lines = file.readlines()
			tags = ""
			multi_line = False
			if len(lines) > 1:
				multi_line = True
			# Handle webui and Hydrus tags without duplicates
			for line in lines:
				# Strip the contents of the line ie newlines, EOF:
				tags = line.strip().lower()

				# Replace misc things like underscores to spaces
				tags = tags.replace("_", " ")
				#tags = tags.replace("-", " ")

				# Handle BLIP/Deepbooru captioning/tags
				if not multi_line:
					# Split the comma separated tags into singular tags
					one_line_tag = tags.split(",")
					for split_tags in one_line_tag:
						new_tag = split_tags.strip()
						if new_tag not in source_tags:
							source_tags.append(new_tag)
				# Handle Hydrus captioning/tags
				else:
					# Split the first instance of the namespace, ie, "rating:safe" and returns "safe" - if there isn't a namespace, use the tag directly
					if ":" in tags:
						tag = tags.split(":", 1)
						nspace = tag[0].strip()
						# Skip tags with specific hydrus namespaces
						if nspace in ["creator", "title", "series", "site", "booru", "circle", "discord", "dragon age", "emotion", "ethinicity", "face", "faults", "filename", "firearm", "id", "meta", "page", "pixiv id", "pixiv illustration", "pixiv profile", "pixiv work", "resident evil 2", "set", "source", "studio", "subreddit", "subscribestar id", "tag source", "tagme", "temp", "website", "uploader", "year"]:
							continue
						new_tag = tag[1].strip()
						if new_tag not in source_tags:
							source_tags.append(new_tag)
					else:
						if tags not in source_tags:
							source_tags.append(tags)

		# Sort alphabetically
		source_tags.sort()

		# Write new tags to disk in the output folder.
		i = 1
		all_tags = ""
		for tag in source_tags:
			# Do not append a comma and a space when it's the last entry
			if i == len(source_tags):
				all_tags += tag
			else:
				all_tags += tag + ", "
			i += 1
		with open(sdir+"/output/"+filename+".txt", "w", encoding="utf-8") as file:
			file.write(all_tags)

def main():
	source_images = []

	# Grab a list of image filenames
	for filename in os.listdir(source_dir):
		current = os.path.join(source_dir, filename)

		if os.path.isfile(current):
			ext = os.path.splitext(filename)[1].lower()
			if ext in ['.jpg', '.jpeg', '.png', '.bmp', '.webp', ".txt"]:
				source_images.append(os.path.splitext(filename)[0])

	#[print(img) for img in source_images]
	with ProcessPoolExecutor(8) as exe:
		_ = [exe.submit(process_tags, i, source_dir) for i in source_images]

	return len(source_images)

if __name__ == "__main__":
	l = main()
	print("Successfully merged tags of " + str(l) + " images, tags exported to " + source_dir+"/output")