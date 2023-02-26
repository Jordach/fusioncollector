import os
import argparse
import math
from transformers import CLIPTokenizer
from tqdm import tqdm

pwd = os.getcwd()

# Arguments
parser = argparse.ArgumentParser(description="Downloader for e6 based on tag for machine learning models")
parser.add_argument("--dir", help="Where the downloaded images and tags reside. Separate from the DB.", required=True)
parser.add_argument("--batch_size", help="Location of the e6 DB generated by e6_export_to_sqlite.py", required=False, default=20, type=int)
args = parser.parse_args()

if args.dir == "" or args.dir == None:
	Exception("No directory supplied, stopping.")

# Strip any directory names ending with a slash
if args.dir[len(args.dir)-1] == "/" or args.dir[len(args.dir)-1] == "\\":
	args.dir = args.dir[:-1]

max_len = 77
tokenizer = CLIPTokenizer.from_pretrained("openai/clip-vit-base-patch32")

directory_contents = os.listdir(args.dir)
if len(directory_contents) < args.batch_size:
	Exception("Not enough files to make a batch, stopping.")

# Each item in this list looks like the following
# token_batches[i] = ([tokens, tokens, tokens, ...], batch_avg, token_max, padded_len)
token_batches = []
batch_tokens = []
iterator = tqdm(directory_contents)
for filename in iterator:
	if len(batch_tokens) % args.batch_size == 0 and len(batch_tokens) > 0:
		# Find the maximum length of the tokens
		max_token_len = max(len(x) for x in batch_tokens)

		# Calculate the number of chunks 
		num_chunks = math.ceil(max_token_len / 75)
		
		# Get an average of each batch
		total = 0
		pad_total = 0
		for x in batch_tokens:
			len_x = len(x)
			total += len_x
			pad_total += (75 * num_chunks) - len_x
		avg_tokens = total // args.batch_size
		avg_pad = pad_total // args.batch_size
		

		# Append our data to the master list for processing afterwards
		listtup = (batch_tokens.copy(), avg_tokens, avg_pad, max_token_len, 75 * num_chunks)
		token_batches.append(listtup)
		batch_tokens = []

	ext = os.path.splitext(filename)[1].lower()
	current = os.path.join(args.dir, filename)
	if ext in ['.txt']:
		if os.path.isfile(current):
			# Read in tags from file
			tags = ""
			with open(current, "r", encoding="utf-8") as tagfile:
				lines = tagfile.readlines()
				for line in lines:
					tags = line.strip().lower()
					# Ignore empty text files
					if tags == "":
						continue
					
			batch_tokens.append(tokenizer(tags, padding="do_not_pad", verbose=False).input_ids)

def get_list_avg(list):
	return sum(list) / len(list)

token_avg = []
pad_avg = []
token_long = []
padded_len = []


csv_data = ""
for batch in token_batches:
	csv_data += f"{batch[1]},{batch[2]},{batch[3]},{batch[4]}\n"
	token_avg.append(batch[1])
	pad_avg.append(batch[2])
	token_long.append(batch[3])
	padded_len.append(batch[4])

csv_header = f"token average,padding average,average length,average padded length\n{get_list_avg(token_avg)},{get_list_avg(pad_avg)},{get_list_avg(token_long)},{math.ceil(get_list_avg(padded_len) / 75)}\ntoken average,padding average,average length,average padded length\n"

with open(pwd + "/db/token_analysis.csv", "w", encoding="utf-8") as csv_file:
	csv_file.write(csv_header + csv_data)

#print(len(token_batches), get_list_avg(token_avg), get_list_avg(pad_avg), get_list_avg(token_long), math.ceil(get_list_avg(padded_len) / 75))