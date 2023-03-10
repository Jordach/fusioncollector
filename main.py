# MIT License

import discord
import sqlite3
import os
import random
from operator import itemgetter

# Backup the DB every 15 minutes
import shutil
import schedule
import time
from datetime import datetime, date
import threading

def run_continuously(interval=1):
	"""Continuously run, while executing pending jobs at each
	elapsed time interval.
	@return cease_continuous_run: threading. Event which can
	be set to cease continuous run. Please note that it is
	*intended behavior that run_continuously() does not run
	missed jobs*. For example, if you've registered a job that
	should run every minute and you set a continuous run
	interval of one hour then your job won't be run 60 times
	at each interval but only once.
	"""
	cease_continuous_run = threading.Event()

	class ScheduleThread(threading.Thread):
		@classmethod
		def run(cls):
			while not cease_continuous_run.is_set():
				schedule.run_pending()
				time.sleep(interval)

	continuous_thread = ScheduleThread()
	continuous_thread.start()
	return cease_continuous_run

# Start the background thread
stop_run_continuously = run_continuously()

# Bot things
bot_token = ""

pwd = os.getcwd()

if not os.path.isfile(pwd + "/token.conf"):
	with open("token.conf") as file:
		file.write("")
		raise Exception("Bot token is missing")

with open("token.conf", "r") as token:
	token_line = token.readlines()
	for line in token_line:
		bot_token = line.strip()
		break

	if bot_token == "":
		with open("token.conf") as file:
			file.write("")
			raise Exception("Bot token is missing")

# Undesired tags - ie things you don't want exported in public data
undesired_tags = [""]

# Create the file if missing, but do not pause execution on failure
if not os.path.isfile(pwd + "/undesired_tags.conf"):
	with open("undesired_tags.conf") as file:
		file.write("")

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

add_lines = []
with open("add_lines.conf", "r", encoding="utf-8") as al:
	lines = al.readlines()
	for line in lines:
		add_lines.append(line.strip())

# Construct the DB folder
if not os.path.isdir(os.getcwd()+"/db"):
	os.mkdir(os.getcwd()+"/db")

def load_db():
	_pwd = os.getcwd()
	__con = sqlite3.connect(_pwd + "/db/tags.db")
	__cur = __con.cursor()

	return __con, __cur

# Automate DB backups
def backup_db():
	if not os.path.isdir(os.getcwd()+"/backup"):
		os.mkdir(os.getcwd()+"/backup")

	print("Backing up the db.")
	today = date.today()
	current_date = today.strftime("%Y_%m_%d")
	now = datetime.now()
	current_time = now.strftime("%H_%M")
	resulting_backup = current_date + "_" + current_time + "_tags.db"
	shutil.copy2(pwd + "/db/tags.db", pwd + "/backup/" + resulting_backup)

	con, cur = load_db()

	global_tag_list = []
	global_tag_dict = {}
	downloaded_tags = []
	if os.path.isfile(pwd + "/downloaded_tags.txt"):
		with open(pwd + "/downloaded_tags.txt", "r") as file:
			lines = file.readlines()
			for raw_tag in lines:
				tag = raw_tag.strip()
				if tag not in downloaded_tags:
					downloaded_tags.append(tag)

	tag_query = "SELECT * FROM tags ORDER BY tag ASC"
	mistakes = ["", ")_sssonic", "female_feral_sylveon_meowstic_alolan_ninetales_watersports_skirt_upskirt_feathered_wings_flying_cloudscape", "flower._musk", "stealth_masturbation_absurd_res"]
	for tag in cur.execute(tag_query):
		# Do not list undesired tags and do not list downloaded tags
		if tag[0] not in mistakes and tag[0] not in downloaded_tags:
			global_tag_list.append(tag[0])
			global_tag_dict[tag[0]] = 0

	id_query = "SELECT * FROM known_id ORDER BY id ASC"
	discord_ids = []
	for id in cur.execute(id_query):
		discord_ids.append(id[0])

	for id in discord_ids:
		personal_tags = []
		personal_tag_query = "SELECT * FROM `" + id + "_tags` ORDER BY tag ASC"
		for tag in cur.execute(personal_tag_query):
			if tag[0] in global_tag_list:
				global_tag_dict[tag[0]] += 1

	con.close()

	# Generate the csv:
	tag_tuples = []
	csv_data = "tag_name,votes\n"
	# Sort the list based on number of votes
	for tag in global_tag_list:
		tag_tuples.append((tag, global_tag_dict[tag]))
	
	tag_tuples = sorted(tag_tuples, key=itemgetter(1), reverse=True)
	
	for tag in tag_tuples:
		csv_data += tag[0] + "," + str(tag[1]) + "\n"

	csv_path = pwd + "/tmp/tags_remaining.csv"
	with open(csv_path, "w") as file:
		file.write(csv_data)

schedule.every().hour.at("00:00").do(backup_db)
schedule.every().hour.at("15:00").do(backup_db)
schedule.every().hour.at("30:00").do(backup_db)
schedule.every().hour.at("45:00").do(backup_db)

# Create the tag DB
_con, _cur = load_db()

try:
	# Globally known tags
	_cur.execute("CREATE TABLE tags (tag text PRIMARY KEY UNIQUE)")
	_con.commit()
	# Known Discord IDs that have added tags previously for iterating against later to get a true global count
	_cur.execute("CREATE TABLE known_id (id text PRIMARY KEY UNIQUE)")
	_con.commit()
except Exception as e:
	pass
_con.close()

# Construct the temp file folder
if not os.path.isdir(os.getcwd()+"/tmp"):
	os.mkdir(os.getcwd()+"/tmp")

def set_author(self, message, embed):
	if message.author.avatar:
		embed.set_author(name=message.author.display_name, icon_url=message.author.avatar.url)
	else:
		embed.set_author(name=message.author.display_name)

async def help(self, message):
	embed = discord.Embed(title="Help:")
	set_author(self, message, embed)
	embed.color = discord.Color.blurple()
	embed.add_field(name="!add:", value="Adds any number of tags you want into the voting pool.\n\nUsage:\n```!add male, female, cute, safe, explicit```", inline=False)
	embed.add_field(name="!tags:", value="Gives you a list of all submitted tags.", inline=False)
	embed.add_field(name="!my_tags:", value="Gives you a list of your submitted tags.", inline=False)
	embed.add_field(name="!source_code:", value="Gives you a copy of the currently running source code.", inline=False)
	embed.add_field(name="!downloaded:", value="Gives you a list of the downloaded or downloading tags.", inline=False)
	embed.add_field(name="!downloading:", value="Displays the current status of the gallery downloader.", inline=False)

	await message.channel.send(embed=embed, mention_author=False, reference=message)

async def sauce(self, message):
	await message.channel.send("My source code is also available at: https://github.com/Jordach/fusioncollector", file=discord.File(pwd+"/main.py"), mention_author=False, reference=message)

async def dump(self, message):
	con, cur = load_db()

	tag_query = "SELECT * FROM tags ORDER BY tag ASC"
	global_tag_list = []
	global_tag_dict = {}

	for tag in cur.execute(tag_query):
		# Do not list undesired tags
		if tag[0] not in undesired_tags:
			global_tag_list.append(tag[0])
			global_tag_dict[tag[0]] = 0

	id_query = "SELECT * FROM known_id ORDER BY id ASC"
	discord_ids = []
	for id in cur.execute(id_query):
		discord_ids.append(id[0])

	for id in discord_ids:
		personal_tag_query = "SELECT * FROM `" + id + "_tags` ORDER BY tag ASC"
		for tag in cur.execute(personal_tag_query):
			if tag[0] in global_tag_list:
				global_tag_dict[tag[0]] += 1

	con.close()

	# Generate the csv:
	csv_data = "tag_name,votes\n"
	for tag in global_tag_list:
		csv_data += tag + "," + str(global_tag_dict[tag]) + "\n"

	csv_path = pwd + "/tmp/tags_global_" + message.author.display_name + ".csv"
	with open(csv_path, "w") as file:
		file.write(csv_data)

	await message.channel.send(file=discord.File(csv_path), mention_author=False, reference=message)

async def dump_me(self, message):
	con, cur = load_db()
	id_str = str(message.author.id)

	query = "SELECT * FROM `" + id_str + "_tags` ORDER BY tag ASC"

	list_of_tags = ""
	for tag in cur.execute(query):
		list_of_tags += tag[0] + "\n"

	file_path = pwd + "/tmp/tags_" + message.author.display_name + ".txt"
	with open(file_path, "w") as file:
		file.write(list_of_tags)
	
	con.close()
	await message.channel.send(file=discord.File(file_path), mention_author=False, reference=message)

async def add(self, message):
	# Strip off the !add and 
	raw_tags = message.content[5:].split(",")

	if raw_tags == "":
		return

	con, cur = load_db()

	id_str = str(message.author.id)

	# Try and assemble our per-user table as this is how we get the true number of tag votes later
	try:
		cur.execute("CREATE TABLE `" + id_str + "_tags` (tag text PRIMARY KEY UNIQUE)")
		con.commit()
	except:
		pass

	tags = []
	# Strip duplicates - SQLite will delete a ny duplicates
	for tag in raw_tags:
		strip_tag = tag.strip().lower()
		if strip_tag not in tags:
			tags.append(strip_tag)

	# Add tags to DBs, personal and globally
	
	for tag in tags:
		ln = 1
		try:
			cur.execute("INSERT OR IGNORE INTO known_id (id) VALUES (" + id_str + ")")
			ln += 1
			cur.execute("INSERT OR IGNORE INTO tags (tag) VALUES (?)", (tag,))
			ln += 1
			cur.execute("INSERT OR IGNORE INTO `" + id_str + "_tags` (tag) VALUES (?)", (tag,))
			con.commit()
		except Exception as e:
			print(e)
			print("Tag failure: " + tag)
			print(ln)
			pass

	con.close()

	try:
		embed = discord.Embed(title="Success:")
		embed.color = discord.Color.blurple()
		set_author(self, message, embed)
		embed.add_field(name="Tags:", value=raw_tags)
		embed.set_footer(text=random.choice(add_lines))
		await message.channel.send(embed=embed, mention_author=False, reference=message)
	except:
		await message.channel.send("Alright, tags were accepted, but the message was too long OwO and made Discord bulge. > w <", mention_author=False, reference=message)

	return
		
async def downloaded(self, message):
	if not os.path.isfile(pwd + "/downloaded_tags.txt"):
		await message.channel.send("It appears this file is missing, or tags haven't been added yet.")
	else:
		await message.channel.send(file=discord.File(pwd+"/downloaded_tags.txt"))

async def downloading(self, message):
	if os.path.isfile(pwd + "/downloader_status.conf"):
		with open(pwd + "/downloader_status.conf", "r") as file:
			lines = file.readlines()
			for line in lines:
				await message.channel.send(line.strip())

async def remaining(self, message):
	con, cur = load_db()

	global_tag_list = []
	global_tag_dict = {}
	downloaded_tags = []
	if os.path.isfile(pwd + "/downloaded_tags.txt"):
		with open(pwd + "/downloaded_tags.txt", "r", encoding="utf-8") as file:
			lines = file.readlines()
			for raw_tag in lines:
				tag = raw_tag.strip()
				if tag not in downloaded_tags:
					downloaded_tags.append(tag)

	tag_query = "SELECT * FROM tags ORDER BY tag ASC"
	for tag in cur.execute(tag_query):
		# Do not list undesired tags and do not list downloaded tags
		if tag[0] not in undesired_tags and tag[0] not in downloaded_tags:
			global_tag_list.append(tag[0])
			global_tag_dict[tag[0]] = 0

	id_query = "SELECT * FROM known_id ORDER BY id ASC"
	discord_ids = []
	for id in cur.execute(id_query):
		discord_ids.append(id[0])

	for id in discord_ids:
		personal_tags = []
		personal_tag_query = "SELECT * FROM `" + id + "_tags` ORDER BY tag ASC"
		for tag in cur.execute(personal_tag_query):
			if tag[0] in global_tag_list:
				global_tag_dict[tag[0]] += 1

	con.close()

	# Generate the csv:
	csv_data = "tag_name,votes\n"
	for tag in global_tag_list:
		csv_data += tag + "," + str(global_tag_dict[tag]) + "\n"

	csv_path = pwd + "/tmp/tags_remain_" + message.author.display_name + ".csv"
	with open(csv_path, "w") as file:
		file.write(csv_data)

	await message.channel.send(file=discord.File(csv_path), mention_author=False, reference=message)

class Bot(discord.Client):
	async def on_ready(self):
		await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="for e621 tags."))

	async def on_member_join(self, member):
		try:
			message = "Hi there " + member.name + ", I'm the bot that handles and organises tagging for the Fluffusion model. You can get started with me by running:```\n!help```"
			await member.send(message)
		except Exception as e:
			print(e)
			print(f"on join failed for {member.name}.")

	async def on_message(self, message):
		# The bot should never respond to itself, ever
		if message.author == self.user:
			return

		# Ignore noisy bots
		if message.author.bot:
			return

		# Prevent the bot from reading any joined public servers
		if message.guild:
			return

		if message.content.startswith("!my_tags"):
			await dump_me(self, message)
		elif message.content.startswith("!tags"):
			await dump(self, message)
		elif message.content.startswith("!add"):
			await add(self, message)
		elif message.content.startswith("!source_code"):
			await sauce(self, message)
		elif message.content.startswith("!help"):
			await help(self, message)
		elif message.content.startswith("!downloaded"):
			await downloaded(self, message)
		elif message.content.startswith("!downloading"):
			await downloading(self, message)
		elif message.content.startswith("!remaining"):
			await remaining(self, message)

client = Bot(intents=discord.Intents(dm_messages=True, dm_reactions=True, message_content=True, members=True))
client.run(bot_token)