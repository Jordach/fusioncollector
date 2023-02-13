import imageio
import os
import argparse

parser = argparse.ArgumentParser(description="The folder to make a gif from.")
parser.add_argument("--dir", help="Where the directory of images are.", required=True)
args = parser.parse_args()

# Strip any directory names ending with a slash
if args.dir[len(args.dir)-1] == "/" or args.dir[len(args.dir)-1] == "\\":
	args.dir = args.dir[:-1]

if len(os.listdir(args.dir)) < 1:
	raise Exception("Target directory contains no images or files, stopping.")

images = []
for filename in os.listdir(args.dir):
	f = os.path.join(args.dir, filename)
	images.append(imageio.imread(f))

imageio.mimsave(os.path.join(args.dir) + "/out.gif", images)