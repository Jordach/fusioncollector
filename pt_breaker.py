import os
import torch
import pprint
import diffusers
from torch.utils.data import Dataset
pp = pprint.PrettyPrinter(indent=4)

pwd = os.getcwd()
file = "/db/latents_cache_0.pt"

class LatentsDataset(Dataset):
    def __init__(self, latents_cache=None, text_encoder_cache=None, conditioning_latent_cache=None, extra_cache=None,tokens_cache=None):
        self.latents_cache = latents_cache
        self.text_encoder_cache = text_encoder_cache
        self.conditioning_latent_cache = conditioning_latent_cache
        self.extra_cache = extra_cache
        self.tokens_cache = tokens_cache
    def add_latent(self, latent, text_encoder, cached_conditioning_latent, cached_extra, tokens_cache):
        self.latents_cache.append(latent)
        self.text_encoder_cache.append(text_encoder)
        self.conditioning_latent_cache.append(cached_conditioning_latent)
        self.extra_cache.append(cached_extra)
        self.tokens_cache.append(tokens_cache)
    def __len__(self):
        return len(self.latents_cache)
    def __getitem__(self, index):
        return self.latents_cache[index], self.text_encoder_cache[index], self.conditioning_latent_cache[index], self.extra_cache[index], self.tokens_cache[index]

model = torch.load(pwd + file, map_location=torch.device('cpu'))

for i in model:
	print(i)
	break