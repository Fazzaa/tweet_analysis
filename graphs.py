import os
from bson.json_util import dumps
from path import *
from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt
import numpy as np


def generate_wordcloud(counter):
	pink_mask = np.array(Image.open(MASK_PATH))
	wc = WordCloud(width = 800, height = 800, background_color='white', mask=pink_mask).generate_from_frequencies(counter)
	image_colors = ImageColorGenerator(pink_mask)
	wc.recolor(color_func = image_colors) 
	plt.figure(figsize = (8, 8), facecolor = None)
	plt.imshow(wc)
	plt.savefig("test.png")
	plt.axis("off")
	plt.tight_layout(pad = 0)
	plt.show()
