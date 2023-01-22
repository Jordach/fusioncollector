import pandas as pd
import os

pwd = os.getcwd()

true_a = pd.read_csv("ftags_true.csv")
true_b = pd.read_csv("tags_true.csv")

true_c = pd.concat([true_a, true_b])
out = true_c.groupby(["tag"],as_index=False)["count"].sum()

out.to_csv("merge_true.csv", index=False)