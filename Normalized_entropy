import numpy as np

BINS = [0, 50, 100, 200, 400, 800, np.inf]

def normalized_entropy(values, bins, min_count=7):
    """
    values: array-like comment lengths for a UUID-month
    bins: fixed bin edges
    min_count: minimum data points required
    """
    values = np.asarray(values)

    if len(values) < min_count:
        return np.nan

    counts, _ = np.histogram(values, bins=bins)
    probs = counts / counts.sum()

    # Remove zero probabilities
    probs = probs[probs > 0]

    entropy = -np.sum(probs * np.log(probs))
    max_entropy = np.log(len(bins) - 1)

    return entropy / max_entropy

import pandas as pd

entropy_df = (
    df.groupby("uuid")["comment_length"]
      .apply(lambda x: normalized_entropy(x, BINS))
      .reset_index(name="norm_entropy")
)

#join at monthly level
monthly_df = stats_df.merge(entropy_df, on=["uuid", "month"])

monthly_df["bot_flag"] = (
    (monthly_df["cv"] < 0.10) &
    (monthly_df["norm_entropy"] < 0.30) &
    (monthly_df["days"] >= 10)
)
