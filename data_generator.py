import pandas as pd
import numpy as np

df = pd.DataFrame({
    'user_id': np.random.randint(1, 100000, size=1000000),
    'age': np.random.randint(18, 65, size=1000000),
    'income': np.random.normal(50000, 15000, size=1000000).astype(int),
    'clicks': np.random.randint(0, 100, size=1000000),
    'purchases': np.random.randint(0, 5, size=1000000),
})

df.to_csv("user_data.csv", index=False)
