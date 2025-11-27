import time
import pandas as pd
from tqdm import tqdm
import os
import re

makes = ['honda', 'toyota', 'nissan', 'suzuki', 'micro', 'mitsubishi', 'mahindra', 'mazda', 'daihatsu', 'hyundai', 'kia', 'bmw', 'perodua', 'tata']

for make in makes:
    file_path = os.path.join('mapped',f'{make}_mapped.csv')

    dataset = pd.read_csv(file_path)

    # Function to clean and concatenate model names
    def clean_model_name(model_name):
        return "".join(model_name.replace("-", "").split())

    # Apply the function to the dataset
    dataset['map_model_name'] = dataset['map_model_name'].apply(clean_model_name)

    output_file_path = os.path.join('data','mapped_data', f'{make}_mapped.csv')
    dataset.to_csv(output_file_path, index=False)

