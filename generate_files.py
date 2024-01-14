import pandas as pd
import random


def split_csv(dataframe, output_folder, chunk_size=10):
    # Calculate the number of chunks
    num_chunks = (len(dataframe) // chunk_size
                  + (1 if len(dataframe) % chunk_size else 0))

    # Split and save the chunks, adding random errors to every second file
    for i in range(num_chunks):
        chunk = dataframe[i * chunk_size:(i + 1) * chunk_size]

        # Add an error to every second file
        if i % 2 == 1:
            random_row = random.randint(0, chunk.shape[0] - 1)
            random_col = random.randint(0, chunk.shape[1] - 1)
            # Select a random error value
            error_value = random.choice(
                [-9, 'null', 'Diamond', "They", -10000, "SHIT"])
            chunk.iloc[random_row, random_col] = error_value

        chunk.to_csv(f"{output_folder}/customers_file_{i+1}.csv", index=False)


df = pd.read_csv("../dsp-finalproject/data/customer_churn_records.csv")
df_with_only_features = df.drop(
    ["RowNumber", "CustomerId", "Geography", "Surname", "Complain", "Exited"],
    axis=1)
split_csv(df_with_only_features, "../dsp-finalproject/data/Folder A")
