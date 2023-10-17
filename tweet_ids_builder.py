import json  # Import the json module

# Specify the file path
file_path = "dataset.json"  # Replace with the actual file path

# Initialize an empty list to store the data
data = []
tweet_ids = []


# Open and read the file
with open(file_path, 'r') as file:
    for line in file:
        # Parse the JSON data from each line
        entry = json.loads(line)
        
        # Check if 'y' is equal to 's' and add it to the data list
        if entry.get("y") == "s":
            tweet_id = entry.get("tid")
            data.append(entry)
            tweet_ids.append(tweet_id)

# Now, 'data' contains the filtered data
# for entry in data:
def get_tweets():
    return tweet_ids
