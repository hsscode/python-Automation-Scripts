import pandas as pd
import json
# Path to your JSON file
json_file_path = "C:\\Users\\Harsh Singh\\Downloads\\chat_json.json"

# Read the JSON file content
with open(json_file_path, 'r',encoding="utf8") as file:
    data = json.load(file)


session_data = []
message_data = []

for session_id, session_info in data["chatData"].items():
    session = {
        "session_id": session_id,
        "createdAt": session_info["createdAt"],
        "sessionStartTime": session_info["sessionStartTime"],
        "sessionEndTime": session_info["sessionEndTime"],
    }
    session_data.append(session)

    for message in session_info["conversation"]:
        msg = {
            "session_id": session_id,
            "msgText": message["msgText"],
            "msgType": message["msgType"],
            "createdAt": message["createdAt"],
            # Add other message attributes here as needed
        }
        message_data.append(msg)

# Create DataFrame for sessions
session_df = pd.DataFrame(session_data)

# Create DataFrame for messages
message_df = pd.DataFrame(message_data)


message_df.to_csv("C:\\Users\\Harsh Singh\\Downloads\\message_csv.csv", index=False)

