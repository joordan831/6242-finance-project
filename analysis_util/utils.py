import re
# import subprocess
# import sys
import csv

# def install_py_package(package):
#     subprocess.check_call([sys.executable, "-m", "pip", "install", package])


# Removes emojis from text
# ref https://stackoverflow.com/a/58356570
from pathlib import Path


def remove_emojis(text_str):
    emoji_regex = re.compile("["
                             u"\U0001F600-\U0001F64F"  # emoticons
                             u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                             u"\U0001F680-\U0001F6FF"  # transport & map symbols
                             u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                             u"\U00002500-\U00002BEF"  # chinese char
                             u"\U00002702-\U000027B0"
                             u"\U00002702-\U000027B0"
                             u"\U000024C2-\U0001F251"
                             u"\U0001f926-\U0001f937"
                             u"\U00010000-\U0010ffff"
                             u"\u2640-\u2642"
                             u"\u2600-\u2B55"
                             u"\u200d"
                             u"\u23cf"
                             u"\u23e9"
                             u"\u231a"
                             u"\ufe0f"  # dingbats
                             u"\u3030"
                             "]+", re.UNICODE)
    return re.sub(emoji_regex, '', text_str)


# Cleans text of noisy characters using some regular expressions
def clean_text(text_str):
    text_str = text_str.replace("\n", ' ')
    text_str = text_str.replace('#', '')
    text_str = re.sub('@[^\s]+', '', text_str)
    text_str = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', text_str, flags=re.MULTILINE)
    text_str = remove_emojis(text_str)

    return text_str


def save_to_csv(data_dump, mode, file_name):
    if len(data_dump) > 0:
        columns = list(data_dump[0].keys())
        file = Path(file_name)
        if not file.exists():
            with open(file_name, mode, encoding="utf-8", newline='') as file:
                csv_writer = csv.DictWriter(file, fieldnames=columns)
                if mode == 'w':
                    csv_writer.writeheader()
                for data in data_dump:
                    csv_writer.writerow(data)
                file.close()
                print(f'File {file_name} saved!')
        else:
            print(f'File {file_name} already exists')
