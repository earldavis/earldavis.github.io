---
layout: post
title: Generating Unit Tests Using OpenAI and Git Diff
date: 2024-02-14 09:55
categories: [data_science,]
description: The Process of Generating a Test Based on the Git Output and the Insertion of the Context
---

Import the necessary packages and modules. Azure OpenAI and Langchain are used to create a connection to an endpoint. The environment variables are stored locally using the `dotenv` package. The API key could and probably should be stored in Azure Key Vault.

```python
import os, subprocess, re, hashlib, datetime
from langchain_openai import AzureOpenAI
from dotenv import dotenv_values

os.getcwd()
root = '/path/to/a/directory/gh-diff'
config = dotenv_values('.env')
# {key:val for key, val in config.items()}
```

A connection the the OpenAI deployment endpoint is initialized. The necessary values are passed in.

```python
llm = AzureOpenAI(
    deployment_name="gh-diff",
    model_name="gpt-3.5-turbo",
    api_key=config['KEY'],
    azure_endpoint=config['ENDPOINT'],
    api_version=config['API_VERSION'], 
    # temperature=2
)
```

The `master` and `feature/development` code branches are compared using the `git diff` command. The output of the command is passed to the python variable `out`.

```python
master, dev = ('master', 'feature/development')

# !cd ../git-repo/ && git diff branch/a..branch/b > ../gh-diff/a-b-diff
command = f"cd ../git-repo/ && git diff {master}..{dev}"
process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
out, err = process.communicate()
```

The contents of the command output are split by line and the file names are captured. The git diff "chunks" are separated and the ones pertaining to binary files are removed. Two lists are initiallized. `plist` is used to collect a list of file names from where the change that the chunk represents came from. `clist` is a list of three-element tuples that contain the name of the file where the change occurred, the contents of that file, and the chunk that represents the change to that file.  

```python
content = out.decode('utf-8')

lines = content.split('\n')
path_stem = '/path/to/a/directory/git-repo'
file_names = [line.split('/')[1:] for line in lines if line.startswith('+++')]
file_names = ['/'.join([path_stem, *line]) for line in file_names if len(line) > 1]

chunks = content.split('diff --git')
chunks = [chunk for chunk in chunks[1:] if chunk.find('Binary files') < 0]
clist = []
plist = []
for chunk in chunks:
    lines = chunk.split('\n')[3:]
    file_path = lines[0].split(' b/')[1]
    plist.append(file_path)
    path = os.path.join(path_stem, file_path)
    with open(path, 'r') as f:
        source = f.read()
    chunk = '\n'.join(lines[1:-1])
    clist.append((file_path, source, chunk))
```

In order to pass the context of the change to the LLM model, we search the git repo for a reference to the file name collected above. The search string represents that file name being sought. Every file in the `src` path is read to search for the file name string, and when that string is found, the `src` filepath is saved to a dictionary using the source string as the key.

```python
source_dict = dict()
for path in plist:
    search_string = path.split('/')[-1]
    # Use os.walk to get all files in the directory and subdirectories
    for root, dirs, files in os.walk(path_stem):
        for filename in files:
            file_path = os.path.join(root, filename)
            with open(file_path, 'r') as f:
                # Read the file
                try:
                    content = f.read()
                    # If the search string is in the content, print the filename
                    if search_string in content:
                        source_dict.update({search_string:file_path})
                        print(f"Found '{search_string}' in {file_path}")
                except UnicodeDecodeError:
                    # Skip files that can't be decoded to text
                    pass
```

The `clist` is processed to grab the shortened file path that is the key in the dictionary above, `file`. The `source` represents the full contents of the original file change. The `chunk` is the actual change. The `file` is processed to extract the key value, which is used to look up the contents of the file where the `source` is referenced. The contents of that file are trimmed of comments and truncated to 7000 tokens to guarantee that enough tokens are left over to deliver a non-trivial result. The results are written to a file titled `master-feature-diff-report-%Y%m%d%H%M%S.txt`. The last bit is the year, month, day, hour, minute, and second concatenated together for uniqueness.

```python
pattern = r"<!--.*?-->"
for n in range(5):
    diff_list = []
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    hash_object = hashlib.sha1(timestamp.encode())
    hash_hex = hash_object.hexdigest()
    filename = f'master-feature-diff-report-{timestamp}.txt'

    for file, source, chunk in clist:
        file_path = file.split('/')[-1]
        try:
            with open(source_dict.get(file_path), 'r') as f:
                source_content = f.read()
                source_content = source_content.split('\n')
                source_content = [line for line in source_content if line.strip() and not line.startswith('//') and not line.strip().startswith(r'\n')]
                source_content = '\n'.join(source_content)
                source_content = re.sub(pattern, '', source_content, flags=re.DOTALL)[:7000]
                # source_content = ' '.join(source_content)
        except Exception as e:
            # print(e)
            source_content = None
        query = f"""
        Create a unit test to test the change identified in the github diff chunk: {chunk}. 
        Write the test using Mulesoft's MUnit module. The changes are to the file {source}.
        The file contents are referenced in {source_content}. 
        """
        if source_content:
            try:
                result = llm(query, max_tokens=2000)
                # pass
            except Exception as e:
                # print(e)
                result = None
            chunk = file_path + '\n' + chunk
            diff_list.append((chunk, result))

    with open(filename, 'w') as f:
        for txt, res in diff_list:
            if res and (len(res) < 100): continue
            f.write(f"{txt}\n{80 * '='}\n")
            if res:
                f.write(res)
            else:
                f.write("No result")
            f.write(f"\n{80 * '='}\n{80 * '='}\n")
```
Overall the exercise when interesting, although the output was mostly garbage. Github is in the process of building this kind of thing anyway.
