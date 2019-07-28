forked from djih

This is an example script that uploads data to Amplitude. The data should be in a zipped file format where each line is a json object in the same format as required by the HTTP API docs: https://amplitude.zendesk.com/hc/en-us/articles/204771828. It is highly recommended to include an insert_id with each event to prevent data duplication on retries.

Usage: `python sample_import.py data.zip starting_line_number` with 0 - starting_line_number
