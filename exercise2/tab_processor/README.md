## Install dependencies
For installing needed dependencies, run `pip install -r requirements.txt`

## Go to the tab_processor directory in the terminal
To navigate to the tab_processor directory, run:
```bash
cd 'path/to/tab_processor'
```
or in vs code, right click on the `tab_processor` folder and select "Open in Integrated Terminal".

## Run the scrapper
To run the scrapper and reload the catalog, execute:

```bash
python scrapper/main.py
``` 
This will create a directory `files`. A `catalog.json` will be created inside, and a `songs` directory will be created. The `songs` directory will contain the downloaded tabs, and the `catalogs` directory will contain the catalogs of songs.

If you want to download tabs for a specific letters range, you can use the `-sc` and `-ec` options, stating or 'start char' and 'end char'. For example, to download tabs for artists starting with letters from A to C, execute:

```bash
python scrapper/main.py -sc a -ec c
```

## Clean the tabs
To clean the downloaded tabs, execute:
```bash
python tab_cleaner/main.py
```
This will create a subdirectory `cleaned` inside the `files` directory, containing the cleaned tabs.

## Validate the cleaned tabs
To validate the cleaned tabs, execute:
```bash
python tab_validator/main.py
```
This will create two subdirectories inside the `files` directory: `validations/ok` and `validations/ko`. The `ok` directory will contain the valid tabs, and the `ko` directory will contain the invalid tabs.

