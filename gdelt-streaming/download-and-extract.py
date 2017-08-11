import urllib
import zipfile
import datetime
import os

NUMBER_OF_DAYS = 90
file = urllib.URLopener()
base = datetime.datetime.today()
date_list = [base - datetime.timedelta(days=x) for x in range(1, NUMBER_OF_DAYS)]

if not os.path.exists("zipped/"):
    os.makedirs("zipped/")

if not os.path.exists("daily/"):
    os.makedirs("daily/")

for d in date_list:
	filename = d.strftime('%Y%m%d.export.CSV.zip')
	url = "http://data.gdeltproject.org/events/" + filename
	zipped = "zipped/" + filename
	file.retrieve(url, zipped)
	zip_ref = zipfile.ZipFile(zipped, 'r')
	zip_ref.extractall("daily/")
	zip_ref.close()

master_file = str(NUMBER_OF_DAYS) + '-days.csv'
with open(master_file, 'w') as outfile:
    for f in sorted(os.listdir("daily")):
        with open("daily/" + f) as infile:
            	outfile.write(infile.read())
