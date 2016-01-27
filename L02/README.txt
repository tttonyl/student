To run the Shakespear wordcount demos on Hadoop:

Run inline:
$ python wordcount.py -r inline hamlet.txt
$ python wordcount_top10.py -r inline hamlet.txt

Run locally:
$ python wordcount.py -r local hamlet.txt
$ python wordcount_top10.py -r local hamlet.txt

Run on hadoop:
$ export HADOOP_HOME=/usr/lib/hadoop-mapreduce
$ python wordcount.py -r hadoop --hadoop-bin=/usr/bin/hadoop hamlet.txt
$ python wordcount_top10.py -r hadoop --hadoop-bin=/usr/bin/hadoop hamlet.txt








