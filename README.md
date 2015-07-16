# GameAnalysisDemo

#way to compile

$ mvn clean
$ mvn package

$ hadoop fs -mkdir /user/cloudera/GameLogAnalysis/intput/1
$ hadoop fs -put book.txt /user/cloudera/GameLogAnalysis/input/1
$ hadoop fs -mkdir /user/cloudera/GameLogAnalysis/output/1

$ hadoop jar target/src.mapreduce.demo-0.0.1-SNAPSHOT.jar mapreduce.demo /user/cloudera/GameLogAnalysis/input/1 /user/cloudera/GameLogAnalysis/output/1

$ hadoop fs -cat /user/cloudera/GameLogAnalysis/output/1/*

######### sanpple output
{"author":[{"Test":"book3"},{"Test":"book2"},{"Test":"book1"}],"book":"author1"}
{"author":[{"Test":"book5"},{"Test":"book4"}],"book":"author2"}
{"author":[{"Test":"book6"}],"book":"author3"}



