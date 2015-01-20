# Hadoop_Genomic_Analysis
This is a Hadoop version genome analysis tool on HiC data with the purpose of mapping HiC intra interactions to genes, then obtaining a gene-gene interaction network and enabling further study.

Please refer Spark version for more details.

How to compile and run:

mkdir GenomeAnalysis

javac -classpath /usr/local/hadoop-1.2.1/hadoop-core-1.2.1.jar -d ./GenomeAnalysis/ *.java

jar -cvf GenomeAnalysis.jar -C ./GenomeAnalysis/ .

/usr/local/hadoop-1.2.1/bin/hadoop jar GenomeAnalysis.jar GenomeAnalysis /user/group5/project/input /user/group5/project/gene /user/group5/project/output 4 4
