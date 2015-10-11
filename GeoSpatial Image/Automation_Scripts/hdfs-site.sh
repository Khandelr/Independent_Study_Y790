. config.file
echo $HADOOP_HOME
while read line
do
        if [ "$line" != "</configuration>" ]
        then
                echo "$line"
                data+="$line\n"
        fi
done  < /home/ubuntu/Images/TextFiles/try.xml

parta="<property>
<name>dfs.datanode.data.dir</name>
<value>$HADOOP_HOME/hdfs/datanode</value>
<description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
</property>\n\n"

partb="<property>
<name>dfs.namenode.name.dir</name>
<value>$HADOOP_HOME/hdfs/namenode</value>
<description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
</property>

</configuration>"

data="$data$parta$partb"

echo "$data"

rm hdfs-site.xml

echo -e "$data" >>/home/ubuntu/Images/TextFiles/hdfs-site.xml

