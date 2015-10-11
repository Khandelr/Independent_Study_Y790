. config.file

echo "Master IP Address: "$Master_IP

ip_Slaves=""

echo $ip_Slaves 

for((var = 1; var<=$Total_Slaves; var++))
do
        varSlave="Slave"$var"_IP"
        echo "Slave"$var" IP Address: "${!varSlave}
        if [ $var == $Total_Slaves ]
        then
                ip_Slaves+="${!varSlave}"
        else
                ip_Slaves+="${!varSlave},"
        fi
done

echo $ip_Slaves

while read line
do
        if [ "$line" != "</configuration>" ]
        then
                echo "$line"
                data+="$line\n"
        fi
done  < /home/ubuntu/Images/TextFiles/try.xml

parta="<property>
        <name>hbase.master</name>
        <value>$Master_IP</value>
</property>\n\n"

partb="<property>
        <name>hbase.zookeeper.quorum</name>
        <value>$ip_Slaves</value>
</property>"

partc="<property>
        <name>hbase.rootdir</name>
        <value>hdfs:///master/hbase</value>
</property>"

partd="<property>
        <name>hbase.zookeeper.property.dataDir</name>
        <value>/home/ubuntu/zookeeper</value>
</property>"

parte="<property>
		<name>hbase.cluster.distributed</name>
        <value>true</value>
</property>

</configuration>"

data="$data$parta$partb$partc$partd$parte"

echo "$data"

rm hbase-site.xml

echo -e "$data" >>/home/ubuntu/Images/TextFiles/hbase-site.xml
