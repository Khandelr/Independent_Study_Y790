. config.file
echo $Master_IP
while read line
do
        if [ "$line" != "</configuration>" ]
        then
                echo "$line"
                data+="$line\n"
        fi
done  < /home/ubuntu/Images/TextFiles/try.xml

part="<property>
<name>fs.defaultFS</name>
<value>hdfs://$Master_IP/</value>
<description>NameNode URI</description>
</property>

</configuration>"

data="$data$part"

echo "$data"

rm core-site.xml

echo -e "$data" >>/home/ubuntu/Images/TextFiles/core-site.xml
