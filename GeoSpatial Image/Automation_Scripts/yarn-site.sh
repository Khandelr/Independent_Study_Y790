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
        <name>yarn.resourcemanager.hostname</name>
        <value>$Master_IP</value>
        <description>The hostname of the RM.</description>
</property>\n\n"

partb="<property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
        <description>Minimum limit of memory to allocate to each container request at the Resource Manager.</description>
</property>\n\n"

partc="<property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>2048</value>
        <description>Maximum limit of memory to allocate to each container request at the Resource Manager.</description>
</property>\n\n"

partd="<property>
        <name>yarn.scheduler.minimum-allocation-vcores</name>
        <value>1</value>
        <description>The minimum allocation for every container request at the RM, in terms of virtual CPU cores. Requests lower than this won't take effect, and the specified value will get alloc$
</property>\n\n"

parte="<property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>2</value>
        <description>The maximum allocation for every container request at the RM, in terms of virtual CPU cores. Requests higher than this won't take effect, and will get capped to this value.</d$
</property>\n\n"

partf=" <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>4096</value>
        <description>Physical memory, in MB, to be made available to running containers</description>
</property>\n\n"

partg="<property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>4</value>
        <description>Number of CPU cores that can be allocated for containers.</description>
</property>
</configuration>"

data="$data$parta$partb$partc$partd$parte$partf$partg"

echo "$data"

rm yarn-site.xml

echo -e "$data" >>/home/ubuntu/Images/TextFiles/yarn-site.xml



