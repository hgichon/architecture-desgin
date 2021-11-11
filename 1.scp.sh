NODE1=10.0.5.119
#NODE2=10.0.5.120
BASEDIR=/root/workspace/usr/kch/distributed


sshpass -p "ketilinux" ssh -o StrictHostKeyChecking=no root@$NODE1 mkdir -p $BASEDIR/nodes
#sshpass -p "ketilinux" ssh -o StrictHostKeyChecking=no root@$NODE2 mkdir -p $BASEDIR/nodes
sshpass -p "ketilinux" scp -r nodes/* root@$NODE1:$BASEDIR/nodes
#sshpass -p "ketilinux" scp -r nodes/* root@$NODE2:$BASEDIR/nodes

sshpass -p "ketilinux" scp -r 2.scp.sh root@$NODE1:$BASEDIR
#sshpass -p "ketilinux" scp -r 2.scp.sh root@$NODE2:$BASEDIR

