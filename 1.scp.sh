NODE1=10.0.5.120
NODE2=10.0.5.121
NGD1=10.1.1.2
NGD2=10.1.2.2
BASEDIR=/root/workspace/usr/kch/distributed


sshpass -p "ketilinux" ssh -o StrictHostKeyChecking=no root@$NODE1 mkdir -p $BASEDIR/nodes
sshpass -p "ketilinux" ssh -o StrictHostKeyChecking=no root@$NODE2 mkdir -p $BASEDIR/nodes
sshpass -p "ketilinux" scp -r nodes/* root@$NODE1:$BASEDIR/nodes
sshpass -p "ketilinux" scp -r nodes/* root@$NODE2:$BASEDIR/nodes

sshpass -p "ketilinux" scp -r 2.scp.sh root@$NODE1:$BASEDIR
sshpass -p "ketilinux" scp -r 2.scp.sh root@$NODE2:$BASEDIR



#sshpass -p "1234" ssh -o StrictHostKeyChecking=no ngd@$NGD1 mkdir -p $BASEDIR/nodes/csd
#sshpass -p "1234" ssh -o StrictHostKeyChecking=no ngd@$NGD2 mkdir -p $BASEDIR/nodes/csd

#sshpass -p "1234" scp -r nodes/csd/* ngd@$NGD1:$BASEDIR/nodes/csd
#sshpass -p "1234" scp -r nodes/csd/* ngd@$NGD2:$BASEDIR/nodes/csd


