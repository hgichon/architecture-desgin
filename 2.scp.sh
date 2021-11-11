NODE1=10.0.5.119
#NODE2=10.0.5.120
NGD1=10.1.1.2
NGD2=10.1.2.2
BASEDIR=/root/workspace/usr/kch/distributed
CSD_BASEDIR=/home/ngd/workspace/usr/kch/ditributed


#sshpass -p "ketilinux" scp -r nodes root@$NODE1:$BASEDIR/nodes
#sshpass -p "ketilinux" scp -r nodes root@$NODE2:$BASEDIR/nodes


sshpass -p "1234" ssh -o StrictHostKeyChecking=no ngd@$NGD1 mkdir -p $CSD_BASEDIR/nodes/csd
#sshpass -p "1234" ssh -o StrictHostKeyChecking=no ngd@$NGD2 mkdir -p $CSD_BASEDIR/nodes/csd

sshpass -p "1234" scp -r nodes/csd/* ngd@$NGD1:$CSD_BASEDIR/nodes/csd
#sshpass -p "1234" scp -r nodes/csd/* ngd@$NGD2:$CSD_BASEDIR/nodes/csd





