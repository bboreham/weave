#! /bin/bash

. ./config.sh

NAME=seetwo.weave.local

check_attached() {
    assert_raises "proxy exec_on $HOST1 c2 $CHECK_ETHWE_UP"
    assert_dns_record $HOST1 c1 $NAME $C2
}

N=50
start_suite "Proxy restart reattaches networking to containers"

weave_on $HOST1 launch
proxy_start_container          $HOST1 -di --name=c2 --restart=always -h $NAME
proxy_start_container_with_dns $HOST1 -di --name=c1 --restart=always
C2=$(container_ip $HOST1 c2)

proxy docker_on $HOST1 restart -t=1 c2
check_attached

# Kill outside of Docker so Docker will restart it
run_on $HOST1 sudo kill -KILL $(docker_on $HOST1 inspect --format='{{.State.Pid}}' c2)
sleep 1
check_attached

# Restarting proxy shouldn't kill unattachable containers
proxy_start_container $HOST1 -di --name=c3 --restart=always # Use ipam, so it won't be attachable w/o weave
weave_on $HOST1 stop
weave_on $HOST1 launch-proxy
assert_raises "proxy exec_on $HOST1 c3 $CHECK_ETHWE_UP"

# Restart docker itself, using different commands for systemd- and upstart-managed.
run_on $HOST1 sh -c "command -v systemctl >/dev/null && sudo systemctl restart docker || sudo service docker restart"
sleep 1
weave_on $HOST1 launch
check_attached

# Create and remove a lot of containers; the failure mode is that this
# takes a long time as it has to wait for the old ones to time out.
for i in $(seq $N); do
    proxy docker_on $HOST1 run -e WEAVE_CIDR=net:10.32.4.0/28 --rm -t $SMALL_IMAGE /bin/true
done

end_suite
