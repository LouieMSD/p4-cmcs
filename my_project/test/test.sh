h4 iperf -s -p 5011 -f m > test/result/h4.log &

h1 iperf -c 10.0.4.4 -p 5011 -t 10 -i 1 > test/result/h1.log &
