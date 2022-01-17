set -x #echo on

python3 c_hello.py 50000
curl localhost:11000/

python3 c_hello_2.py 50000
curl localhost:11000/

python3 c_hello_revert.py 50000
curl localhost:11000/

