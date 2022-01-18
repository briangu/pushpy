set -x #echo on

python3 c_kvstore.py 50000
curl -X PUT -d'{"k":"my_key", "v":"my_value"}' -H 'Content-Type: application/json' localhost:11000/kv
curl localhost:11000/kv
curl localhost:11000/kv?k=my_key
