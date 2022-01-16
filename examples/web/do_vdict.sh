set -x #echo on

python3 c_versions.py 50000
curl -X DELETE localhost:11000/greeting
curl -X PUT -d'{"greeting":"Hello, 0"}' -H 'Content-Type: application/json' localhost:11000/greeting
curl -X PUT -d'{"greeting":"Hello, 1!"}' -H 'Content-Type: application/json' localhost:11000/greeting
curl -X PUT -d'{"greeting":"Hello, 2!"}' -H 'Content-Type: application/json' localhost:11000/greeting
curl localhost:11000/greeting
curl localhost:11000/greeting?v=1
curl localhost:11000/greeting?v=0
curl localhost:11000/greeting?v=2
curl -X PUT -d'{"greeting":"Hello, 3!"}' -H 'Content-Type: application/json' localhost:11000/greeting
curl localhost:11000/greeting


