set -x #echo on

python3 c_loader.py 50000
curl -X POST -d'["add", "add", 1, 2, "mul", 3, 4]' -H 'Content-Type: application/json' localhost:11000/math
