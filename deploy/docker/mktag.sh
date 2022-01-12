source ../../push/version.py
echo $VERSION
docker tag eismcc/push-tensorflow-gpu eismcc/push-tensorflow-gpu:$VERSION
docker push eismcc/push-tensorflow-gpu:$VERSION 
