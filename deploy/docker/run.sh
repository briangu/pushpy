# ./build.sh tensorflow
docker run -v $PUSH_HOME:/scm/push -it push/push-tensorflow-gpu:latest bash
