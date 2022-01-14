# ./build.sh tensorflow
docker run -v $PUSH_HOME:/scm/push -it eismcc/push-tensorflow-gpu:latest bash
