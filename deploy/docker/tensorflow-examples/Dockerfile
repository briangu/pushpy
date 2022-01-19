FROM eismcc/push-tensorflow-gpu:latest

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY entrypoint.sh /tmp/entrypoint.sh

WORKDIR /tmp

ENTRYPOINT ["/tmp/entrypoint.sh"]
