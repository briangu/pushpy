cat $PUSH_HOME/requirements.txt > $1/requirements.txt
cat $PUSH_HOME/pushpy_examples/requirements.txt >> $1/requirements.txt
echo "git+https://github.com/briangu/push.git" >> $1/requirements.txt
