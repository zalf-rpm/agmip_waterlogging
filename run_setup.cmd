set PATH_TO_MONICA_BIN_DIR=c:\Users\giri\Documents\monica_win64_3.6.13\bin
set PATH_TO_PYTHON=c:\Users\giri\AppData\Local\anaconda3\python.exe
set MONICA_PARAMETERS=%cd%\data\monica-parameters
echo "MONICA_PARAMETERS=%MONICA_PARAMETERS%"

START "MONICA" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788

echo "run producer"
START "producer" %PATH_TO_PYTHON% run-producer.py

echo "run consumer"
%PATH_TO_PYTHON% run-consumer.py

