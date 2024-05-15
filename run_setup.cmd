set PATH_TO_MONICA_BIN_DIR=c:\Users\giri\Documents\monica_win64_3.6.13\bin
set PATH_TO_PYTHON=c:\Users\giri\AppData\Local\anaconda3\python.exe
set MONICA_PARAMETERS=%cd%\data\monica-parameters
echo "MONICA_PARAMETERS=%MONICA_PARAMETERS%"

START "ZMQ_IN_PROXY" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-proxy -pps -f 6666 -b 6677 &
START "ZMQ_OUT_PROXY" /MIN %PATH_TO_MONICA_BIN_DIR%\monica-zmq-proxy -pps -f 7788 -b 7777 &

START "ZMQ_MONICA_1" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_2" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_3" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_4" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788
START "ZMQ_MONICA_5" %PATH_TO_MONICA_BIN_DIR%\monica-zmq-server -ci -i tcp://localhost:6677 -co -o tcp://localhost:7788

echo "run producer"
START "run-calibration.py" %PATH_TO_PYTHON% run-calibration.py

echo "killing proxies"
taskkill /FI "WindowTitle eq ZMQ_IN_PROXY*" /T /F
taskkill /FI "WindowTitle eq ZMQ_OUT_PROXY*" /T /F

echo "killing MONICAs
taskkill /FI "WindowTitle eq ZMQ_MONICA_*" /T /F