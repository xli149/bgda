# big-data-viz

## v1 setup

Note: this assumes your python version is 3. If your system defaults to 2, then use `virtualenv3` below.
```
cd ~/where-ever/big-data-viz
virtualenv .
pip install -r requirements.txt
```

## v1 usage

```
cd ~/where-ever/big-data-viz
source bin/activate

# Run in three separate terminals
python ./API_layer.py
python ./AggregatorServer.py
python ./DataEmitter.py
```

Head to http://localhost:5000 to view the updating visualizations as data streams in.
