# Comet - COnfig & MEtadata Tracker

Comet is designed to track various config and metadata properties of the CHIME system,
and to associate dataset with the state of these properties that were active when
the data was collected.

##  Installation
Comet requires a running redis server:
```
sudo apt-get install redis
sudo systemctl start redis
```  
Comet can then be installed in the usual way, either by cloning the repo or installing directly from GitHub:
```
git clone https://github.com/chime-experiment/comet.git
cd comet/
pip install .
```
```
pip install https://github.com/chime-experiment/comet.git
```

## Running Comet
To see the usage of comet, run:
```
comet --help
```
