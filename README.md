Installation
------------
Comet requires a running redis server:

  sudo apt-get install redis
  sudo systemctl start redis
  
Download comet:

  git clone https://github.com/chime-experiment/comet.git
  cd comet
  
pymysql requires mysql_config so install:

  sudo apt-get install mysql-server libmysqlclient-dev
  
Install comet:

  sudo pip3 install . -r requirements.txt
  
Running Comet
-------------
To see the usage of comet, run:

  comet --help
