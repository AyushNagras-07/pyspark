python3 --version
Python 3.8+ is good

Step 1: Install Java 
  Spark needs Java.
  for linux 
    sudo apt update
    sudo apt install openjdk-11-jdk -y
  Verify:
    java -version
Step 2: Install PySpark (includes Spark)
    pip install pyspark
  Verify:
    pyspark --version
Step 3: Test PySpark (most important step)
  Run:
    pyspark
  You should see:
    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /___/ .__/\_,_/_/ /_/\_\   version X.X.X
