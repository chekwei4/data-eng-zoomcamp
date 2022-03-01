# Run Spark with Docker
Here's a guide on how to run Spark without install anything. 

Only thing required is to have Docker installed. 

Full credits go to: [Max Melnick](https://maxmelnick.com/2016/06/04/spark-docker.html)

```bash
$ docker run -d -p 8888:8888 -v $PWD:/home/jovyan/work --name spark jupyter/pyspark-notebook
```

The `-d` runs the container in the background.

The `-p 8888:8888` makes the container’s port 8888 accessible to the host (i.e., your local computer) on port 8888. This will allow us to connect to the Jupyter Notebook server since it listens on port 8888.

The `-v $PWD:/home/jovyan/work` allows us to map our spark-docker folder (which should be our current directory - $PWD) to the container’s /home/joyvan/work working directory (i.e., the directory the Jupyter notebook will run from). This makes it so notebooks we create are accessible in our spark-docker folder on our local computer. It also allows us to make additional files such as data sources (e.g., CSV, Excel) accessible to our Jupyter notebooks.

The --name spark gives the container the name spark, which allows us to refer to the container by name instead of ID in the future.

The final part of the command, jupyter/pyspark-notebook tells Docker we want to run the container from the jupyter/pyspark-notebook image.