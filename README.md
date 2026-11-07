# Hadoop and Spark Text Analytics
This project counts words and characters in text files using Hadoop and Spark. It also finds the words with the lowest and highest frequencies.

Created for USC EE 542 Laboratory 4 using AWS EC2 and Project Gutenberg books.

## Files
| File | What it does |
| --- | --- |
| `wordcount_mapper.py` | Splits text into lowercase words. |
| `wordcount_reducer.py` | Adds up each word's count. |
| `minmax_reducer.py` | Finds the lowest and highest word counts. |
| `char_mapper.py` | Reads individual characters. |
| `char_reducer.py` | Adds up each character's count. |
| `spark_wordcount.py` | Counts words and prints the top 30 using Spark. |
| `spark_charcount.py` | Counts characters using Spark. |
| `spark_minmax.py` | Finds the lowest and highest word counts using Spark. |

## 1. Set up the tools
The lab uses Ubuntu 22.04, Python 3, Java 11, Hadoop 3.3.6, and Spark 3.5.0. Run the commands in a Linux terminal. Keep all eight Python files in the same folder.

If your cluster is already configured, skip the setup details below.

<details>
<summary>Open installation and Hadoop setup instructions</summary>

### Install the software
Use a fresh Ubuntu laboratory environment, with two nodes named `master` and `worker`. The examples assume the login user is `ubuntu` and the CPU architecture is amd64. Run installation steps on **both nodes**.

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk python3 ssh rsync wget
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
```

Check each archive against its matching Apache checksum before extracting. The following assumes `/usr/local/hadoop` and `/usr/local/spark` do not already contain installations:

```bash
tar -xvzf hadoop-3.3.6.tar.gz
tar -xvzf spark-3.5.0-bin-hadoop3.tgz
sudo mv hadoop-3.3.6 /usr/local/hadoop
sudo mv spark-3.5.0-bin-hadoop3 /usr/local/spark
sudo chown -R ubuntu:ubuntu /usr/local/hadoop /usr/local/spark
```

### Set environment variables
Run once on **both nodes**. Adjust `JAVA_HOME` if your Java 11 installation uses another path:

```bash
cat <<'EOF' >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PYSPARK_PYTHON=python3
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin
EOF
source ~/.bashrc
```
Also set Java explicitly for Hadoop daemons, including noninteractive SSH launches, on both nodes:

```bash
printf '\nexport JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64\n' >> "$HADOOP_CONF_DIR/hadoop-env.sh"
```

### Set up hostnames and SSH
On both nodes, configure `/etc/hosts` so `master` and `worker` resolve to their actual EC2 private addresses. Example only:

```text
10.0.1.10 master
10.0.1.11 worker
```

Replace these addresses with yours. Allow cluster traffic between the nodes using their private network/security group. Keep Hadoop services private and allow administration SSH from your own address. Verify:
```bash
getent hosts master worker
```

On the master, create a dedicated Hadoop SSH key if it does not exist; preserve any existing key:
```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
if [ ! -e ~/.ssh/id_ed25519_hadoop ]; then
  ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519_hadoop
fi
cat ~/.ssh/id_ed25519_hadoop.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

Copy **only the public key** to the worker using your existing EC2 SSH access. The following assumes that access is already available through an SSH agent or configuration. If needed, add `-i /path/to/your-existing-ec2-key.pem` to both `scp` and `ssh`:
```bash
scp ~/.ssh/id_ed25519_hadoop.pub ubuntu@worker:/tmp/master_hadoop.pub
ssh ubuntu@worker 'mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat /tmp/master_hadoop.pub >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && rm /tmp/master_hadoop.pub'
```

Add the following host settings to the master's `~/.ssh/config`, merging them with any existing matching entries:
```text
Host master worker
    User ubuntu
    IdentityFile ~/.ssh/id_ed25519_hadoop
    IdentitiesOnly yes
```

Set `chmod 600 ~/.ssh/config`. Connect to both hosts interactively once to verify their host fingerprints, then confirm passwordless access:
```bash
ssh master hostname
ssh worker hostname
ssh -o BatchMode=yes master hostname
ssh -o BatchMode=yes worker hostname
```

Hadoop's startup scripts need access to **master itself as well as worker**, because both hosts are listed as workers. Never upload either private key to GitHub.

### Configure Hadoop
Write these files on **both nodes**, after setting `HADOOP_CONF_DIR`. These are fresh-lab examples; back up existing configuration before replacing it.

#### core-site.xml
```bash
cat > "$HADOOP_CONF_DIR/core-site.xml" <<'XML'
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://master:9000</value></property>
</configuration>
XML
```

#### hdfs-site.xml
```bash
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<'XML'
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///usr/local/hadoop/dfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///usr/local/hadoop/dfs/data</value></property>
</configuration>
XML
```

#### mapred-site.xml
```bash
cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<'XML'
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.application.classpath</name><value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value></property>
</configuration>
XML
```

#### yarn-site.xml
```bash
cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<'XML'
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>2048</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>2</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>2048</value></property>
</configuration>
XML
```
The resource values are starting points for small lab nodes. Adjust them to actual available RAM and CPU, allowing space for the operating system and Hadoop daemons.

#### Workers and storage
```bash
printf 'master\nworker\n' > "$HADOOP_CONF_DIR/workers"
mkdir -p /usr/local/hadoop/dfs/name /usr/local/hadoop/dfs/data
```

For a one-node lab, list only the active node and use replication one. DataNodes alone do not establish distributed compute; also check YARN NodeManagers and job placement.

### Start a new cluster
On the master, format the NameNode **only once for a new empty cluster**. Do not run this on an existing HDFS installation:

```bash
hdfs namenode -format
```

Start the services on the master:
```bash
start-dfs.sh
start-yarn.sh
hdfs dfsadmin -report
yarn node -list
```

Require two live DataNodes and two RUNNING NodeManagers for the two-node setup. If HDFS permissions prevent creating `/gutenberg` or output paths, have the HDFS administrator create writable directories for the lab user. Run all job commands from the folder containing the eight scripts.

</details>

## 2. Start Hadoop
On the master:

```bash
start-dfs.sh
start-yarn.sh
```

## 3. Upload the books
Put your input `.txt` files in a local folder named `books`, then run:

```bash
hdfs dfs -mkdir -p /gutenberg
hdfs dfs -put books/*.txt /gutenberg/
```

## 4. Run Hadoop jobs
Run these from the folder containing your Python files. Each output folder must be new.

### Word count
```bash
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files wordcount_mapper.py,wordcount_reducer.py \
  -input /gutenberg -output /output_wordcount_clean \
  -mapper "python3 wordcount_mapper.py" \
  -reducer "python3 wordcount_reducer.py"
```

### Lowest and highest word counts
Run this after the word-count job. It reads the counts, not the original books.

```bash
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.reduces=1 \
  -files minmax_reducer.py \
  -input /output_wordcount_clean -output /output_wordcount_minmax \
  -mapper /bin/cat -reducer "python3 minmax_reducer.py"
```

### Character count
```bash
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files char_mapper.py,char_reducer.py \
  -input /gutenberg -output /output_charcount \
  -mapper "python3 char_mapper.py" \
  -reducer "python3 char_reducer.py"
```

### View the results
```bash
hdfs dfs -cat '/output_wordcount_clean/part-*'
hdfs dfs -cat '/output_wordcount_minmax/part-*'
hdfs dfs -cat '/output_charcount/part-*'
```

Show the top 20 words:
```bash
hdfs dfs -cat '/output_wordcount_clean/part-*' \
  | sort -t $'\t' -k2,2nr -k1,1 | head -20
```

## 5. Run Spark jobs
```bash
spark-submit spark_wordcount.py
spark-submit spark_charcount.py
spark-submit spark_minmax.py
```

View saved counts:
```bash
hdfs dfs -cat '/output_spark_wordcount_clean/part-*'
hdfs dfs -cat '/output_spark_charcount/part-*'
```

Spark word count prints the top 30 words in the terminal. Spark min/max also prints its results in the terminal.

## 6. Stop Hadoop
```bash
stop-yarn.sh
stop-dfs.sh
```

Stop unused EC2 instances separately to avoid continued instance charges.

## Notes
- Words are converted to lowercase; punctuation separates words. Character counts include spaces and punctuation.
- Spark currently uses `local[*]`, so it computes on one machine even when the books are in HDFS. For YARN, replace the hardcoded `SparkContext("local[*]", ...)` with `SparkContext(appName="JobName")`, then use `spark-submit --master yarn SCRIPT.py`.
- Hadoop saves tab-separated counts; Spark saves tuple-format counts. Character encoding and tied min/max words can differ between the scripts.

## Team

Jacinta Rose Menezes, Yichen Zheng, and Junjun Liu.
