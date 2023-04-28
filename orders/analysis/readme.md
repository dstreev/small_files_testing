## Building Source Dataset

We used [iot-data-utility](https://github.com/dstreev/iot-data-utility) to create the source dataset used for this test.

The source config is a part of the utilities sample configs.

Configuration for [orders](https://github.com/dstreev/iot-data-utility/blob/master/src/main/resources/sample_domains/orders.yaml)

### Running the Generator

[Install](https://github.com/dstreev/iot-data-utility)

Run the generator as a MapReduce Job to generate the source dataset used.

`datagenmr -d /sample_domains/orders.yaml -s order -c 1000000 -m 50 -o datasets/orders`

This creates `datasets/orders` in your `hdfs` home directory.  The job will create 50 files (mappers) with a total of 1 million order records.

The order records are in `json` format and contain an array of `order_items`.

Because the array is random, I recommend doing all testing from the same 'base' dataset.  Do not regenerate if you want to ensure the results are comparable.

If you'd like to see a sample of the dataset before running the map reduce job, run:
`datagencli -d /sample_domains/orders.yaml -s order`
That will generate 500 records to `system.out`.

### Running the Tests


### Parsing the Results

_Grepping for the Long Move on Inserts of Small Files_

`grep '| Set\|| START\|| END\|rows affected\|rows selected\|Run DAG\|TOTAL_LAUNCHED_TASKS' bl_*.out | grep -A 4 'START SMALL_FILE ORDER/ORDER_ITEM Build out' > $HOME/temp/buildout.txt`

`grep '| Set\|| START\|| END\|rows affected\|rows selected\|Run DAG\|TOTAL_LAUNCHED_TASKS' bl_*.out`