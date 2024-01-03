import csv
import threading
from storage.validator.mysql_validator_storage import MysqlValidatorStorage
import datetime as dt
import mysql.connector


def clear_validator_storage():
    # Clean up the test database.
    test_storage = MysqlValidatorStorage("localhost", "test-user", "test-pw", "test_db")
    cursor = test_storage.connection.cursor()
    cursor.execute("DROP TABLE MinerIndex2")
    cursor.execute("DROP TABLE Miner")
    cursor.execute("DROP TABLE Label")
    cursor.execute("DROP TABLE Bucket")
    test_storage.connection.commit()


def load_validator_storage(
    miners: int, unique_buckets: int, labels: int, total_buckets: int
):
    # Connect to already setup local test storage
    test_storage = MysqlValidatorStorage("localhost", "test-user", "test-pw", "test_db")
    cursor = test_storage.connection.cursor()

    # Create miners
    last_updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    # Distribute credibilities evenly from 1 to 100%
    miner_values = [
        [
            "hotkey" + str(i),  # hotkey
            last_updated,  # lastUpdated
            i % 100 + 1,  # credibility
        ]
        for i in range(1, miners + 1)
    ]

    cursor.executemany(
        "INSERT IGNORE INTO Miner2 (hotkey, lastUpdated, credibility) VALUES (%s, %s, %s)",
        miner_values,
    )
    test_storage.connection.commit()

    # Create labels
    label_values = [["label" + str(i)] for i in range(1, labels + 1)]

    cursor.executemany(
        "INSERT IGNORE INTO Label (labelValue) VALUES (%s)",
        label_values,
    )
    test_storage.connection.commit()

    # Create buckets
    bucket_values = [
        [
            i,  # timeBucketId
            i % 2,  # source
            i % labels + 1,  # labelId (these auto increment and therefore will match)
            1000,  # credAdjSize
        ]
        for i in range(1, unique_buckets + 1)
    ]
    cursor.executemany(
        "INSERT IGNORE INTO Bucket (timeBucketId, source, labelId, credAdjSize) VALUES (%s, %s, %s, %s)",
        bucket_values,
    )
    test_storage.connection.commit()

    # Create miner index in chunks of 1m
    buckets_per_miner = total_buckets // miners
    index_values = [
        [
            i // buckets_per_miner
            + 1,  # minerId (these auto increment and therefore will match)
            i % unique_buckets
            + 1,  # bucketId (these auto increment and therefore will match)
            100,  # contentSizeBytes
        ]
        for i in range(0, total_buckets)
    ]

    # file_path = "test.csv"
    # with open(file_path, mode="w", newline="") as file:
    #    writer = csv.writer(file)
    #    writer.writerows(index_values)

    # load_query = """LOAD DATA INFILE 'test.csv' INTO TABLE MinerIndex2
    #                FIELDS TERMINATED BY ','
    #                LINES TERMINATED BY '\n'
    #                """

    # Maybe skip header?
    # cursor.execute(load_query)

    value_subsets = [
        index_values[x : x + 1000000] for x in range(0, len(index_values), 1000000)
    ]

    inserted_subsets = 0
    for value_subset in value_subsets:
        cursor.executemany(
            """INSERT IGNORE INTO MinerIndex2 VALUES (%s, %s, %s)""", value_subset
        )
        inserted_subsets += 1
        print(f"Inserted {inserted_subsets * 1000000} rows.")

    test_storage.connection.commit()

    # threads = []

    # for value_subset in value_subsets:
    #    thread = threading.Thread(
    #        target=_insert_rows, args=(test_storage.connection, value_subset)
    #    )
    #    threads.append(thread)
    #    thread.start()

    # for thread in threads:
    #    thread.join()


def _insert_rows(connection, values):
    try:
        print("A thread started inserting")
        connection = mysql.connector.connect(
            host="localhost",
            user="test-user",
            password="test-pw",
            database="test_db",
        )
        cursor = connection.cursor()
        cursor.executemany(
            """INSERT IGNORE INTO MinerIndex2 VALUES (%s, %s, %s)""", values
        )
        connection.commit()
        print("A thread finished inserting")
        cursor.close()
    except Exception as e:
        print(f"Hit exception: {e}")


if __name__ == "__main__":
    clear_validator_storage()
    # Initially use valis approximately matching what we have in prod.
    start = dt.datetime.now()
    print("Started loading: " + str(start))
    load_validator_storage(
        miners=300, unique_buckets=500000, labels=50000, total_buckets=3000000
    )
    # 2.5m buckets and 15m rows for full
    end = dt.datetime.now()
    print("Finished Loading: " + str(end))
    print("Total seconds: " + str((end - start).total_seconds()))
