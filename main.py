from indexer import BinlogIndexer

indexer = BinlogIndexer(base_path="./")
indexer.add("mysql-bin.000300")
# indexer.remove("mysql-bin.000300")
