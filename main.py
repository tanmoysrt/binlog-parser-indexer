from indexer import BinlogIndexer

indexer = BinlogIndexer(base_path="./")
# indexer.add("mysql-bin.013096")
indexer.remove("mysql-bin.013096")