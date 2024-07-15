from zns_sim import ZnsFileSystem

# Create a ZNS with 1 zones, 1 blocks per zone, and 100 bytes per block
zns_fs = ZnsFileSystem(num_of_zones=2, num_of_blocks=1, block_size=100)
zns_fs.createFile(20)
zns_fs.createFile(130)
zns_fs.createFile(25)
zns_fs.deleteFile(1)
zns_fs.garbageCollection()
zns_fs.createFile(40)
zns_fs.garbageCollection()
zns_fs.createFile(65)
zns_fs.printSSD()