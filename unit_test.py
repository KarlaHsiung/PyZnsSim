from zns_sim import *

def test_write():
    zns_fs = ZnsFileSystem(num_of_zones=3, num_of_blocks=3, block_size=100, verbose=True)

    zns_fs.createFile(400)
    zns_fs.createFile(400)

    # Check if the inode of Zone 1, Block 0's inode is 0
    assert zns_fs.ssd.getFileChunk(1, 0, 0).inode == 0

    # Check if the inode of Zone 1, Block 1's inode is 1
    assert zns_fs.ssd.getFileChunk(1, 1, 0).inode == 1
    assert zns_fs.ssd.remain_space == 100


def test_delete():
    zns_fs = ZnsFileSystem(num_of_zones=2, num_of_blocks=5, block_size=100, verbose=True)

    zns_fs.createFile(150)
    zns_fs.createFile(225)
    zns_fs.createFile(375)
    # Delete only mark file as stale, the real delete is Zone reset
    zns_fs.deleteFile(1) 
    stale_size = zns_fs.ssd.getStaleSize()

    assert zns_fs.ssd.remain_space == 250
    assert stale_size == 225


def test_append():
    zns_fs = ZnsFileSystem(num_of_zones=2, num_of_blocks=2, block_size=100, verbose=True)
    zns_fs.createFile(250)
    zns_fs.createFile(100)
    zns_fs.appendFile(0, 50)

    assert zns_fs.ssd.remain_space == 0
    assert zns_fs.file_list[0].size == 300
    # Check if the inode of Zone 1, Block 1's 2nd Chunk inode is 0, size is 50
    assert zns_fs.ssd.getFileChunk(1, 1, 1).inode == 0
    assert zns_fs.ssd.getFileChunk(1, 1, 1).size == 50

def test_delete_chunks():
    zns_fs = ZnsFileSystem(num_of_zones=3, num_of_blocks=3, block_size=100, verbose=True)

    zns_fs.createFile(400)
    zns_fs.createFile(400)
    zns_fs.deleteFileChunks(0, 1, 3) # Delete File 0's Chunk 1, Chunk 2
    stale_size = zns_fs.ssd.getStaleSize()

    # Check if the inode of Zone 0, Block 0's was marked as Staled
    assert zns_fs.ssd.getFileChunk(0, 1, 0).is_stale == True
    assert zns_fs.ssd.getFileChunk(0, 2, 0).is_stale == True
    assert zns_fs.ssd.getFileChunk(1, 0, 0).is_stale == False
    assert zns_fs.file_list[0].size == 200
    assert zns_fs.ssd.remain_space == 100
    assert stale_size == 200
    
def test_move_one_chunk():
    zns_fs = ZnsFileSystem(num_of_zones=2, num_of_blocks=2, block_size=100, verbose=True)
    zns_fs.createFile(250)
    # Move Zone 0 Block 0's chunk to Zone 1 (shall be written to Block 1)
    dst_zone_id = 1
    file_chunk = zns_fs.ssd.getFileChunk(0, 0, 0)
    zns_fs.moveOneChunk(file_chunk, 0, dst_zone_id)
    org_chunk = zns_fs.ssd.getFileChunk(0, 0, 0)
    new_chunk = zns_fs.ssd.getFileChunk(dst_zone_id, 1, 0)
    
    assert org_chunk.is_stale == True
    assert new_chunk.is_stale == False
    assert org_chunk.id == new_chunk.id
    assert new_chunk.logi_unit.zone_id == dst_zone_id
    assert zns_fs.file_list[0].chunk_list[0].logi_unit.zone_id == dst_zone_id


def test_gc_stale_greedy():
    zns_fs = ZnsFileSystem(num_of_zones=2, num_of_blocks=1, block_size=100) 

    zns_fs.createFile(20)
    zns_fs.createFile(130)
    zns_fs.createFile(25)

    zns_fs.deleteFile(1)
    zns_fs.gcStaleGreedy()
    zns_fs.createFile(40)
    zns_fs.deleteFile(3)
    zns_fs.gcStaleGreedy()
    zns_fs.createFile(65)
    stale_size = zns_fs.ssd.getStaleSize()

    assert zns_fs.gc_migrate_times == 3
    assert zns_fs.gc_zone_reset_times == 2
    assert zns_fs.ssd.remain_space == 50
    assert stale_size == 40

def test_life_time():
    zns_fs = ZnsFileSystem(num_of_zones=2, num_of_blocks=2, block_size=100) 
    zns_fs.createFile(100)
    zns_fs.createFile(100)
    zns_fs.createFile(200)
    zns_fs.deleteFile(0)
    zns_fs.ssd.updateZoneLifeTimeRatio()

    assert zns_fs.ssd.zone_life_time_ratio[0] == 1
    assert zns_fs.ssd.zone_life_time_ratio[1] == 1
