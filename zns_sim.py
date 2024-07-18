# Simple Zone Namespace SSD & File System simulator

class File:
    def __init__(self, filesize, inode):
        self.size = filesize
        self.data_written = 0
        self.inode = inode
        self.status = 'created'
        self.chunk_list = []

    def addChunk(self, file_chunk):
        self.chunk_list.append(file_chunk)


class FileChunk:
    def __init__(self, inode, logi_unit, id, chunck_size, life_time=0):
        self.inode = inode
        self.logi_unit = logi_unit # Pointer to the Logical data unit
        self.id = id
        self.size = chunck_size
        self.life_time = life_time
        self.is_stale = False

    def markStale(self):
        self.is_stale = True
        self.life_time = 0

    def print(self):
        print('inode: {}, Chunk {}: size {}, is_stale {}, life {}'.format(self.inode, self.id, self.size, self.is_stale, self.life_time))


class LogiDataGroup:
    def __init__(self, id, num_of_group):
        self.id = id
        self.num_of_group = num_of_group
        self.group_list = []
        self.remain_space = 0
        self.max_space = 0

    # Note! FileSystem needs to call updateRemainSpace() after modifying files
    def updateRemainSpace(self):
        self.remain_space = 0
        for item in self.group_list:
            self.remain_space += item.updateRemainSpace()
        return self.remain_space

    def isFull(self):
        if self.remain_space > 0:
            return False
        else:
            return True

    def resetState(self):
        for item in self.group_list:
            item.resetState()
        self.updateRemainSpace()

    def writeFile(self, file: File):
        # print("writeFile() in ", self.name, self.id) # Debug
        if file.data_written >= file.size:
            return 0
        data_written = 0
        for item in self.group_list:
            ret = item.writeFile(file)
            data_written += ret
        self.updateRemainSpace()
        return data_written

    def writeChunk(self, file_chunk):
        for item in self.group_list:
            if item.writeChunk(file_chunk) == True:
                self.remain_space -= file_chunk.size
                return True
        return False

    def getFileChunkList(self, file_chunk_list):
        for item in self.group_list:
            item.getFileChunkList(file_chunk_list);

    def markStale(self, file: File):
        for item in self.group_list:
            item.markStale(file)

    def getStaleSize(self):
        stale_size = 0
        for item in self.group_list:
            stale_size += item.getStaleSize()
        return stale_size

    def print(self):
        print('{} {}: '.format(self.name, self.id))
        for item in self.group_list:
            item.print()


class LogiDataUnit:
    def __init__(self, id, zone_id, block_id, max_size):
        self.id = id
        self.zone_id = zone_id
        self.block_id = block_id
        self.max_size = max_size
        self.remain_space = max_size
        self.file_chunk_list = []

    def updateRemainSpace(self):
        self.remain_space = self.max_size
        for file_chunk in self.file_chunk_list:
            self.remain_space -= file_chunk.size
        return self.remain_space

    def resetState(self):
        self.file_chunk_list.clear()
        self.remain_space = self.max_size

    def writeFile(self, file: File):
        ''' Debug
        print('writeFile() in LogiDataUnit, max_size=', self.max_size, ', remain=',self.remain_space)
        print('Before: Data written', file.data_written, '/', file.size, '; ', end='')
        '''
        if self.remain_space == 0:
            return 0
        if file.data_written >= file.size:
            return 0
        file_remain_size = file.size - file.data_written
        data_written = 0
        if file_remain_size >= self.remain_space:
            data_written = self.remain_space
            self.remain_space = 0
        else:
            data_written = file_remain_size
            self.remain_space -= file_remain_size
        new_chunk = FileChunk(file.inode, self, len(file.chunk_list), data_written)
        # addChunk will update file.data_written
        file.addChunk(new_chunk)
        file.data_written += data_written
        self.file_chunk_list.append(new_chunk)

        # print('After: Data written', file.data_written, '/', file.size) # Debug
        return data_written

    def writeChunk(self, file_chunk):
        if self.remain_space == 0:
            return False
        if file_chunk.size > self.remain_space:
            return False
        file_chunk.logi_unit = self
        self.file_chunk_list.append(file_chunk)
        self.remain_space -= file_chunk.size
        return True

    def markStale(self, file: File):
        for file_chunk in self.file_chunk_list:
            if file_chunk.inode == file.inode:
                file_chunk.markStale(True)

    def getStaleSize(self):
        stale_size = 0
        for file_chunk in self.file_chunk_list:
            if file_chunk.is_stale:
                stale_size += file_chunk.size
        return stale_size

    def getFileChunkList(self, file_chunk_list):
        for file_chunk in self.file_chunk_list:
            file_chunk_list.append(file_chunk)

    def print(self):
        for file_chunk in self.file_chunk_list:
            file_chunk.print()


class Block(LogiDataGroup):
    # default size is 4K
    def __init__(self, id, zone_id, block_size=4096):
        num_of_group=1 # Currently, Block is the basic unit in our Simulator
        super().__init__(id, num_of_group)
        self.name = 'Block'
        self.zone_id = zone_id
        self.num_of_group = num_of_group
        self.block_size = block_size

        for i in range(self.num_of_group):
            self.group_list.append(LogiDataUnit(i, self.zone_id, self.id, block_size))
        self.max_space = len(self.group_list)*self.block_size
        self.remain_space = self.max_space


class Zone(LogiDataGroup):
    def __init__(self, id, num_of_group=32768, block_size=4096):
        super().__init__(id, num_of_group)
        self.name = 'Zone'

        for i in range(self.num_of_group):
            self.group_list.append(Block(i, self.id, block_size))
        self.remain_space = self.updateRemainSpace()
        self.max_space = self.remain_space


class SSD(LogiDataGroup):
    # Default number of zones is 32
    def __init__(self, id, num_of_zones=32, num_of_blocks=32768, block_size=4096):
        super().__init__(id, num_of_zones)
        self.name = 'SSD'
        self.num_of_zones = num_of_zones
        self.num_of_blocks = num_of_blocks
        self.block_size = block_size
        self.max_space = num_of_zones * num_of_blocks * block_size
        self.remain_space = self.max_space

        for i in range(self.num_of_group):
            self.group_list.append(Zone(i, num_of_blocks, block_size))

    def writeFile(self, file: File):
        if file.size > self.remain_space:
            #print("Error! Not enough space in ", self.name, ' Filesize: ', file.size, ', Remain: ', self.remain_space) #debug
            return -1
        return super().writeFile(file)
    
    def appendFile(self, file: File, data_size):
        if data_size > self.remain_space:
            return -1
        return super().writeFile(file)
    
    def writeFileToZone(self, file: File, zone_id):
        if file.size > self.group_list[zone_id].remain_space:
            #print("Error! Not enough space in Zone ", zone_id, ' Filesize: ', file.size, ', Remain: ', self.group_list[zone_id].remaind_space) #debug
            return -1
        return self.group_list[zone_id].writeFile(file)

    def getFileChunk(self, zone_id, block_id, chunk_id):
        return self.group_list[zone_id].group_list[block_id].group_list[0].file_chunk_list[chunk_id]

class ZnsFileSystem:

    def __init__(self, num_of_zones=32, num_of_blocks=32768, block_size=4096, verbose=False):
        self.verbose = verbose
        self.zone_gc_threshold = 0
        self.gc_migrate_times = 0
        self.gc_zone_reset_times = 0
        self.inode = 0
        self.file_list = []
        self.ssd = SSD(0, num_of_zones, num_of_blocks, block_size)

    def createFile(self, size):
        file = File(size, self.inode)
        data_written = self.ssd.writeFile(file)
        if (data_written == -1):
            print("Error! Not enough space in SSD")
            return -1
        if (data_written != file.size):
            print("Error! Cannot write all data! ", data_written, "/", file.size)
            return -2
        
        if (self.verbose):
            print("File {} (size: {}) is created.".format(self.inode, size))

        self.file_list.append(file)
        self.inode += 1
        return file.inode
    
    def createFileOnZone(self, size, zone_id):
        file = File(size, self.inode)
        data_written = self.ssd.writeFileToZone(file, zone_id)
        if (data_written == -1):
            print("Error! Not enough space in SSD")
            return -1
        if (self.verbose):
            print("File {} (size: {}) is created on Zone {}.".format(self.inode, size, zone_id))

        self.file_list.append(file)
        self.inode += 1
        return file.inode

    def deleteFile(self, inode):
        if inode >= len(self.file_list):
            print('Error! Unknown file inode.')
            return -2

        file = self.file_list[inode]
        file.status = 'deleted'
        for chunk in file.chunk_list:
            chunk.markStale()
        
        if (self.verbose):
            print("File " + str(inode) + "'s chunks are marked stale.")
        # TODO: don't pop now, we use inode as index sometimes
        # self.file_list.pop(i)
        self.updateLifeTime()

    def deleteFileChunks(self, inode, beg_id, end_id):
        if inode >= len(self.file_list):
            print('Error! Unknown file inode.')
            return -2
        
        file = self.file_list[inode]
        for i in range(beg_id, end_id):
            file.chunk_list[i].markStale()
            file.size -= file.chunk_list[i].size
        del file.chunk_list[beg_id : end_id]
            

        
    def appendFile(self, inode, data_size):
        if inode > len(self.file_list):
            print('Error! Unknown file inode.')
            return -2
        if data_size > self.ssd.remain_space:
            print('Error! Not enough space in SSD.')
            return -1
        file = self.file_list[inode]
        file.size += data_size
        self.ssd.appendFile(file, data_size)
        
        if (self.verbose):
            print("Data {} have been appended to File {}.".format(data_size, file.inode))

    def printDataWritten(self):
        for file in self.file_list:
            print('File {} => Size / DataWritten = {} / {}'.format(file.inode, file.size, file.data_written))

    def setZoneGCThreshold(self, threshold):
        self.zone_gc_threshold = threshold

    def moveOneChunk(self, file_chunk, src_zone_id, dst_zone_id):
        # Create a new FileChunk, and call Zone.writeChunk() to search a LogiDataUnit to save it
        new_chunk = FileChunk(file_chunk.inode, None, file_chunk.id, file_chunk.size, file_chunk.life_time)
        self.file_list[file_chunk.inode].addChunk(new_chunk)
        self.ssd.group_list[dst_zone_id].writeChunk(new_chunk) # Zone list
        file_chunk.markStale()

    def gcStaleGreedy(self):
        # Find the zone with max stale FileChunk size
        max_stale = 0
        zone_id = -1
        zone_list = self.ssd.group_list
        for i, zone in enumerate(zone_list):
            stale_size = zone.getStaleSize()
            if max_stale < stale_size:
                max_stale = stale_size
                zone_id = i

        if zone_id > -1:
            # Trigger GC when total stale data exceed GC threshold
            if max_stale / zone.max_space < self.zone_gc_threshold:
                return 0

            # Copy chunks
            zone_file_chunk_list = []
            zone_list[zone_id].getFileChunkList(zone_file_chunk_list)
            for file_chunk in zone_file_chunk_list:
                if file_chunk.is_stale:
                    continue

                # Find a new space to copy the chunk
                for i, zone in enumerate(zone_list):
                    if i == zone_id:
                        continue

                    if file_chunk.size <= zone.remain_space:
                        self.moveOneChunk(file_chunk, src_zone_id=zone_id, dst_zone_id=zone.id)
                        self.gc_migrate_times += 1
                        if (self.verbose):
                            print("Chunk (", file_chunk.inode, ",", file_chunk.id, ") in zone ", zone_id, " is moved to zone " + str(i))
                        break

                if i == len(zone_list):
                    print("Error! FileChunk is too large! (inode, size) = (", file_chunk.inode, ",", file_chunk.size, ")")
                    return -1

            self.ssd.group_list[zone_id].resetState()
            self.ssd.updateRemainSpace()
            self.gc_zone_reset_times += 1
            if (self.verbose):
                print("Zone " + str(zone_id) + " is reset.")

        if (self.verbose):
            print("Garbage collection done.")
        
        return 1

    def garbageCollection(self):
        self.gcStaleGreedy()

    def updateLifeTime(self):
        for file in self.file_list:
            if file.status == 'deleted':
                continue
            for chunk in file.chunk_list:
                chunk.life_time += 1

    def printFileChunks(self):
        for file in self.file_list:
            for chunk in file.chunk_list:
                chunk.print()

    def printSSD(self):
        self.ssd.print()

    def printGCStats(self):
        print("GC Stats:")
        print("Migration times:", self.gc_migrate_times)
        print("Zone reset times:", self.gc_zone_reset_times)
