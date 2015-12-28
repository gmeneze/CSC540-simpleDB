package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 * 
 */
public class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	public static Map<String, Integer> bufferPoolMap;

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	 BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i = 0; i < numbuffs; i++){
			
			bufferpool[i] = new Buffer();
			bufferpool[i].index=i;
		}
		bufferPoolMap = new HashMap<String, Integer>();
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool) {
			if (buff.isModifiedBy(txnum)) {
				buff.flush();
			}
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		int buff_index = 0;
		String key;
		Buffer buff = findExistingBuffer(blk);
		
		if (buff == null) {
			buff_index = chooseUnpinnedBufferNew();
			if (buff_index == -1)
				return null;
			buff = bufferpool[buff_index];
			buff.assignToBlock(blk);

			key = blk.fileName() + blk.number();
			bufferPoolMap.put(key, buff_index);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		int buff_index = 0;
		String key;
		buff_index = chooseUnpinnedBufferNew();
		Buffer buff;
		if (buff_index == -1) {
			return null;
		}

		buff = bufferpool[buff_index];
		buff.assignToNew(filename, fmtr);
		numAvailable--;
		buff.pin();

		/* Add the mapping to the bufferPoolMap for this buffer block */
		key = filename + buff.block().number();
		bufferPoolMap.put(key, buff_index);

		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();

		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		String key;
		Integer buff_index = 0;
	

		key = blk.fileName() + blk.number();

		buff_index = bufferPoolMap.get(key);
		if (buff_index == null) {
			return null;
		}
		return bufferpool[buff_index];
	}

	private int chooseUnpinnedBuffer() {
		int buff_index = 0;
		for (Buffer buff : bufferpool) {
			if (!buff.isPinned()) {
				return buff_index;
			}
			buff_index++;
		}
		return -1;
	}

	private int chooseUnpinnedBufferNew() {
		int smallestLSN = -1;
		boolean firstInd = true;
		int returnModifiedBuff_i = -1;
		int returnPinnedBuff_i = -1;
		int buff_index = 0;
		int returnUnusedbuff_i = -1;
		
		
		for (Buffer buff : bufferpool) {
			if (!buff.isPinned()) {
				if (buff.getLogSequenceNumber() == -1) {   
					returnUnusedbuff_i = buff_index;
				}
			}
			buff_index++;
		}		
				
		buff_index = 0;
		
	    if( returnUnusedbuff_i > -1 )
	    {
			return returnUnusedbuff_i;
	    }
				
		for (Buffer buff : bufferpool) {
			if (!buff.isPinned()) {
				if (buff.isModified()) {
					if (firstInd) {
						smallestLSN = buff.getLogSequenceNumber();
						returnModifiedBuff_i = buff_index;
						firstInd = false;
					}

					if (smallestLSN > buff.getLogSequenceNumber()) {
						smallestLSN = buff.getLogSequenceNumber();
						returnModifiedBuff_i = buff_index;
					}
				}
				returnPinnedBuff_i = buff_index;
			}
			buff_index++;
		}
		
		int i=0;
		
		for (Buffer buff : bufferpool) {
			i++;
		}
		
		if (returnModifiedBuff_i != -1) {
			bufferPoolMap.remove(bufferpool[returnModifiedBuff_i].block().fileName() + bufferpool[returnModifiedBuff_i].block().fileName());
			return returnModifiedBuff_i;
		} else if (returnPinnedBuff_i != -1) {
			bufferPoolMap.remove(bufferpool[returnPinnedBuff_i].block().fileName() + bufferpool[returnPinnedBuff_i].block().fileName());
			return returnPinnedBuff_i;
		} else {
			return -1;
		}
	}

	public static void clearMap() {
		bufferPoolMap.clear();
	}

	public static void removeFromMap(Block blk) {
		bufferPoolMap.remove(blk.fileName() + blk.number());
	}
	
	boolean containsMapping (Block blk) {
		return bufferPoolMap.containsKey(blk.fileName() + blk.number());
		}
	
	/*public Buffer getMapping (Block blk) {
		return bufferPoolMap.get(blk);
		}*/
	
	public void getStatistics() {
		for (int i = 0; i < bufferpool.length; i++) {

			System.out.println("The buffer no. "+i+" is read : " + bufferpool[i].getRead() + " times");
			System.out.println("The buffer no. "+i+" is written : " + bufferpool[i].getWrite() + " times");
			System.out.println("The buffer no. "+i+" is modified by the transaction number : "+ bufferpool[i].getModifiedBy());
		}

	}

}