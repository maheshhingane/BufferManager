package bufmgr;

import global.GlobalConst;
import global.Page;
import global.PageId;
import java.util.HashMap;
import global.Minibase;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a main memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 */
public class BufMgr implements GlobalConst {

    /** Actual pool of pages (can be viewed as an array of byte arrays). */
    protected Page[] bufpool;

    /** Array of descriptors, each containing the pin count, dirty status, etc\
	. */
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames; used for efficient lookups. */
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy to use. */
    protected Replacer replacer;
//-------------------------------------------------------------

  /**
   * Constructs a buffer manager with the given settings.
   * 
   * @param numbufs number of buffers in the buffer pool
   */
  public BufMgr(int numbufs) {

	  /** Initializes the buffer pool as an array of pages. Also initializes each element of the array as a page object. */
	  bufpool = new Page[numbufs];
	  for(int i=0; i<numbufs; i++) {
		  bufpool[i] = new Page();
	  }
    
	  /** Initializes frametab as an array of descriptors. Also initializes each element of the array as a discriptor object. */
	  frametab = new FrameDesc[numbufs];
	  for(int i=0; i<numbufs; i++) {
		  frametab[i] = new FrameDesc(i);
	  }
	  
	  /** Initializes pagemap as a hashmap. Also initializes each record in the hashmap with frametab objects. */
	  pagemap = new HashMap<Integer, FrameDesc>(numbufs);
    
	  /** The replacer attribute initialized as a clock object. */
	  replacer = new Clock(this);
  }

  /**
   * Allocates a set of new pages, and pins the first one in an appropriate
   * frame in the buffer pool.
   * 
   * @param firstpg holds the contents of the first page
   * @param run_size number of pages to allocate
   * @return page id of the first new page
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */

  public PageId newPage(Page firstpg, int run_size) {

	  /** Create new attribute of PageId to hold the id of the first page.
	  * Allocate pages on disk. */
	  PageId pageno =  Minibase.DiskManager.allocate_page(run_size);
	  
	  /** Try to pin the first page. If unsuccessful, deallocate all the pages. */
	  try {
		  pinPage(pageno, firstpg, GlobalConst.PIN_MEMCPY);
	  }
	  catch(Exception e) {
		  Minibase.DiskManager.deallocate_page(pageno, run_size);
		  return null;
	  }

	  /** Return the page id of the first page. */
	  return pageno;
  }

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {
	  
	  /** Check if the PageId is in the buffer pool. */
	  FrameDesc fdesc = pagemap.get(pageno.pid);
	  if(fdesc != null) {
		  
		  /** Check if the pincount of the asked page is non-zero. If yes, raise an exception. Else, deallocate the page from disk. */
		  if(fdesc.pincnt != 0) {
			  throw new IllegalArgumentException("Trying to free pinned page.");
		  }
		  else {
			  pagemap.remove(pageno.pid);
			  fdesc.pincnt = 0;
			  fdesc.dirty = false;
			  fdesc.state = Clock.AVAILABLE;
			  fdesc.pageno.pid = INVALID_PAGEID;
		  }
		  Minibase.DiskManager.deallocate_page(pageno);
	  }
  }

  /**
   * Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
   * 
   * @param pageno identifies the page to pin
   * @param page holds contents of the page, either an input or output param
   * @param skipRead PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public void pinPage(PageId pageno, Page page, boolean skipRead) {
	  if(pageno.pid == -1){
		  return;
	  }
	  
	  /** Check if the page is already in the buffer pool and is pinned. If yes, increment the pincount. */
	  if(pagemap.containsKey(pageno.pid)) {
		  FrameDesc fdesc = pagemap.get(pageno.pid); 
		  
		  /** If PIN_MEMCPY is set, raise an exception as some other process is using this page. */
		  if(skipRead == GlobalConst.PIN_MEMCPY) {
			  throw new IllegalArgumentException("Trying to pin a page that is already pinned.");
		  }
		  else {
			  fdesc.pincnt++;
			  replacer.pinPage(fdesc);
			  page.setPage(bufpool[fdesc.index]);
			  return;
		  }
	  }

	  /** If the page is not in the buffer pool, find a replacement candidate from the buffer pool. 
	   * If the dirty bit is set for the replacement candidate, flush it to the disk and clear the dirty bit.
	   * Remove the replacement candidate page from the buffer.
	   * If PIN_MEMCPY is set, copy page from memory to buffer. If PIN_DISKIO is set, read page from disk to buffer. */
	  else {
		  int i = replacer.pickVictim();
		  if(i<0) {
			  throw new IllegalStateException("Buffer pool exceeded.");
		  }
		  
		  FrameDesc fdesc = frametab[i];
		  
		  if(fdesc.pageno.pid != INVALID_PAGEID) {
			  pagemap.remove(fdesc.pageno.pid);
			  if(fdesc.dirty) {
				  flushPage(fdesc.pageno);
			  }
		  }
		  
		  /** If PIN_MEMCPY is set, copy the page from memory. Otherwise, read it form the disk. */
		  if(skipRead == GlobalConst.PIN_MEMCPY) {
			  bufpool[i].copyPage(page);
		  }
		  else {
			  Minibase.DiskManager.read_page(pageno, bufpool[i]);
		  }
		  page.setPage(bufpool[i]);
		  
		  fdesc.pageno.pid = pageno.pid;
		  fdesc.pincnt = 1;
		  fdesc.state = Clock.PINNED;
		  fdesc.dirty = false;
		  pagemap.put(pageno.pid, fdesc);
		  replacer.pinPage(fdesc);
	  }
  }

  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherrwise
   * @throws IllegalArgumentException if the page is not present or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {
  	  if(pageno.pid == -1){
		  return;
	  }

  	  /** Check if the page to unpin is in the buffer pool. If not, raise an exception. */
	  if(pagemap.containsKey(pageno.pid)) {

		  /** Check if the page to unpin has pincount more than 0. If not, raise an exception. If yes, decrement the pincount. */
		  FrameDesc fdesc = pagemap.get(pageno.pid);
		  if (fdesc.pincnt > 0) {
			  fdesc.pincnt--;
			  fdesc.dirty = fdesc.dirty || dirty;
			  replacer.unpinPage(fdesc);
			  return;
		  }
		  else {
			  throw new IllegalArgumentException("Trying to unpin page that is not pinned.");
		  }
	  }
	  else {
		  throw new IllegalArgumentException("Trying to unpin page that is not in the buffer.");
	  }
  }

  /**
   * Immediately writes a page in the buffer pool to disk, if dirty.
   */
  public void flushPage(PageId pageno) {
	  
	  /** If the dirty bit of the page is set, write the containts of the page to disk. Clear the dirty bit. */
	  for(int i=0; i<frametab.length; i++) {
		  if(frametab[i].pageno.pid == pageno.pid) {
			  if(frametab[i].dirty) {
				  Minibase.DiskManager.write_page(pageno, bufpool[frametab[i].index]);
				  frametab[i].dirty = false;
			  }
		  }
	  }
  }

  /**
   * Immediately writes all dirty pages in the buffer pool to disk.
   */
  public void flushAllPages() {
	  
	  /** Flush all the pages in the buffer pool. */
	  for(int i=0; i<frametab.length; i++) {
		  flushPage(frametab[i].pageno);
	  }
  }

  /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() {
	  
	  /** Return the length of the buffer pool. */
	  return bufpool.length;
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
	  
	  /** Return the number of unpinned pages in the buffer pool. */
	  int numUnpinned = 0;
	  for(int i=0; i<frametab.length; i++) {
		  if(frametab[i].pincnt == 0) {
			  numUnpinned++;
		  }
	  }
	  return numUnpinned;
  }

} // public class BufMgr implements GlobalConst
