package bufmgr;

import global.PageId;

/**
 * A frame descriptor; contains info about each page in the buffer pool.
 */
class FrameDesc {

  /** Index in the buffer pool. */
  public int index;
  
  /** Identifies the frame's page. */
  public PageId pageno;

  /** The frame's pin count. */
  public int pincnt;

  /** The frame's dirty status. */
  public boolean dirty;

  /** Generic state used by replacers. */
  public int state;

  // --------------------------------------------------------------------------

  /**
   * Default constructor; empty frame.
   */
  public FrameDesc(int index) {
    this.index = index;
    pageno = new PageId();
    pincnt = 0;
    dirty = false;
    state = 0;
  }

} // class FrameDesc
