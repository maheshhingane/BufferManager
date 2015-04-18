package bufmgr;

/**
 * The "Clock" replacement policy.
 */
class Clock extends Replacer {

  //
  // Frame State Constants
  //
  protected static final int AVAILABLE = 10;
  protected static final int REFERENCED = 11;
  protected static final int PINNED = 12;

  /** Clock head; required for the default clock algorithm. */
  protected int head;

  // --------------------------------------------------------------------------

  /**
   * Constructs a clock replacer.
   */
  public Clock(BufMgr bufmgr) {
    super(bufmgr);

    // initialize the frame states
    for (int i = 0; i < frametab.length; i++) {
      frametab[i].state = AVAILABLE;
    }

    // initialize the clock head
    head = -1;

  } // public Clock(BufMgr bufmgr)

  /**
   * Notifies the replacer of a new page.
   */
  public void newPage(FrameDesc fdesc) {
    // no need to update frame state
  }

  /**
   * Notifies the replacer of a free page.
   */
  public void freePage(FrameDesc fdesc) {
    fdesc.state = AVAILABLE;
  }

  /**
   * Notifies the replacer of a pined page.
   */
  public void pinPage(FrameDesc fdesc) {
	  fdesc.state = PINNED;
  }

  /**
   * Notifies the replacer of an unpinned page.
   */
  public void unpinPage(FrameDesc fdesc) {
	  /** If, after decrement, the pincount becomes 0, mark this page as a replacement candidate. */
	  if (fdesc.pincnt == 0) {
		  fdesc.state = Clock.REFERENCED;
	  }
  }

  /**
   * Selects the best frame to use for pinning a new page.
   * 
   * @return victim frame number, or -1 if none available
   */
  public int pickVictim() {
	  /** Integer counter to check through the frametab twice. */
	  int i = 0;
	  
	  /** A do-while loop searching for an AVAILABLE frame. If not found any, returns -1. */
	  do {
		  head = (head+1) % frametab.length;
		   i++;
		   if(i >= 2 * frametab.length)
			   return -1;
		   if(frametab[head].state == REFERENCED)
			   frametab[head].state = AVAILABLE;
	   }while(frametab[head].state != AVAILABLE );
	   return head;
  } // public int pick_victim()

} // class Clock extends Replacer
