type dbWrite struct {
	rwtx     *Tx
	freelist     *freelist
	freelistLoad sync.Once
	pagePool sync.Pool
	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.
	metalock sync.Mutex   // Protects meta page access.
}

// init creates a new database file and initializes its meta pages.
func (db *DB) init() error {
	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// Initialize the meta page.
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// Write an empty freelist at page 3.
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// Write the buffer to our data file.
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}



// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(txid txid, count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	if p.id = db.freelist.allocate(txid, count); p.id != 0 {
		return p, nil
	}

	// Resize mmap() if we're at the end.
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) freepages() []pgid {
	tx, err := db.beginTx()
	defer func() {
		err = tx.Rollback()
		if err != nil {
			panic("freepages: failed to rollback tx")
		}
	}()
	if err != nil {
		panic("freepages: failed to open read only tx")
	}

	reachable := make(map[pgid]*page)
	nofreed := make(map[pgid]bool)
	ech := make(chan error)
	go func() {
		for e := range ech {
			panic(fmt.Sprintf("freepages: failed to get all reachable pages (%v)", e))
		}
	}()
	tx.checkBucket(&tx.root, reachable, nofreed, ech)
	close(ech)

	var fids []pgid
	for i := pgid(2); i < db.meta().pgid; i++ {
		if _, ok := reachable[i]; !ok {
			fids = append(fids, i)
		}
	}
	return fids
}

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (db *DB) Sync() error { return fdatasync(db) }

// freePages releases any pages associated with closed read-only transactions.
func (db *DB) freePages() {
	// Free all pending pages prior to earliest open transaction.
	sort.Sort(txsById(db.txs))
	minid := txid(0xFFFFFFFFFFFFFFFF)
	if len(db.txs) > 0 {
		minid = db.txs[0].meta.txid
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}
	// Release unused txid extents.
	for _, t := range db.txs {
		db.freelist.releaseRange(minid, t.meta.txid-1)
		minid = t.meta.txid + 1
	}
	db.freelist.releaseRange(minid, txid(0xFFFFFFFFFFFFFFFF))
	// Any page both allocated and freed in an extent is safe to release.
}

func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	db.rwlock.Lock()

	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// Create a transaction associated with the database.
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t
	db.freePages()
	return t, nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
func (db *DB) mmapSize(size int) (int, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// loadFreelist reads the freelist if it is synced, or reconstructs it
// by scanning the DB if it is not synced. It assumes there are no
// concurrent accesses being made to the freelist.
func (db *DB) loadFreelist() {
	db.freelistLoad.Do(func() {
		db.freelist = newFreelist()
		if !db.hasSyncedFreelist() {
			// Reconstruct free list by scanning the DB.
			db.freelist.readIDs(db.freepages())
		} else {
			// Read free list from freelist page.
			db.freelist.read(db.page(db.meta().freelist))
		}
		db.stats.FreePageN = len(db.freelist.ids)
	})
}

func (db *DB) hasSyncedFreelist() bool {
	return db.meta().freelist != pgidNoFreelist
}

close() {
	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()


	db.freelist = nil

	// Clear ops.
	db.ops.writeAt = nil
}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func mmap() {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// Ensure the size is at least the minimum size.
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// Dereference all mmap references before unmapping.
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	// Save references to the meta pages.
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}


func open() {
	// Default values for test hooks
	db.ops.writeAt = db.file.WriteAt


	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	db.loadFreelist()

	// Flush freelist when transitioning from no sync to sync so
	// NoFreelistSync unaware boltdb can open the db later.
	if !db.NoFreelistSync && !db.hasSyncedFreelist() {
		tx, err := db.Begin(true)
		if tx != nil {
			err = tx.Commit()
		}
		if err != nil {
			_ = db.close()
			return nil, err
		}
	}

	// Mark the database as opened and return.
	return db, nil

}
