package ondisk

import (
	"fmt"
	"hash/fnv"
)

type Meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *Meta) Validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// copy copies one meta object to another.
func (m *Meta) Copy(dest *Meta) {
	*dest = *m
}

// write writes the meta onto a page.
func (m *Meta) Write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid && m.freelist != pgidNoFreelist {
		// TODO: reject pgidNoFreeList if !NoFreelistSync
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txid % 2)
	p.flags |= MetaPageFlag

	// Calculate the checksum.
	m.checksum = m.sum64()

	m.copy(p.Meta())
}

// generates the checksum for the meta.
func (m *Meta) Sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(Meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

func LatestMeta(metaA *Meta, metaB *Meta) *Meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	if metaB.txid > metaA.txid {
		metaA, metaB = metaB, metaA
	}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}
