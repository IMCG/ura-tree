// foster btree version g
// 02 JAN 2014

// author: karl malbrain, malbrain@cal.berkeley.edu
// edits: nicolas arciniega, naarcini@uwaterloo.ca

/*
This work, including the source code, documentation
and related data, is placed into the public domain.

The orginal author is Karl Malbrain.

THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY
OF ANY KIND, NOT EVEN THE IMPLIED WARRANTY OF
MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE,
ASSUMES _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE
RESULTING FROM THE USE, MODIFICATION, OR
REDISTRIBUTION OF THIS SOFTWARE.
*/

// Please see the project home page for documentation
// code.google.com/p/high-concurrency-btree

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE
#define VERIFY

// Constants
#define BITS 9//12//14
#define POOLSIZE 32768u // Max 65535
#define SEGSIZE 4
#define NUMMODE 0
#define BUFFERSIZE 8589934592u//4294967296u//2147483648 //1073741824
#define MAXTHREADS 32

#define _GNU_SOURCE

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <errno.h>
#include <pthread.h>

#include <memory.h>
#include <string.h>
#include "utils.h"

typedef unsigned long long	uid;

#define BT_ro 0x6f72	// ro
#define BT_rw 0x7772	// rw

#define BT_maxbits		24					// maximum page size in bits
#define BT_minbits		9					// minimum page size in bits
#define BT_minpage		(1 << BT_minbits)	// minimum page size
#define BT_maxpage		(1 << BT_maxbits)	// maximum page size

/*
There are five lock types for each node in three independent sets:
1. (set 1) AccessIntent: Sharable. Going to Read the node. Incompatible with NodeDelete.
2. (set 1) NodeDelete: Exclusive. About to release the node. Incompatible with AccessIntent.
3. (set 2) ReadLock: Sharable. Read the node. Incompatible with WriteLock.
4. (set 2) WriteLock: Exclusive. Modify the node. Incompatible with ReadLock and other WriteLocks.
5. (set 3) ParentLock: Exclusive. Have parent adopt/delete maximum foster child from the node.
*/

typedef enum{
	BtLockAccess,
	BtLockDelete,
	BtLockRead,
	BtLockWrite,
	BtLockParent
}BtLock;

//	Define the length of the page and key pointers

#define BtId 6

//	Page key slot definition.

//	If BT_maxbits is 15 or less, you can save 4 bytes
//	for each key stored by making the first two uints
//	into ushorts.  You can also save 4 bytes by removing
//	the tod field from the key.

//	Keys are marked dead, but remain on the page until
//	it cleanup is called. The fence keys (lowest and highest keys) for
//	the page are always present, even after cleanup.

typedef struct {
	uint off : BT_maxbits;		// page offset for key start
	uint dead : 1;				// set for deleted key
	uint fence : 1;				// set for fence key
	uint tod;					// time-stamp for key
	unsigned char id[BtId];		// id associated with key
} BtSlot;

//	The key structure occupies space at the upper end of
//	each page.  It's a length byte followed by the value
//	bytes.

typedef struct {
	unsigned char len;
	unsigned char key[1];
} *BtKey;

//	mode & definition for spin latch implementation

enum {
	Mutex = 1,
	Write = 2,
	Pending = 4,
	Share = 8
} LockMode;

// mutex locks the other fields
// exclusive is set for write access
// share is count of read accessors

typedef struct {
	volatile ushort mutex : 1;
	volatile ushort exclusive : 1;
	volatile ushort pending : 1;
	volatile ushort share : 13;
} BtSpinLatch;

//	The first part of an index page.
//	It is immediately followed
//	by the BtSlot array of keys.

typedef struct Page {
	BtSpinLatch readwr[1];		// read/write lock
	BtSpinLatch access[1];		// access intent lock
	BtSpinLatch parent[1];		// parent SMO lock
	ushort foster;				// count of foster children
	uint cnt;					// count of keys in page
	uint act;					// count of active keys
	uint min;					// next key offset
	unsigned char bits;			// page size in bits
	unsigned char lvl : 6;		// level of page
	unsigned char kill : 1;		// page is being deleted
	unsigned char dirty : 1;		// page needs to be cleaned
	unsigned char right[BtId];	// page number to right
} *BtPage;

//	The object structure for Btree access

typedef struct {
	uint page_size;				// page size	
	uint page_bits;				// page size in bits	
	uint seg_bits;				// seg size in pages in bits
	uint mode;					// read-write mode
	char *buffer;			    // Buffer pool for tree
	volatile ushort poolcnt;	// highest page pool node in use
	volatile ushort evicted;	// last evicted hash table slot
	ushort poolmax;				// highest page pool node allocated
	ushort poolmask;			// total size of pages in logical memory pool segment - 1

	// Tracking Data
    unsigned long long rootreadwait[MAXTHREADS];
    unsigned long long rootwritewait[MAXTHREADS];
	unsigned long long readlockwait[MAXTHREADS];
	unsigned long long writelockwait[MAXTHREADS];
	unsigned long long readlockaquired[MAXTHREADS];
	unsigned long long writelockaquired[MAXTHREADS];
	unsigned long long readlockfail[MAXTHREADS];
	unsigned long long writelockfail[MAXTHREADS];

	// Test vars, remove this
	int flag;
	int counter;
	unsigned long long lowfenceoverwrite[MAXTHREADS];
	unsigned long long optimisticfail[MAXTHREADS];
	unsigned long long optimisticsuccess[MAXTHREADS];
} BtMgr;

typedef struct {
	BtMgr *mgr;			// buffer manager for thread
	BtPage temp;		// temporary frame buffer (memory mapped/file IO)
	BtPage alloc;		// frame buffer for alloc page ( page 0 )
	BtPage cursor;		// cached frame for start/next (never mapped)
	BtPage frame;		// spare frame for the page split (never mapped)
	BtPage zero;		// page frame for zeroes at end of file
	BtPage page;		// current page
	uid page_no;		// current page number	
	uid cursor_page;	// current cursor page number	
	unsigned char *mem;	// frame, cursor, page memory buffer
	int err;			// last error
	int thread_id;      // thread identifier
} BtDb;

typedef enum {
	BTERR_ok = 0,
	BTERR_struct,
	BTERR_ovflw,
	BTERR_lock,
	BTERR_map,
	BTERR_wrt,
	BTERR_hash,
	BTERR_latch
} BTERR;

// B-Tree functions
extern void bt_close(BtDb *bt);
extern BtDb *bt_open(BtMgr *mgr, int thread_id);
extern BTERR  bt_insertkey(BtDb *bt, unsigned char *key, uint len, uid id, uint tod);
extern BTERR  bt_deletekey(BtDb *bt, unsigned char *key, uint len, uint lvl);
extern uid bt_findkey(BtDb *bt, unsigned char *key, uint len);
extern uint bt_startkey(BtDb *bt, unsigned char *key, uint len);
extern uint bt_nextkey(BtDb *bt, uint slot);

//	manager functions
extern BtMgr *bt_mgr(char *name, uint mode, uint bits, uint poolsize, uint segsize);
void bt_mgrclose(BtMgr *mgr);

//  Helper functions to return cursor slot values

extern BtKey bt_key(BtDb *bt, uint slot);
extern uid bt_uid(BtDb *bt, uint slot);
extern uint bt_tod(BtDb *bt, uint slot);

//  BTree page number constants
#define ALLOC_page		0	// allocation of new pages
#define ROOT_page		1	// root of the btree
#define LEAF_page		2	// first page of leaves

//	Number of levels to create in a new BTree

#define MIN_lvl			2

//  The page is allocated from low and hi ends.
//  The key offsets and row-id's are allocated
//  from the bottom, while the text of the key
//  is allocated from the top.  When the two
//  areas meet, the page is split into two.

// CURRENT PAGE STRUCTURE EXAMPLE
//
// Ordered:
//-----------------------------------------------------------------------------------------------------------------------------------------------------------
//| BtPage Struct | Slot A | Slot B | Slot C | Fence Slot | Foster A | Foster B |             | FosterKey B | FosterKey A | FenceKey | Key C | Key B | Key A |
//| foster=2      | dead=0 | dead=1 | dead=0 |            |          |          |.............|             |             |          |       |       |       |
//| cnt=6, act=5  |        |        |        |            |          |          |             |             |             |          |       |       |       |
//------------------------------------------------------------------------------------------------------------------------------------------------------------
// 0                                                                                         min                                                         page_size
//
// When adding keys, keys are allocated backwards from min and slots are allocated in order without overwriting fence slot or foster slots
// Offset member in slot points to appropriate key
// 
// DESIRED STRUCTURE:
//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
//| BtPage Struct | Low Fence | Slot A | Slot B | Slot C | High Fence | Foster A | Foster B |             | FosterKey B | FosterKey A | HighFenceKey | Key C | Key B | Key A | LowFenceKey |
//| foster=2      |	Slot      | dead=0 | dead=1 | dead=0 | Slot       |          |          |.............|             |             |              |       |       |       |             |
//| cnt=7, act=6  |           |        |        |        |            |          |          |             |             |             |              |       |       |       |             |
//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// 0                                                                                                      min                                                                         page_size
//

//  A key consists of a length byte, two bytes of
//  index number (0 - 65534), and up to 253 bytes
//  of key value.  Duplicate keys are discarded.
//  Associated with each key is a 48 bit row-id.

//  The b-tree root is always located at page 1.
//	The first leaf page of level zero is always
//	located on page 2.

//	When to root page fills, it is split in two and
//	the tree height is raised by a new root at page
//	one with two keys.

//	Deleted keys are marked with a dead bit until
//	page cleanup The fence key for a node is always
//	present, even after deletion and cleanup.

//  To achieve maximum concurrency one page is locked at a time
//  as the tree is traversed to find leaf key in question.

//	An adoption traversal leaves the parent node locked as the
//	tree is traversed to the level in quesiton.

//  Page 0 is dedicated to lock for new page extensions,
//	and chains empty pages together for reuse.

//	Empty pages are chained together through the ALLOC page and reused.

//	Access macros to address slot and key values from the page

#define slotptr(page, slot) (((BtSlot *)(page+1)) + (slot-1))
#define keyptr(page, slot) ((BtKey)((unsigned char*)(page) + slotptr(page, slot)->off))

void bt_putid(unsigned char *dest, uid id)
{
	int i = BtId;

	while (i--)
		dest[i] = (unsigned char)id, id >>= 8;
}

uid bt_getid(unsigned char *src)
{
	uid id = 0;
	int i;

	for (i = 0; i < BtId; i++)
		id <<= 8, id |= *src++;

	return id;
}

//	wait until write lock mode is clear
//	and add 1 to the share count

void bt_spinreadlock(BtSpinLatch *latch, BtDb *bt)
{
	ushort prev;

	do {
		while (__sync_fetch_and_or((ushort *)latch, Mutex) & Mutex) {
			bt->mgr->readlockfail[bt->thread_id] += 1;
			sched_yield();
		}
		//  see if exclusive request is granted or pending

		if (prev = !(latch->exclusive | latch->pending))
			__sync_fetch_and_add((ushort *)latch, Share);

		__sync_fetch_and_and((ushort *)latch, ~Mutex);

		if (prev)
			return;

		bt->mgr->readlockfail[bt->thread_id] += 1;

	} while (sched_yield(), 1);

}

//	wait for other read and write latches to relinquish

void bt_spinwritelock(BtSpinLatch *latch, BtDb *bt)
{
	ushort prev;

	do {
		while (__sync_fetch_and_or((ushort *)latch, Mutex | Pending) & Mutex) {
			bt->mgr->writelockfail[bt->thread_id] += 1;
			sched_yield();
		}

		if (prev = !(latch->share | latch->exclusive))
			__sync_fetch_and_or((ushort *)latch, Write);

		__sync_fetch_and_and((ushort *)latch, ~(Mutex | Pending));

		if (prev)
			return;

		bt->mgr->writelockfail[bt->thread_id] += 1;
		sched_yield();

	} while (1);
}

//	try to obtain write lock

//	return 1 if obtained,
//		0 otherwise

int bt_spinwritetry(BtSpinLatch *latch, BtDb *bt)
{
	ushort prev;

	if (prev = __sync_fetch_and_or((ushort *)latch, Mutex), prev & Mutex) {
		bt->mgr->writelockfail[bt->thread_id] += 1;
		return 0;
	}

	//	take write access if all bits are clear

	if (!prev)
		__sync_fetch_and_or((ushort *)latch, Write);

	return !prev;
}

//	clear write mode

void bt_spinreleasewrite(BtSpinLatch *latch)
{
	__sync_fetch_and_and((ushort *)latch, ~Write);
}

//	decrement reader count

void bt_spinreleaseread(BtSpinLatch *latch)
{
	__sync_fetch_and_add((ushort *)latch, -Share);
}

void bt_mgrclose(BtMgr *mgr)
{
	uint slot;

	free(mgr->buffer);
	free(mgr);
}

//	close and release memory

void bt_close(BtDb *bt)
{
	if (bt->mem)
		free(bt->mem);

	free(bt);
}

//  open/create new btree buffer manager

//	call with file_name, BT_openmode, bits in page size (e.g. 16),
//		size of mapped page pool (e.g. 8192)

BtMgr *bt_mgr(char *name, uint mode, uint bits, uint poolmax, uint segsize)
{
	uint lvl, cacheblk, last;
	BtPage alloc;
	int offset, i;
	BtMgr* mgr;
	BtKey key;
	char* buffer;

	// determine sanity of page size and buffer pool

	if (bits > BT_maxbits)
		bits = BT_maxbits;
	else if (bits < BT_minbits)
		bits = BT_minbits;

	if (!poolmax)
		return NULL;	// must have buffer pool

	mgr = (BtMgr *)malloc(sizeof(BtMgr));

	buffer = (char*)calloc(BUFFERSIZE, sizeof(char));
	mgr->buffer = buffer;

	alloc = malloc(BT_maxpage);

	mgr->page_size = 1 << bits;
	mgr->page_bits = bits;

	mgr->poolmax = poolmax;
	mgr->mode = mode;

	for (i = 0; i < MAXTHREADS; i++) {
        mgr->rootreadwait[i] = 0;
        mgr->rootwritewait[i] = 0;
		mgr->readlockwait[i] = 0;
		mgr->writelockwait[i] = 0;
		mgr->readlockaquired[i] = 0;
		mgr->writelockaquired[i] = 0;
		mgr->readlockfail[i] = 0;
		mgr->writelockfail[i] = 0;
		mgr->lowfenceoverwrite[i] = 0;
		mgr->optimisticfail[i] = 0;
		mgr->optimisticsuccess[i] = 0;
	}

	//  mask for logical memory pools
	cacheblk = 4096;
	if (cacheblk < mgr->page_size)
		cacheblk = mgr->page_size;

	mgr->poolmask = (cacheblk >> bits) - 1;

	//	see if requested size of pages per pool is greater

	if ((1 << segsize) > mgr->poolmask)
		mgr->poolmask = (1 << segsize) - 1;

	mgr->seg_bits = 0;

	while ((1 << mgr->seg_bits) <= mgr->poolmask)
		mgr->seg_bits++;

	// initializes an empty b-tree with root page and page of leaves

	memset(alloc, 0, 1 << bits);
	bt_putid(alloc->right, MIN_lvl + 1);
	alloc->bits = mgr->page_bits;

	offset = 0;
	memcpy(mgr->buffer, alloc, mgr->page_size);
	offset += mgr->page_size;

	memset(alloc, 0, 1 << bits);
	alloc->bits = mgr->page_bits;

	for (lvl = MIN_lvl; lvl--;) {
		slotptr(alloc, 1)->off = mgr->page_size - 3;
		slotptr(alloc, 1)->fence = 1;
        slotptr(alloc, 1)->dead = 1;
		bt_putid(slotptr(alloc, 1)->id, 0);		// No left child
		key = keyptr(alloc, 1);
		key->len = 2;			// create starter key
		key->key[0] = 0x00;
		key->key[1] = 0x00;

		slotptr(alloc, 2)->off = mgr->page_size - 6;
		slotptr(alloc, 2)->fence = 1;
		bt_putid(slotptr(alloc, 2)->id, lvl ? MIN_lvl - lvl + 1 : 0);		// next(lower) page number
		key = keyptr(alloc, 2);
		key->len = 2;			// create stopper key
		key->key[0] = 0xff;
		key->key[1] = 0xff;

		alloc->min = mgr->page_size - 6;
		alloc->lvl = lvl;
		alloc->cnt = 2;
		alloc->act = 2;

		memcpy((mgr->buffer + offset), alloc, mgr->page_size);
		offset += mgr->page_size;
	}

	// create empty page area by writing last page of first
	// segment area (other pages are zeroed by O/S)
	if (mgr->poolmask) {
		memset(alloc, 0, mgr->page_size);
		last = mgr->poolmask;

		while (last < MIN_lvl + 1)
			last += mgr->poolmask + 1;

		memcpy(mgr->buffer + (last << mgr->page_bits), alloc, mgr->page_size);
	}

	free(alloc);

	return mgr;
}

//	open BTree access method
//	based on buffer manager

BtDb *bt_open(BtMgr *mgr, int thread_id)
{
	BtDb *bt = malloc(sizeof(*bt));

	memset(bt, 0, sizeof(*bt));
	bt->mgr = mgr;
	bt->thread_id = thread_id;
	bt->mem = malloc(3 * mgr->page_size);
	bt->frame = (BtPage)bt->mem;
	bt->zero = (BtPage)(bt->mem + 1 * mgr->page_size);
	bt->cursor = (BtPage)(bt->mem + 2 * mgr->page_size);
	return bt;
}

//  compare two keys, returning > 0, = 0, or < 0
//  as the comparison value

int keycmp(BtKey key1, unsigned char *key2, uint len2)
{
	uint len1 = key1->len;
	int ans;

	if (ans = memcmp(key1->key, key2, len1 > len2 ? len2 : len1))
		return ans;

	if (len1 > len2)
		return 1;
	if (len1 < len2)
		return -1;

	return 0;
}

//	Buffer Pool mgr

//	find or place requested page in segment-pool
//	return pool table entry

char *bt_findpool(BtDb *bt, uid page_no)
{
	char *pool_ptr;
	off64_t off = (page_no & ~bt->mgr->poolmask) << bt->mgr->page_bits;

	if (off > (BUFFERSIZE - 1))
		return NULL;

	pool_ptr = bt->mgr->buffer + off;

	return pool_ptr;
}

// Save a page to a pointer

BTERR bt_savepage(BtDb *bt, uid page_no, BtPage *pageptr) {
	char *pool_ptr;
	uint subpage;
	BtPage page;

	//	find pool start
	if (pool_ptr = bt_findpool(bt, page_no))
		subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
	else {
		return bt->err;
	}

	page = (BtPage)(pool_ptr + (subpage << bt->mgr->page_bits));

	if (pageptr)
		*pageptr = page;

	return bt->err = 0;
}

// place write, read, or parent lock on requested page_no.
//	pin to buffer pool and return page pointer

BTERR bt_lockpage(BtDb *bt, uid page_no, BtLock mode, BtPage *pageptr)
{
	char *pool_ptr;
	uint subpage;
	BtPage page;
	
	unsigned long long startTime, endTime;
	
	startTime = preciseTime();

	//	find pool start
	if (pool_ptr = bt_findpool(bt, page_no))
		subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
	else {
		endTime = preciseTime();
		if (mode == BtLockRead || mode == BtLockAccess) {
			bt->mgr->readlockwait[bt->thread_id] += (endTime - startTime);
			bt->mgr->readlockfail[bt->thread_id] = bt->mgr->readlockfail[bt->thread_id] + 1;
            if(page_no == ROOT_page) {
                bt->mgr->rootreadwait[bt->thread_id] += (endTime - startTime);
            }
		}
		else if (mode == BtLockWrite || mode == BtLockDelete || mode == BtLockParent) {
			bt->mgr->writelockwait[bt->thread_id] += (endTime - startTime);
			bt->mgr->writelockfail[bt->thread_id] = bt->mgr->writelockfail[bt->thread_id] + 1;
            if(page_no == ROOT_page) {
                bt->mgr->rootwritewait[bt->thread_id] += (endTime - startTime);
            }
		}
		return bt->err;
	}

	page = (BtPage)(pool_ptr + (subpage << bt->mgr->page_bits));

	switch (mode) {
	case BtLockRead:
		bt_spinreadlock(page->readwr, bt);
		break;
	case BtLockWrite:
		bt_spinwritelock(page->readwr, bt);
		break;
	case BtLockAccess:
		bt_spinreadlock(page->access, bt);
		break;
	case BtLockDelete:
		bt_spinwritelock(page->access, bt);
		break;
	case BtLockParent:
		bt_spinwritelock(page->parent, bt);
		break;
	default:
		return bt->err = BTERR_lock;
	}

	if (pageptr)
		*pageptr = page;

	endTime = preciseTime();
	if (mode == BtLockRead || mode == BtLockAccess) {
		bt->mgr->readlockwait[bt->thread_id] += (endTime - startTime);
		bt->mgr->readlockaquired[bt->thread_id] = bt->mgr->readlockaquired[bt->thread_id] + 1;
        if(page_no == ROOT_page) {
            bt->mgr->rootreadwait[bt->thread_id] += (endTime - startTime);
        }
	}
	else if (mode == BtLockWrite || mode == BtLockDelete || mode == BtLockParent) {
		bt->mgr->writelockwait[bt->thread_id] += (endTime - startTime);
		bt->mgr->writelockaquired[bt->thread_id] = bt->mgr->writelockaquired[bt->thread_id] + 1;
        if(page_no == ROOT_page) {
            bt->mgr->rootwritewait[bt->thread_id] += (endTime - startTime);
        }

	}
	return bt->err = 0;
}

// remove write, read, or parent lock on requested page_no.

BTERR bt_unlockpage(BtDb *bt, uid page_no, BtLock mode)
{
	char *pool_ptr;
	uint subpage;
	BtPage page;
	uint idx;

	if (!(pool_ptr = bt_findpool(bt, page_no)))
		return bt->err = BTERR_hash;

	subpage = (uint)(page_no & bt->mgr->poolmask); // page within mapping
	page = (BtPage)(pool_ptr + (subpage << bt->mgr->page_bits));

	switch (mode) {
	case BtLockRead:
		bt_spinreleaseread(page->readwr);
		break;
	case BtLockWrite:
		bt_spinreleasewrite(page->readwr);
		break;
	case BtLockAccess:
		bt_spinreleaseread(page->access);
		break;
	case BtLockDelete:
		bt_spinreleasewrite(page->access);
		break;
	case BtLockParent:
		bt_spinreleasewrite(page->parent);
		break;
	default:
		return bt->err = BTERR_lock;
	}

	return bt->err = 0;
}

//	deallocate a deleted page
//	place on free chain out of allocator page
//  fence key must already be removed from parent

BTERR bt_freepage(BtDb *bt, uid page_no)
{
	//  obtain delete lock on deleted page

	if (bt_lockpage(bt, page_no, BtLockDelete, NULL))
		return bt->err;

	//  obtain write lock on deleted page

	if (bt_lockpage(bt, page_no, BtLockWrite, &bt->temp))
		return bt->err;

	//	lock allocation page

	if (bt_lockpage(bt, ALLOC_page, BtLockWrite, &bt->alloc))
		return bt->err;

	//	store free chain in allocation page second right
	bt_putid(bt->temp->right, bt_getid(bt->alloc[1].right));
	bt_putid(bt->alloc[1].right, page_no);

	// unlock allocation page

	if (bt_unlockpage(bt, ALLOC_page, BtLockWrite))
		return bt->err;

	//  remove write lock on deleted node

	if (bt_unlockpage(bt, page_no, BtLockWrite))
		return bt->err;

	//  remove delete lock on deleted node

	if (bt_unlockpage(bt, page_no, BtLockDelete))
		return bt->err;

	return 0;
}

//	allocate a new page and write page into it

uid bt_newpage(BtDb *bt, BtPage page)
{
	uid new_page;
	BtPage pmap;
	int subpage;
	int reuse;

	// lock page zero

	if (bt_lockpage(bt, ALLOC_page, BtLockWrite, &bt->alloc))
		return 0;

	// use empty chain first
	// else allocate empty page

	if (new_page = bt_getid(bt->alloc[1].right)) {
		if (bt_lockpage(bt, new_page, BtLockWrite, &bt->temp))
			return 0;
		bt_putid(bt->alloc[1].right, bt_getid(bt->temp->right));
		if (bt_unlockpage(bt, new_page, BtLockWrite))
			return 0;
		reuse = 1;
	}
	else {
		new_page = bt_getid(bt->alloc->right);
		bt_putid(bt->alloc->right, new_page + 1);
		reuse = 0;
	}

	memset(bt->zero, 0, 3 * sizeof(BtSpinLatch)); // clear locks
	memcpy((char *)bt->zero + 3 * sizeof(BtSpinLatch), (char *)page + 3 * sizeof(BtSpinLatch), bt->mgr->page_size - 3 * sizeof(BtSpinLatch));

	if ((new_page << bt->mgr->page_bits) + bt->mgr->page_size > (BUFFERSIZE-1))
		return bt->err = BTERR_wrt, 0;

	memcpy(bt->mgr->buffer + (new_page << bt->mgr->page_bits), bt->zero, bt->mgr->page_size);

	// if writing first page of pool block, zero last page in the block

	if (!reuse && bt->mgr->poolmask > 0 && (new_page & bt->mgr->poolmask) == 0)
	{
		// use zero buffer to write zeros
		memset(bt->zero, 0, bt->mgr->page_size);

		if (((new_page | bt->mgr->poolmask) << bt->mgr->page_bits) + bt->mgr->page_size > (BUFFERSIZE-1))
			return bt->err = BTERR_wrt, 0;

		memcpy(bt->mgr->buffer + ((new_page | bt->mgr->poolmask) << bt->mgr->page_bits), bt->zero, bt->mgr->page_size);
	}

	// unlock allocation latch and return new page no

	if (bt_unlockpage(bt, ALLOC_page, BtLockWrite))
		return 0;

	return new_page;
}

//  find slot in page for given key at a given level

int bt_findslot(BtDb *bt, unsigned char *key, uint len)
{
	uint diff, higher = bt->page->cnt, low = 1, slot;

	//	low is the lowest candidate, higher is already
	//	tested as .ge. the given key, loop ends when they meet

	while (diff = higher - low) {
		slot = low + (diff >> 1);
		if (keycmp(keyptr(bt->page, slot), key, len) < 0)
			low = slot + 1;
		else
			higher = slot;
	}

	return higher;
}

//  find and load page at given level for given key
//	leave page rd or wr locked as requested

int bt_loadpage(BtDb *bt, unsigned char *key, uint len, uint lvl, uint lock)
{
	uid page_no = ROOT_page, prevpage = 0;
	uint drill = 0xff, slot;
	uint mode, prevmode;

	bt->mgr->optimisticfail[bt->thread_id]++;

	//  start at root of btree and drill down

	do {
		// determine lock mode of drill level
		mode = (lock == BtLockWrite) && (drill == lvl) ? BtLockWrite : BtLockRead;

		bt->page_no = page_no;

		// obtain access lock using lock chaining with Access mode

		if (page_no > ROOT_page)
		if (bt_lockpage(bt, page_no, BtLockAccess, NULL))
			return 0;

		//  now unlock our (possibly foster) parent

		if (prevpage)
		if (bt_unlockpage(bt, prevpage, prevmode))
			return 0;
		else
			prevpage = 0;

		// obtain read lock using lock chaining
		// and pin page contents

		if (bt_lockpage(bt, page_no, mode, &bt->page))
			return 0;

		if (page_no > ROOT_page)
		if (bt_unlockpage(bt, page_no, BtLockAccess))
			return 0;

		// re-read and re-lock root after determining actual level of root

		if (bt->page_no == ROOT_page)
		if (bt->page->lvl != drill) {
			drill = bt->page->lvl;

			if (lock == BtLockWrite && drill == lvl)
			if (bt_unlockpage(bt, page_no, mode))
				return 0;
			else
				continue;
		}

		prevpage = bt->page_no;
		prevmode = mode;

		//	if page is being deleted,
		//	move back to preceeding page

		if (bt->page->kill) {
			page_no = bt_getid(bt->page->right);
			continue;
		}

		//  find key on page at this level
		//  and descend to requested level

		slot = bt_findslot(bt, key, len);

		//	is this slot a foster child?

		if (slot <= bt->page->cnt - bt->page->foster)
		if (drill == lvl)
			return slot;

		while (slotptr(bt->page, slot)->dead)
		if (slot++ < bt->page->cnt)
			continue;
		else
			goto slideright;

		if (slot <= bt->page->cnt - bt->page->foster)
			drill--;

		//  continue down / right using overlapping locks
		//  to protect pages being killed or split.

		page_no = bt_getid(slotptr(bt->page, slot)->id);
		continue;

	slideright:
		page_no = bt_getid(bt->page->right);

	} while (page_no);

	// return error on end of chain

	bt->err = BTERR_struct;
	return 0;	// return error
}

// Optimistic version of loadpage
// falls back on original method if it fails

int bt_optimisticloadpage(BtDb *bt, unsigned char *key, uint len, uint lvl, uint lock)
{
	uid page_no = ROOT_page, prevpage = 0;
	uint drill = 0xff, slot;
	uint mode, prevmode;

	//  start at root of btree and drill down

	do {
		// determine lock mode of drill level
		mode = (lock == BtLockWrite) && (drill == lvl) ? BtLockWrite : BtLockRead;

		bt->page_no = page_no;

		// Immediately let go of previous lock, if any
		if (prevpage)
		if (bt_unlockpage(bt, prevpage, prevmode)) {
			fprintf(stdout, "Error is in previous unlock\n");
            return 0;
        }
		else
			prevpage = 0;

		// Lock page
		if (bt_lockpage(bt, page_no, mode, &bt->page)) {
			fprintf(stdout, "Error is in initial lock page\n");
            return 0;
        }

		// re-read and re-lock root after determining actual level of root

		if (bt->page_no == ROOT_page)
		if (bt->page->lvl != drill) {
			drill = bt->page->lvl;

			if (lock == BtLockWrite && drill == lvl)
			if (bt_unlockpage(bt, page_no, mode)) {
				fprintf(stdout, "Error is in drill re-lock\n");
                return 0;
            }
			else
				continue;
		}

		prevpage = bt->page_no;
		prevmode = mode;

		//	if page is being deleted,
		//	move back to preceeding page

		if (bt->page->kill) {
			page_no = bt_getid(bt->page->right);
			continue;
		}

		//  find key on page at this level
		//  and descend to requested level

		slot = bt_findslot(bt, key, len);

		//	is this slot a foster child?

		if (slot <= bt->page->cnt - bt->page->foster)
		if (drill == lvl) {
			// We think this is right, so double check
			// Check fence keys to see if it is possible to be here
			if ((keycmp(keyptr(bt->page, 1), key, len) > 0) || (keycmp(keyptr(bt->page, (bt->page->cnt - bt->page->foster)), key, len) < 0))
			if (bt_unlockpage(bt, page_no, mode)) {
				fprintf(stdout, "Error is in keycomparison\n");
                return 0;
            }
			else
				return bt_loadpage(bt, key, len, lvl, lock);

			bt->mgr->optimisticsuccess[bt->thread_id]++;
			return slot;
		}

		while (slotptr(bt->page, slot)->dead)
		if (slot++ < bt->page->cnt)
			continue;
		else
			goto slideright;

		if (slot <= bt->page->cnt - bt->page->foster)
			drill--;

		//  continue down right without overlapping locks

		page_no = bt_getid(slotptr(bt->page, slot)->id);
		continue;

	slideright:
		page_no = bt_getid(bt->page->right);

	} while (page_no);

	// Immediately let go of any remaing locks
	if (prevpage)
	if (bt_unlockpage(bt, prevpage, prevmode)) {
		fprintf(stdout, "Error is out of dowhile\n");
        return 0;
    }
	else
		return bt_loadpage(bt, key, len, lvl, lock);	// fall back on other search
}

//  find and delete key on page by marking delete flag bit
//  when page becomes empty, delete it from the btree

BTERR bt_deletekey(BtDb *bt, unsigned char *key, uint len, uint lvl)
{
	unsigned char leftkey[256], rightkey[256];
	uid page_no, right;
	uint slot, tod;
	BtKey ptr;

	if (slot = bt_optimisticloadpage(bt, key, len, lvl, BtLockWrite))
		ptr = keyptr(bt->page, slot);
	else
		return bt->err;

	// if key is found delete it, otherwise ignore request

	if (!keycmp(ptr, key, len))
	if (slotptr(bt->page, slot)->dead == 0) {
		slotptr(bt->page, slot)->dead = 1;
		if (slot < bt->page->cnt)
			bt->page->dirty = 1;
		bt->page->act--;
	}

	// return if page is not empty, or it has no right sibling

	right = bt_getid(bt->page->right);
	page_no = bt->page_no;

	if (!right || bt->page->act)
		return bt_unlockpage(bt, page_no, BtLockWrite);

	// obtain Parent lock over write lock

	if (bt_lockpage(bt, page_no, BtLockParent, NULL))
		return bt->err;

	// cache copy of key to delete

	ptr = keyptr(bt->page, bt->page->cnt);
	memcpy(leftkey, ptr, ptr->len + 1);

	// lock and map right page

	if (bt_lockpage(bt, right, BtLockWrite, &bt->temp))
		return bt->err;

	// pull contents of next page into current empty page 
	memcpy(bt->page, bt->temp, bt->mgr->page_size);

	//	cache copy of key to update
	ptr = keyptr(bt->temp, bt->temp->cnt);
	memcpy(rightkey, ptr, ptr->len + 1);

	//  Mark right page as deleted and point it to left page
	//	until we can post updates at higher level.

	bt_putid(bt->temp->right, page_no);
	bt->temp->kill = 1;
	bt->temp->cnt = 0;

	if (bt_unlockpage(bt, right, BtLockWrite))
		return bt->err;
	if (bt_unlockpage(bt, page_no, BtLockWrite))
		return bt->err;

	//  delete old lower key to consolidated node

	if (bt_deletekey(bt, leftkey + 1, *leftkey, lvl + 1))
		return bt->err;

	//  redirect higher key directly to consolidated node

	if (slot = bt_optimisticloadpage(bt, rightkey + 1, *rightkey, lvl + 1, BtLockWrite))
		ptr = keyptr(bt->page, slot);
	else
		return bt->err;

	// since key already exists, update id

	if (keycmp(ptr, rightkey + 1, *rightkey))
		return bt->err = BTERR_struct;

	slotptr(bt->page, slot)->dead = 0;
	bt_putid(slotptr(bt->page, slot)->id, page_no);

	if (bt_unlockpage(bt, bt->page_no, BtLockWrite))
		return bt->err;

	//	obtain write lock and
	//	add right block to free chain

	if (bt_freepage(bt, right))
		return bt->err;

	// 	remove ParentModify lock

	if (bt_unlockpage(bt, page_no, BtLockParent))
		return bt->err;

	return 0;
}

//	find key in leaf level and return row-id

uid bt_findkey(BtDb *bt, unsigned char *key, uint len)
{
	uint  slot;
	BtKey ptr;
	uid id;

	if (slot = bt_optimisticloadpage(bt, key, len, 0, BtLockRead))
		ptr = keyptr(bt->page, slot);
	else
		return 0;

	// if key exists, return row-id
	//	otherwise return 0

	if (ptr->len == len && !memcmp(ptr->key, key, len))
		id = bt_getid(slotptr(bt->page, slot)->id);
	else
		id = 0;

	if (bt_unlockpage(bt, bt->page_no, BtLockRead))
		return 0;

	return id;
}

//	check page for space available,
//	clean if necessary and return
//	0 - page needs splitting
//	1 - go ahead

uint bt_cleanpage(BtDb *bt, uint amt)
{
	uint nxt = bt->mgr->page_size;
	BtPage page = bt->page;
	uint cnt = 0, idx = 0;
	uint max = page->cnt;
	BtKey key;

	if (page->min >= (max + 1) * sizeof(BtSlot)+sizeof(*page) + amt + 1)
		return 1;

	//	skip cleanup if nothing to reclaim

	if (!page->dirty)
		return 0;

	memcpy(bt->frame, page, bt->mgr->page_size);

	// skip page info and set rest of page to zero

	memset(page + 1, 0, bt->mgr->page_size - sizeof(*page));
	page->dirty = 0;
	page->act = 0;

	// try cleaning up page first

	while (cnt++ < max) {
		// always leave fence keys and foster children in list
		if (slotptr(bt->frame, cnt)->fence && slotptr(bt->frame, cnt)->dead)
			continue;

		// copy key
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy((unsigned char *)page + nxt, key, key->len + 1);

		// copy slot
		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		if (!(slotptr(page, idx)->dead = slotptr(bt->frame, cnt)->dead))
			page->act++;
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
	}

	page->min = nxt;
	page->cnt = idx;

	//	see if page has enough space now, or does it need splitting?

	if (page->min >= (idx + 1) * sizeof(BtSlot)+sizeof(*page) + amt + 1)
		return 1;

	return 0;
}

//	add key to current page
//	page must already be writelocked

void bt_addkeytopage(BtDb *bt, uint slot, unsigned char *key, uint len, uid id, uint tod)
{
	BtPage page = bt->page;
	uint idx;

	if (slot == 1)
		bt->mgr->lowfenceoverwrite[bt->thread_id]++;

	// calculate next available slot and copy key into page

	page->min -= len + 1;
	((unsigned char *)page)[page->min] = len;
	memcpy((unsigned char *)page + page->min + 1, key, len);

	for (idx = slot; idx < page->cnt; idx++)
	if (slotptr(page, idx)->dead)
		break;

	// now insert key into array before slot
	// preserving the fence slots

	if (idx == page->cnt)
		idx++, page->cnt++;

	page->act++;

	while (idx > slot && idx > 2)
		*slotptr(page, idx) = *slotptr(page, idx - 1), idx--;

	bt_putid(slotptr(page, slot)->id, id);
	slotptr(page, slot)->off = page->min;
	slotptr(page, slot)->tod = tod;
	slotptr(page, slot)->dead = 0;
    slotptr(page, slot)->fence = 0;
}

// split the root and raise the height of the btree
//	call with current page locked and page no of foster child
//	return with current page (root) unlocked

BTERR bt_splitroot(BtDb *bt, uid right)
{
	uint nxt = bt->mgr->page_size;
	unsigned char fencekey[256], starter[256];

	BtPage root = bt->page, page;
	uid new_page;
	BtKey key, ptr;
	int test;

#ifdef VERIFY
	// TODO: This is testing/debugging code
	if (bt->mgr->flag & 0x1) {
		fprintf(stdout, "\nFirst root split before:\n");

		for (test = 1; test <= root->cnt; test++) {
			ptr = keyptr(root, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fputc('\n', stdout);
		}
	}
#endif

	//  Obtain an empty page to use, and copy the left page
	//  contents into it from the root.  Strip foster child key.
	//	(it's the stopper key)

	root->act--;
	root->cnt--;
	root->foster--;

	//	Save left fence key.
	key = keyptr(root, root->cnt);
	memcpy(fencekey, key, key->len + 1);

	//  copy the lower keys into a new left page

	if (!(new_page = bt_newpage(bt, root)))
		return bt->err;

	// preserve the page info at the bottom
	// and set rest of the root to zero

	memset(root + 1, 0, bt->mgr->page_size - sizeof(*root));

	// insert left fence key on empty newroot page

	// Insert starter key on newroot page
	nxt -= 3;
	starter[0] = 2;
	starter[1] = 0x00;
	starter[2] = 0x00;
	memcpy((unsigned char *)root + nxt, starter, *starter + 1);
	bt_putid(slotptr(root, 1)->id, 0);
	slotptr(root, 1)->off = nxt;
	slotptr(root, 1)->fence = 1;
    slotptr(root, 1)->dead = 1;

	// Insert key on newroot page
	nxt -= *fencekey + 1;
	memcpy((unsigned char *)root + nxt, fencekey, *fencekey + 1);
	bt_putid(slotptr(root, 2)->id, new_page);
	slotptr(root, 2)->off = nxt;

	// Insert stopper key on newroot page
	nxt -= 3;
	fencekey[0] = 2;
	fencekey[1] = 0xff;
	fencekey[2] = 0xff;
	memcpy((unsigned char *)root + nxt, fencekey, *fencekey + 1);
	bt_putid(slotptr(root, 3)->id, right);
	slotptr(root, 3)->off = nxt;
	slotptr(root, 3)->fence = 1;

	// Increase root height
	bt_putid(root->right, 0);
	root->min = nxt;		// reset lowest used offset and key count
	root->cnt = 3;
	root->act = 3;
	root->lvl++;

#ifdef VERIFY
	// TODO: This is test code
	if (bt->mgr->flag & 0x1) {
		bt->mgr->flag = bt->mgr->flag & 0xe;

		fprintf(stdout, "\nFirst root split after - root:\n");
		for (test = 1; test <= root->cnt; test++) {
			ptr = keyptr(root, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fputc('\n', stdout);
		}

		if (bt_lockpage(bt, new_page, BtLockRead, &page))
			return bt->err;

		fprintf(stdout, "\nFirst root split after - new page:\n");
		for (test = 1; test <= page->cnt; test++) {
			ptr = keyptr(page, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fprintf(stdout, ", fence=%u", slotptr(page, test)->fence);
			fputc('\n', stdout);
		}

		if (bt_unlockpage(bt, new_page, BtLockRead))
			return bt->err;

		fprintf(stdout, "-----------------------------------------------------\n");
	}
#endif

	// release root (bt->page)

	return bt_unlockpage(bt, ROOT_page, BtLockWrite);
}

//  split already locked full node
//	in current page variables
//	return unlocked.

BTERR bt_splitpage(BtDb *bt)
{
	uint slot, cnt, idx, max, nxt = bt->mgr->page_size;
	unsigned char fencekey[256];
	uid page_no = bt->page_no;
	BtPage page = bt->page;
	uint tod = time(NULL);
	uint lvl = page->lvl;
	uid new_page, right;
	BtKey key, ptr;
	int test, hasfoster = 0;

#ifdef VERIFY
	// TODO: This is test code
	if (bt->mgr->flag & 0x2 || ((bt->mgr->counter > 100) && (bt->mgr->flag & 0x4) && (lvl == 0)) || page->foster > 0) {
		if((bt->mgr->counter > 100) && (bt->mgr->flag & 0x4) && (lvl == 0)) {
			bt->mgr->flag = bt->mgr->flag | 0x8;
			bt->mgr->flag = bt->mgr->flag & 0xb;
		}

		fprintf(stdout, "\nA normal split before:\n");
		fprintf(stdout, "    cnt=%d\n", page->cnt);
		fprintf(stdout, "    act=%d\n", page->act);
		fprintf(stdout, "    foster=%d\n", page->foster);

		if (page->foster > 0)
			hasfoster = 1;

		for (test = 1; test <= page->cnt; test++) {
			ptr = keyptr(page, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fprintf(stdout, ", fence=%u, dead=%u", slotptr(page, test)->fence, slotptr(page, test)->dead);
			fputc('\n', stdout);
		}
	}
#endif

	//	initialize frame buffer

	memset(bt->frame, 0, bt->mgr->page_size);
	max = page->cnt - page->foster;
	tod = (uint)time(NULL);
	cnt = max / 2;
	idx = 0;

	//  split higher half of keys to bt->frame
	//	leaving foster children in the left node.

	// Include what will be fence key
    // Mark as dead so key isn't read twice
	key = keyptr(page, cnt);
	nxt -= key->len + 1;
	memcpy((unsigned char *)bt->frame + nxt, key, key->len + 1);
	memcpy(slotptr(bt->frame, ++idx)->id, slotptr(page, cnt)->id, BtId);
	slotptr(bt->frame, idx)->tod = slotptr(page, cnt)->tod;
	slotptr(bt->frame, idx)->off = nxt;
	slotptr(bt->frame, idx)->fence = 1;
    slotptr(bt->frame, idx)->dead = 1;
    bt->frame->act++;

	while (cnt++ < max) {
		key = keyptr(page, cnt);
		nxt -= key->len + 1;
		memcpy((unsigned char *)bt->frame + nxt, key, key->len + 1);
		memcpy(slotptr(bt->frame, ++idx)->id, slotptr(page, cnt)->id, BtId);
		slotptr(bt->frame, idx)->tod = slotptr(page, cnt)->tod;
		slotptr(bt->frame, idx)->off = nxt;
		bt->frame->act++;
	}

	// Mark high fence key
	slotptr(bt->frame, idx)->fence = 1;

	// transfer right link node

	if (page_no > ROOT_page) {
		right = bt_getid(page->right);
		bt_putid(bt->frame->right, right);
	}

	bt->frame->bits = bt->mgr->page_bits;
	bt->frame->min = nxt;
	bt->frame->cnt = idx;
	bt->frame->lvl = lvl;

	//	get new free page and write frame to it.

	if (!(new_page = bt_newpage(bt, bt->frame)))
		return bt->err;

	//	remember fence key for new page to add
	//	as foster child

	key = keyptr(bt->frame, idx);
	memcpy(fencekey, key, key->len + 1);

	//	update lower keys and foster children to continue in old page

	memcpy(bt->frame, page, bt->mgr->page_size);
	memset(page + 1, 0, bt->mgr->page_size - sizeof(*page));
	nxt = bt->mgr->page_size;
	page->act = 0;
	cnt = 0;
	idx = 0;

	//  assemble page of smaller keys
	//	to remain in the old page

	while (cnt++ < max / 2) {
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy((unsigned char *)page + nxt, key, key->len + 1);
		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
		page->act++;
	}

	// Mark fence keys
	slotptr(page, 1)->fence = 1;
    slotptr(page, 1)->dead = 1;
	slotptr(page, idx)->fence = 1;

	//	insert new foster child at beginning of the current foster children
	nxt -= *fencekey + 1;
	memcpy((unsigned char *)page + nxt, fencekey, *fencekey + 1);
	bt_putid(slotptr(page, ++idx)->id, new_page);
	slotptr(page, idx)->tod = tod;
	slotptr(page, idx)->off = nxt;
	page->foster++;
	page->act++;

	//  continue with old foster child keys if any

	cnt = bt->frame->cnt - bt->frame->foster;

	while (cnt++ < bt->frame->cnt) {
		key = keyptr(bt->frame, cnt);
		nxt -= key->len + 1;
		memcpy((unsigned char *)page + nxt, key, key->len + 1);
		memcpy(slotptr(page, ++idx)->id, slotptr(bt->frame, cnt)->id, BtId);
		slotptr(page, idx)->tod = slotptr(bt->frame, cnt)->tod;
		slotptr(page, idx)->off = nxt;
		page->act++;
	}

	page->min = nxt;
	page->cnt = idx;

	//	link new right page

	bt_putid(page->right, new_page);

	// if current page is the root page, split it

	if (page_no == ROOT_page)
		return bt_splitroot(bt, new_page);

	//  release wr lock on our page

	if (bt_unlockpage(bt, page_no, BtLockWrite))
		return bt->err;

	//  obtain ParentModification lock for current page
	//	to fix fence key and highest foster child on page

	if (bt_lockpage(bt, page_no, BtLockParent, NULL))
		return bt->err;

	//  get our highest foster child key to find in parent node

	if (bt_lockpage(bt, page_no, BtLockRead, &page))
		return bt->err;

	key = keyptr(page, page->cnt);
	memcpy(fencekey, key, key->len + 1);

	if (bt_unlockpage(bt, page_no, BtLockRead))
		return bt->err;

	//  update our parent
try_again:

	do {
		slot = bt_optimisticloadpage(bt, fencekey + 1, *fencekey, lvl + 1, BtLockWrite);

		if (!slot)
			return bt->err;

		// check if parent page has enough space for any possible key

		if (bt_cleanpage(bt, 256))
			break;

		if (bt_splitpage(bt))
			return bt->err;
	} while (1);

	//  see if we are still a foster child from another node

	if (bt_getid(slotptr(bt->page, slot)->id) != page_no) {
		if (bt_unlockpage(bt, bt->page_no, BtLockWrite))
			return bt->err;

		sched_yield();

		goto try_again;
	}

	//	wait until readers from parent get their locks
	//	on our page

	if (bt_lockpage(bt, page_no, BtLockDelete, NULL))
		return bt->err;

	//	lock our page for writing

	if (bt_lockpage(bt, page_no, BtLockWrite, &page))
		return bt->err;

	//	switch parent fence key to foster child

	if (slotptr(page, page->cnt)->dead)
		slotptr(bt->page, slot)->dead = 1;
	else
		bt_putid(slotptr(bt->page, slot)->id, bt_getid(slotptr(page, page->cnt)->id));

	//	remove highest foster child from our page

	page->cnt--;
	page->act--;
	page->foster--;
	page->dirty = 1;
	key = keyptr(page, page->cnt);

	//	add our new fence key for foster child to our parent

	bt_addkeytopage(bt, slot, key->key, key->len, page_no, tod);

	if (bt_unlockpage(bt, bt->page_no, BtLockWrite))
		return bt->err;

	if (bt_unlockpage(bt, page_no, BtLockDelete))
		return bt->err;

	if (bt_unlockpage(bt, page_no, BtLockWrite))
		return bt->err;

	if (bt_unlockpage(bt, page_no, BtLockParent))
		return bt->err;

#ifdef VERIFY
	// TODO: This is test code
	if (bt->mgr->flag & 0x2 || bt->mgr->flag & 0x8 || hasfoster) {
		if(bt->mgr->flag & 0x2)
			bt->mgr->flag = bt->mgr->flag & 0xd;

		if(bt->mgr->flag & 0x8)
			bt->mgr->flag = bt->mgr->flag & 0x7;

		fprintf(stdout, "\nA normal split - page 1:\n");

		for (test = 1; test <= page->cnt; test++) {
			ptr = keyptr(page, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fprintf(stdout, ", fence=%u, dead=%u", slotptr(page, test)->fence, slotptr(page, test)->dead);
			fputc('\n', stdout);
		}

		if (bt_lockpage(bt, new_page, BtLockRead, &page))
			return bt->err;

		fprintf(stdout, "\nA normal split - page 2:\n");
		for (test = 1; test <= page->cnt; test++) {
			ptr = keyptr(page, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fprintf(stdout, ", fence=%u, dead=%u", slotptr(page, test)->fence, slotptr(page, test)->dead);
			fputc('\n', stdout);
		}

		if (bt_unlockpage(bt, new_page, BtLockRead))
			return bt->err;

		fprintf(stdout, "\nA normal split - parent:\n");

		for (test = 1; test <= bt->page->cnt; test++) {
			ptr = keyptr(bt->page, test);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fprintf(stdout, ", fence=%u, dead=%u", slotptr(bt->page, test)->fence, slotptr(bt->page, test)->dead);
			fputc('\n', stdout);
		}

		fprintf(stdout, "-----------------------------------------------------\n");
		
	}
#endif
	bt->mgr->counter++;

	//return bt_unlockpage(bt, page_no, BtLockParent);
	return 0;
}

//  Insert new key into the btree at leaf level.

BTERR bt_insertkey(BtDb *bt, unsigned char *key, uint len, uid id, uint tod)
{
	uint slot, idx;
	BtPage page;
	BtKey ptr;

	while (1) {
		if (slot = bt_optimisticloadpage(bt, key, len, 0, BtLockWrite)) {
			ptr = keyptr(bt->page, slot);
		}
		else
		{
			if (!bt->err)
				bt->err = BTERR_ovflw;
			return bt->err;
		}

		// if key already exists, update id and return

		page = bt->page;

		if (!keycmp(ptr, key, len)) {
			slotptr(page, slot)->dead = 0;
			slotptr(page, slot)->tod = tod;
			bt_putid(slotptr(page, slot)->id, id);
			return bt_unlockpage(bt, bt->page_no, BtLockWrite);
		}

		// check if page has enough space

		if (bt_cleanpage(bt, len))
			break;

		if (bt_splitpage(bt))
			return bt->err;
	}

	bt_addkeytopage(bt, slot, key, len, id, tod);

	return bt_unlockpage(bt, bt->page_no, BtLockWrite);
}

//  cache page of keys into cursor and return starting slot for given key

uint bt_startkey(BtDb *bt, unsigned char *key, uint len)
{
	uint slot;
	
	// cache page for retrieval
	if (slot = bt_optimisticloadpage(bt, key, len, 0, BtLockRead))
		memcpy(bt->cursor, bt->page, bt->mgr->page_size);
	bt->cursor_page = bt->page_no;
	if (bt_unlockpage(bt, bt->page_no, BtLockRead))
		return 0;

	return slot;
}

//  return next slot for cursor page
//  or slide cursor right into next page

uint bt_nextkey(BtDb *bt, uint slot)
{
	BtPage page;
	uid right;

	do {
		right = bt_getid(bt->cursor->right);
		while (slot++ < bt->cursor->cnt - bt->cursor->foster)
		if (slotptr(bt->cursor, slot)->dead)
			continue;
		else if (right || (slot < bt->cursor->cnt - bt->cursor->foster))
			return slot;
		else
			break;

		if (!right)
			break;

		bt->cursor_page = right;

		if (bt_lockpage(bt, right, BtLockRead, &page))
			return 0;

		memcpy(bt->cursor, page, bt->mgr->page_size);

		if (bt_unlockpage(bt, right, BtLockRead))
			return 0;

		slot = 0;
	} while (1);

	return bt->err = 0;
}

BtKey bt_key(BtDb *bt, uint slot)
{
	return keyptr(bt->cursor, slot);
}

uid bt_uid(BtDb *bt, uint slot)
{
	return bt_getid(slotptr(bt->cursor, slot)->id);
}

uint bt_tod(BtDb *bt, uint slot)
{
	return slotptr(bt->cursor, slot)->tod;
}

typedef struct {
	char type, idx;
	char *ctx_string;
	int ctx_num;
	BtMgr *mgr;
	int num;
} ThreadArg;

//  standalone program to index file of keys
//  then list them onto std-out

void *index_file(void *arg)
{
	int line = 0, found = 0, cnt = 0;
	unsigned char key[256];
	ThreadArg *args = arg;
	int len = 0, slot, rowid, numchars;
	time_t tod[1];
	BtKey ptr;
	BtPage page;
	uid right;
	BtDb *bt;
	FILE *in;
	char *fileBuffer, *token;
	unsigned char ch[1];
	long numbytes;

	bt = bt_open(args->mgr, args->idx);
	time(tod);

	switch (args->type | 0x20)
	{
	case 'w':
		fprintf(stdout, "started indexing for %s\n", args->ctx_string);

		/* open an existing file for reading */
		in = fopen(args->ctx_string, "r");

		/* quit if the file does not exist */
		if (in == NULL)
			fprintf(stderr, "Error: File does not exist"), exit(0);

		/* Get the number of bytes */
		fseek(in, 0L, SEEK_END);
		numbytes = ftell(in);

		/* reset the file position indicator to
		the beginning of the file */
		fseek(in, 0L, SEEK_SET);

		/* grab sufficient memory for the
		buffer to hold the text */
		fileBuffer = (char*)calloc(numbytes, sizeof(char));

		/* memory error */
		if (fileBuffer == NULL)
			fprintf(stderr, "Error: Memory issue"), exit(0);

		/* copy all the text into the buffer */
		fread(fileBuffer, sizeof(char), numbytes, in);
		fclose(in);

		/* confirm we have read the file by
		outputing it to the console */
		fprintf(stdout, "The file called %s has been loaded\n", args->ctx_string);

		/*Split the text by endline characters*/
		numchars = 0;
		while (numchars < numbytes) {
			line++;
			len = 0;

			ch[0] = '\0';
			while (ch[0] != '\n') {
				memcpy(ch, fileBuffer + numchars + len, 1);
				len += 1;
			}
			memcpy(key, fileBuffer + numchars, len - 1);
			numchars += len;

                if (bt_insertkey(bt, key, (len-1), line, *tod))
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
		}

		/* free the memory we used for the buffer */
		//free(fileBuffer);

		fprintf(stdout, "finished %s for %d keys\n", args->ctx_string, line);
		/*
		fprintf(stdout, "Check root page:\n");

		if (bt_lockpage(bt, ROOT_page, BtLockRead, &page))
			return 0;

		fprintf(stdout, "    cnt: %d\n", page->cnt);
		fprintf(stdout, "    act: %d\n", page->act);
		fprintf(stdout, "    foster: %d\n", page->foster);
		fprintf(stdout, "    lvl: %d\n", page->lvl);

		for (found = 1; found <= page->cnt; found++) {
			ptr = keyptr(bt->page, found);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fputc('\n', stdout);
		}

		if (bt_unlockpage(bt, ROOT_page, BtLockRead))
			return 0;
		*/
		fprintf(stdout, "started reading\n");

        line = len = key[0] = 0;
		if (slot = bt_startkey(bt, key, len))
			slot--;
		else
			fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

		line++;
		while (slot = bt_nextkey(bt, slot)) {
			ptr = bt_key(bt, slot);
			line++;
			fwrite(ptr->key, ptr->len, 1, stdout);
            fprintf(stdout, ", dead=%d", slotptr(bt->cursor, slot)->dead);
			fputc('\n', stdout);
		}

		fprintf(stdout, "Finished reading %d keys\n", line);

        fprintf(stdout, "Delete everything\n");
        numchars = line = 0;
		while (numchars < numbytes) {
			line++;
			len = 0;

			ch[0] = '\0';
			while (ch[0] != '\n') {
				memcpy(ch, fileBuffer + numchars + len, 1);
				len += 1;
			}
			memcpy(key, fileBuffer + numchars, len - 1);
			numchars += len;

                if (bt_deletekey(bt, key, (len-1), 0))
				    fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
		}

        fprintf(stdout, "started reading again\n");

        line = len = key[0] = 0;
		if (slot = bt_startkey(bt, key, len))
			slot--;
		else
			fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

		line++;
		while (slot = bt_nextkey(bt, slot)) {
			ptr = bt_key(bt, slot);
			line++;
			fwrite(ptr->key, ptr->len, 1, stdout);
			fputc('\n', stdout);
		}

        fprintf(stdout, "Finished reading %d keys\n", line);

        fprintf(stdout, "Re-Insert everything\n");
        numchars = line = 0;
		while (numchars < numbytes) {
			line++;
			len = 0;

			ch[0] = '\0';
			while (ch[0] != '\n') {
				memcpy(ch, fileBuffer + numchars + len, 1);
				len += 1;
			}
			memcpy(key, fileBuffer + numchars, len - 1);
			numchars += len;

                if (bt_insertkey(bt, key, (len-1), line, *tod))
				fprintf(stderr, "Error %d Line: %d\n", bt->err, line), exit(0);
		}

        fprintf(stdout, "started reading\n");

        line = len = key[0] = 0;
		if (slot = bt_startkey(bt, key, len))
			slot--;
		else
			fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

		line++;
		while (slot = bt_nextkey(bt, slot)) {
			ptr = bt_key(bt, slot);
			line++;
			fwrite(ptr->key, ptr->len, 1, stdout);
            fprintf(stdout, ", dead=%d", slotptr(bt->cursor, slot)->dead);
			fputc('\n', stdout);
		}

		fprintf(stdout, "Finished reading %d keys\n", line);

		/*
		if (rowid = bt_findkey(bt, "voluntary", 9)) {
			fprintf(stdout, "Found the key in row: %d\n", rowid);
		}
		else {
			fprintf(stdout, "Key not found\n");
		}

		if (slot = bt_optimisticloadpage(bt, "voluntary", 9, 0, BtLockRead)) {
			fprintf(stdout, "Found the slot: %d\n", slot);
			ptr = keyptr(bt->page, slot);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fputc('\n', stdout);
		}
		else {
			fprintf(stdout, "Slot not found\n");
		}

		if (slot && rowid && bt->page) {
			fprintf(stdout, "Printing out page:\n");
			for (found = 1; found <= bt->page->cnt; found++) {
				ptr = keyptr(bt->page, found);
				fwrite(ptr->key, ptr->len, 1, stdout);
				fputc('\n', stdout);
			}
		}
		
		fprintf(stdout, "Check next page: %ld, next is: %ld\n", bt->page_no, bt_getid(bt->page->right));
		if (bt->page->right) {
			right = bt_getid(bt->page->right);
			bt->cursor_page = right;

			if (bt_lockpage(bt, right, BtLockRead, &page))
				return 0;

			memcpy(bt->cursor, page, bt->mgr->page_size);

			if (bt_unlockpage(bt, right, BtLockRead))
				return 0;

			slot = 0;

			for (found = 1; found <= bt->cursor->cnt; found++) {
				ptr = keyptr(bt->cursor, found);
				fwrite(ptr->key, ptr->len, 1, stdout);
				fputc('\n', stdout);
			}
		}
		*/
		
		break;
	case 's':
		len = key[0] = 0;

		fprintf(stdout, "started reading\n");

		if (slot = bt_startkey(bt, key, len))
			slot--;
		else
			fprintf(stderr, "Error %d in StartKey. Syserror: %d\n", bt->err, errno), exit(0);

		line++;
		while (slot = bt_nextkey(bt, slot)) {
			ptr = bt_key(bt, slot);
			line++;
			//fwrite(ptr->key, ptr->len, 1, stdout);
			//fputc('\n', stdout);
		}

		fprintf(stdout, "Finished reading %d keys\n", line);

		break;
	case 'f':
		fprintf(stdout, "started finding\n");
		
		if (rowid = bt_findkey(bt, args->ctx_string, args->ctx_num)) {
			fprintf(stdout, "Found the key in row: %d\n", rowid);
		}
		else {
			fprintf(stdout, "Key not found\n");
		}

		if (slot = bt_optimisticloadpage(bt, args->ctx_string, args->ctx_num, 0, BtLockRead)) {
			fprintf(stdout, "Found the slot: %d\n", slot);
			ptr = keyptr(bt->page, slot);
			fwrite(ptr->key, ptr->len, 1, stdout);
			fputc('\n', stdout);
		}
		else {
			fprintf(stdout, "Slot not found\n");
		}

		if (slot && rowid && bt->page) {
			fprintf(stdout, "Printing out page:\n");
			for (found = 1; found < bt->page->cnt; found++) {
				ptr = keyptr(bt->page, found);
				fwrite(ptr->key, ptr->len, 1, stdout);
				fputc('\n', stdout);
			}
		}

		break;
	}

	bt_close(bt);
	return NULL;
}

typedef struct timeval timer;

int main(int argc, char **argv)
{
	int idx, cnt, len, slot, err, stats;
	pthread_t *threads;
	timer start, stop;
	double real_time;
	ThreadArg *args;
	int ctx_num = 0;
	char ch[256];
	BtMgr *mgr;
	BtKey ptr;
	BtDb *bt;

	if (argc < 5) {
		fprintf(stderr, "Usage: %s stats(0 or 1) Write/Find/Multithread/Scan context_num context_string1 [context_string2...]\n", argv[0]);
		fprintf(stderr, "  stats is whether or not you want to see timing statistics\n");
		fprintf(stderr, "  Write (w), Find (f), Multithread (m), and Scan (s) are the only options right now\n");
		fprintf(stderr, "  context_num is some number that might be used in processing\n");
		fprintf(stderr, "  context_string1 thru context_stringn are dependent on function\n\n");

		fprintf(stderr, "  in the context of writes, the number means nothing and the strings are filenames\n");
		fprintf(stderr, "  in the context of finds, the number is the key length and the first string is the key to find\n");
		fprintf(stderr, "  in the context of scans, neither matter\n");
		exit(0);
	}

	gettimeofday(&start, NULL);

	stats = atoi(argv[1]);
	ctx_num = atoi(argv[3]);

	cnt = argc - 4;
	threads = malloc(cnt * sizeof(pthread_t));

	args = malloc(cnt * sizeof(ThreadArg));

	mgr = bt_mgr((argv[1]), BT_rw, BITS, POOLSIZE, SEGSIZE);
	mgr->flag = 0x7;
	mgr->counter = 0;

	if (!mgr) {
		fprintf(stderr, "Index Open Error %s\n", argv[1]);
		exit(1);
	}

	//	fire off threads
	calibratePreciseTime();

	for (idx = 0; idx < cnt; idx++) {
		args[idx].ctx_string = argv[idx + 4];
		args[idx].ctx_num = ctx_num;
		args[idx].type = argv[2][0];
		args[idx].mgr = mgr;
		args[idx].num = NUMMODE;
		args[idx].idx = idx;
		if (err = pthread_create(threads + idx, NULL, index_file, args + idx))
			fprintf(stderr, "Error creating thread %d\n", err);
	}

	// 	wait for termination

	for (idx = 0; idx < cnt; idx++)
		pthread_join(threads[idx], NULL);

	gettimeofday(&stop, NULL);
	real_time = 1000.0 * (stop.tv_sec - start.tv_sec) + 0.001 * (stop.tv_usec - start.tv_usec);

	if (stats) {
		fprintf(stdout, " Time to complete: %.2f seconds\n", real_time / 1000);
        fprintf(stdout, " Time waiting for read locks on root:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %.2f seconds\n", idx, (double)(mgr->rootreadwait[idx] / getTicksPerNano() / 1000000000));
		}
		fprintf(stdout, " Time waiting for write locks on root:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %.2f seconds\n", idx, (double)(mgr->rootwritewait[idx] / getTicksPerNano() / 1000000000));
		}

		fprintf(stdout, " Time waiting for read locks:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %.2f seconds\n", idx, (double)(mgr->readlockwait[idx] / getTicksPerNano() / 1000000000));
		}
		fprintf(stdout, " Time waiting for write locks:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %.2f seconds\n", idx, (double)(mgr->writelockwait[idx] / getTicksPerNano() / 1000000000));
		}
		fprintf(stdout, " Number of read locks aquired:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %llu aquired\n", idx, mgr->readlockaquired[idx]);
		}
		fprintf(stdout, " Number of write locks aquired:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %llu aquired\n", idx, mgr->writelockaquired[idx]);
		}
		fprintf(stdout, " Number of read locks failed:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %llu attempts\n", idx, mgr->readlockfail[idx]);
		}
		fprintf(stdout, " Number of write locks failed:\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "     Thread %d - %llu attempts\n", idx, mgr->writelockfail[idx]);
		}
		fprintf(stdout, "\n CSV Lines: \n");
		fprintf(stdout, "Threads,Execution,Thread_Id,Read_Lock_Wait,Write_Lock_Wait,Read_Lock_Aquired,Write_Lock_Aquired,Read_Lock_Failed,Write_Lock_Failed\n");
		for (idx = 0; idx < cnt; idx++) {
			fprintf(stdout, "%d,%.2f,%d,%.2f,%.2f,%llu,%llu,%llu,%llu\n", cnt, (real_time / 1000), idx, (double)(mgr->readlockwait[idx] / getTicksPerNano()),
				(double)(mgr->writelockwait[idx] / getTicksPerNano()), mgr->readlockaquired[idx], mgr->writelockaquired[idx], mgr->readlockfail[idx], mgr->writelockfail[idx]);
		}
	}

	fprintf(stdout, " Low fence overwrites:\n");
	for (idx = 0; idx < cnt; idx++) {
		fprintf(stdout, "     Thread %d - %llu times\n", idx, mgr->lowfenceoverwrite[idx]);
	}
	fprintf(stdout, " Optimistic search failures:\n");
	for (idx = 0; idx < cnt; idx++) {
		fprintf(stdout, "     Thread %d - %llu times\n", idx, mgr->optimisticfail[idx]);
	}
	fprintf(stdout, " Optimistic search successes:\n");
	for (idx = 0; idx < cnt; idx++) {
		fprintf(stdout, "     Thread %d - %llu times\n", idx, mgr->optimisticsuccess[idx]);
	}

	bt_mgrclose(mgr);
}
