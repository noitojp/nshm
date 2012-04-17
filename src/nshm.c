#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include "nshm.h"

#define _NOW_VERSION 1
#define _ALIGNMENT_BYTE 8
#define _MAGIC "_nshm_"
#define _MAGIC_SIZ ((size_t)8)
#define _SMALL_MSEG_NUM 64
#define _SMALL_MSEG_NMEMB 8
#define _BIG_MSEG_NUM 20
#define _BIG_MSEG_NMEMB 4
#define _SEGSIZE_THRESHOLD 1024
#define _MAX_ALLOCSIZE (1024*1024*1024)
#define _DATA_HASH_SIZ 1024

typedef struct{
	char magic[_MAGIC_SIZ];
	pthread_spinlock_t lock_point;
	int64_t free_offset;
	int64_t small_msegs[_SMALL_MSEG_NUM];
	int64_t big_msegs[_BIG_MSEG_NUM];
	int64_t data[_DATA_HASH_SIZ];
	int32_t replaced;
	int32_t version;
	time_t ctime;
} _nshm_base_t;

typedef struct{
	int64_t key_offset;
	int32_t klen;
	int64_t val_offset;
	int64_t next_offset;
} _tokarr_t;

typedef struct{
	int64_t next_offset;
	int32_t seg_siz;
} _shmem_hdr;

static void* _alloc_small_shmem(NShm *nshm,int32_t size);
static void* _alloc_big_shmem(NShm *nshm,int32_t size);
static void _free_small_shmem(NShm *nshm,_shmem_hdr *hdr);
static void _free_big_shmem(NShm *nshm,_shmem_hdr *hdr);
static void* _shmalloc(NShm *nshm,int32_t size);
static void _init_meta(NShm *nshm);
static void _lock_nshm(NShm *nshm);
static void _unlock_nshm(NShm *nshm);
static int _get_key_hash(const char *key,int klen);

static inline int get_smallseg_index(int32_t siz)
{
	return((siz-1)/0x10);
}

static inline int32_t get_smallseg_size(int32_t index)
{
	return((index+1)*0x10);
}

static inline int get_bigseg_index(int32_t siz)
{
	int rc = 0;
	unsigned int ix;
	unsigned int usiz;

	usiz = (unsigned int)siz;
	for(ix = _SEGSIZE_THRESHOLD; ix < usiz; ix <<= 1){
		rc++;
	}

	return(rc - 1);
}

static inline int32_t get_bigseg_size(int32_t index)
{
	int32_t rc = _SEGSIZE_THRESHOLD * 2 ;
	int ix;

	for(ix = 0; ix < index; ix++){
		rc <<= 1;
	}

	return(rc);
}

NShm* nshm_create(const char *path,size_t size,mode_t mode)
{
	NShm *nshm = NULL;
	_nshm_base_t *base;
	int ix;

	if( NULL == path || size <= sizeof(_nshm_base_t) ){
		goto error;
	}

	nshm = (NShm*)malloc(sizeof(*nshm));
	if( NULL == nshm ){
		fprintf(stderr,"ERROR: malloc: %s\n",strerror(errno));
		goto error;
	}
	_init_meta(nshm);

	nshm->_fd = open(path,O_RDWR|O_CREAT|O_TRUNC,mode);
	if( nshm->_fd < 0 ){
		fprintf(stderr,"ERROR: open %s: %s\n",path,strerror(errno));
		goto error;
	}

	if( ftruncate(nshm->_fd,(off_t)size) != 0 ){
		fprintf(stderr,"ERROR: ftruncate %s: %s\n",path,strerror(errno));
		goto error;
	}
	nshm->_size = (size_t)size;

	nshm->_base = mmap(0,nshm->_size,PROT_READ|PROT_WRITE,MAP_SHARED,nshm->_fd,(off_t)0);
	if( MAP_FAILED == nshm->_base ){
		fprintf(stderr,"ERROR: mmap %s: %s\n",path,strerror(errno));
		goto error;
	}

	base = (_nshm_base_t*)nshm->_base;
	strncpy(base->magic,_MAGIC,_MAGIC_SIZ);
	base->version = _NOW_VERSION;
	base->replaced = 0;
	base->ctime = time(NULL);
	if( pthread_spin_init(&base->lock_point,PTHREAD_PROCESS_SHARED) != 0 ){
		fprintf(stderr,"ERROR: pthread_spin_init %s: %s\n",path,strerror(errno));
		goto error;
	}
	base->free_offset = (int64_t)sizeof(_nshm_base_t);

	for(ix = 0; ix < _SMALL_MSEG_NUM; ix++ ){
		base->small_msegs[ix] = (int64_t)-1;
	}

	for(ix = 0; ix < _BIG_MSEG_NUM; ix++ ){
		base->big_msegs[ix] = (int64_t)-1;
	}

	for(ix = 0; ix < _DATA_HASH_SIZ; ix++ ){
		base->data[ix] = (int64_t)-1;
	}

	fprintf(stderr,"INFO: create v%d nshm: %s\n",base->version,path);
	return(nshm);

 error:
	nshm_detach(nshm);
	return(NULL);
}

NShm* nshm_attach(const char *path)
{
	NShm *nshm = NULL;
	_nshm_base_t *base;
	struct stat st;

	if( NULL == path ){
		goto error;
	}

	nshm = (NShm*)malloc(sizeof(*nshm));
	if( NULL == nshm ){
		fprintf(stderr,"ERROR: malloc: %s\n",strerror(errno));
		goto error;
	}

	_init_meta(nshm);
	nshm->_fd = open(path,O_RDWR);
	if( nshm->_fd < 0 ){
		fprintf(stderr,"ERROR: open %s: %s\n",path,strerror(errno));
		goto error;
	}

	if( fstat(nshm->_fd,&st) < 0 ){
		fprintf(stderr,"ERROR: fstat %s: %s\n",path,strerror(errno));
		goto error;
	}

	nshm->_size = (size_t)st.st_size;
	nshm->_base = mmap(0,nshm->_size,PROT_READ|PROT_WRITE,MAP_SHARED,nshm->_fd,(off_t)0);
	if( MAP_FAILED == nshm->_base ){
		fprintf(stderr,"ERROR: mmap %s: %s\n",path,strerror(errno));
		goto error;
	}

	base = (_nshm_base_t*)nshm->_base;
	if( strncmp(base->magic,_MAGIC,strlen(_MAGIC)) != 0 ){
		fprintf(stderr,"ERROR: %s isn't nshm\n",path);
		goto error;
	}

	if( base->version != _NOW_VERSION ){
		fprintf(stderr,"ERROR: %s isn't supported(v%d)\n",path,base->version);
		goto error;
	}

	fprintf(stderr,"INFO: attach v%d nshm: %s\n",base->version,path);
	return(nshm);

 error:
	nshm_detach(nshm);
	return(NULL);
}

void nshm_detach(NShm *nshm)
{
	if( NULL == nshm ){
		return;
	}

	if( nshm->_base != MAP_FAILED ){
		munmap(nshm->_base,nshm->_size);
		nshm->_base = MAP_FAILED;
	}

	if( nshm->_fd >= 0 ){
		close(nshm->_fd); nshm->_fd = -1;
	}

	_init_meta(nshm);
	free(nshm);
	fprintf(stderr,"INFO: detach nshm\n");
}

NShm* nshm_reattach(const char *path,NShm *nshm)
{
	nshm_detach(nshm);
	return( nshm_attach(path) );
}

void* nshm_memalloc(NShm *nshm,int32_t size)
{
	if( NULL == nshm ){
		return(NULL);
	}

	if( size <= 0 ){
		return(NULL);
	}
	else if( size <= _SEGSIZE_THRESHOLD ){
		return( _alloc_small_shmem(nshm,size) );
	}
	else if( size <= _MAX_ALLOCSIZE ){
		return( _alloc_big_shmem(nshm,size) );
	}
	else{
		return(NULL);
	}
}

void nshm_memfree(NShm *nshm,void *ptr)
{
	_shmem_hdr *hdr;

	if( NULL == nshm || NULL == ptr ){
		return;
	}

	hdr = (_shmem_hdr*)ptr;
	hdr--;
	if( hdr->seg_siz <= 0 ){
		return;
	}
	else if( hdr->seg_siz <= _SEGSIZE_THRESHOLD ){
		_free_small_shmem(nshm,hdr);
	}
	else if( hdr->seg_siz <= _MAX_ALLOCSIZE ){
		_free_big_shmem(nshm,hdr);
	}

	return;
}

void* shmalloc(NShm *nshm,int32_t size)
{
	void *rc;

	if( NULL == nshm || size <= 0 ){
		return(NULL);
	}

	_lock_nshm(nshm);
	rc = _shmalloc(nshm,size);
	_unlock_nshm(nshm);
	return(rc);
}

void* nshm_get(NShm *nshm,const char *key,int klen)
{
	void *rc = NULL;
	void *_nshm_base = NULL;
	_nshm_base_t *base;
	int hash;
	int64_t offset;
	_tokarr_t *tokarr;
	const char *key_vaddr;

	if( NULL == nshm || NULL == key || klen <= 0 ){
		return(NULL);
	}

	hash = _get_key_hash(key,klen);
	base = (_nshm_base_t*)nshm->_base;
	SET_NSHMBASE(nshm);

	offset = base->data[hash];
	while( offset >= 0 ){
		tokarr = vos_ptr(_tokarr_t*,offset);
		if( tokarr->klen == klen ){
			key_vaddr = vos_ptr(const char*,tokarr->key_offset);
			if( memcmp(key_vaddr,key,(size_t)klen) == 0 ){
				rc = vos_ptr(char*,tokarr->val_offset);
				break;
			}
		}

		offset = tokarr->next_offset;
	}

	return(rc);
}

int nshm_set(NShm *nshm,const char *key,int klen,const void *val)
{
	void *_nshm_base = NULL;
	_nshm_base_t *base;
	int hash;
	_tokarr_t *tokarr = NULL;
	char *key_vaddr = NULL;

	if( NULL == nshm || NULL == key || klen <=0 ){
		return(-1);
	}

	if( nshm_get(nshm,key,klen) != NULL ){
		return(0);
	}

	tokarr = (_tokarr_t*)nshm_memalloc(nshm,(int32_t)sizeof(_tokarr_t));
	if( NULL == tokarr ){
		goto error;
	}

	SET_NSHMBASE(nshm);
	key_vaddr = (char*)nshm_memalloc(nshm,klen);
	if( NULL == key_vaddr ){
		goto error;
	}

	memcpy(key_vaddr,key,(size_t)klen);
	tokarr->key_offset = vos_assign(int64_t,key_vaddr);
	tokarr->klen = klen;
	tokarr->val_offset = vos_assign(int64_t,val);

	hash = _get_key_hash(key,klen);
	base = (_nshm_base_t*)nshm->_base;

	_lock_nshm(nshm);
	tokarr->next_offset = base->data[hash];
	base->data[hash] = vos_assign(int64_t,tokarr);
	_unlock_nshm(nshm);
	return(1);

 error:
	if( tokarr != NULL ){
		nshm_memfree(nshm,tokarr);
	}

	if( key_vaddr != NULL ){
		nshm_memfree(nshm,key_vaddr);
	}

	return(-1);
}

time_t nshm_get_ctime(NShm *nshm)
{
	if( NULL == nshm ){
		return(-1);
	}

	return(((_nshm_base_t*)nshm->_base)->ctime);
}

int nshm_get_replaced(NShm *nshm)
{
	if( NULL == nshm ){
		return(-1);
	}

	return(((_nshm_base_t*)nshm->_base)->replaced);
}

int64_t nshm_get_restbyte(NShm *nshm)
{
	if( NULL == nshm ){
		return(0);
	}

	return((int64_t)(nshm->_size - ((_nshm_base_t*)nshm->_base)->free_offset));
}

static void* _alloc_small_shmem(NShm *nshm,int32_t size)
{
	void *_nshm_base = NULL;
	void *rc = NULL;
	int index;
	int32_t seg_siz;
	_nshm_base_t *base;
	_shmem_hdr *ptr;

	index = get_smallseg_index(size);
	seg_siz = get_smallseg_size(index);
	base = (_nshm_base_t*)nshm->_base;

	SET_NSHMBASE(nshm);
	_lock_nshm(nshm);
	if( base->small_msegs[index] < 0 ){
		int ix;

		for( ix = 0; ix < _SMALL_MSEG_NMEMB; ix++){
			int32_t alloc_siz;

			alloc_siz = (int32_t)(sizeof(int64_t) + sizeof(int32_t) + seg_siz);
			ptr = (_shmem_hdr*)_shmalloc(nshm,(int32_t)alloc_siz);
			if( NULL == ptr ){
				goto finally;
			}

			ptr->seg_siz = seg_siz;
			ptr->next_offset = base->small_msegs[index];
			base->small_msegs[index] = vos_assign(int64_t,ptr);
		}
	}

	ptr = vos_ptr(_shmem_hdr*,base->small_msegs[index]);
	base->small_msegs[index] = ptr->next_offset;
	ptr->next_offset = -1;
	rc = (void*)(ptr + 1);

 finally:
	_unlock_nshm(nshm);
	return(rc);
}

static void* _alloc_big_shmem(NShm *nshm,int32_t size)
{
	void *_nshm_base = NULL;
	void *rc = NULL;
	int index;
	int32_t seg_siz;
	_nshm_base_t *base;
	_shmem_hdr *ptr;

	index = get_bigseg_index(size);
	seg_siz = get_bigseg_size(index);
	base = (_nshm_base_t*)nshm->_base;

	SET_NSHMBASE(nshm);
	_lock_nshm(nshm);
	if( base->big_msegs[index] < 0 ){
		int ix;

		for( ix = 0; ix < _BIG_MSEG_NMEMB; ix++){
			int32_t alloc_siz;

			alloc_siz = (int32_t)(sizeof(int64_t) + sizeof(int32_t) + seg_siz);
			ptr = (_shmem_hdr*)_shmalloc(nshm,(int32_t)alloc_siz);
			if( NULL == ptr ){
				goto finally;
			}

			ptr->seg_siz = seg_siz;
			ptr->next_offset = base->big_msegs[index];
			base->big_msegs[index] = vos_assign(int64_t,ptr);
		}
	}

	ptr = vos_ptr(_shmem_hdr*,base->big_msegs[index]);
	base->big_msegs[index] = ptr->next_offset;
	ptr->next_offset = -1;
	rc = (void*)(ptr + 1);

 finally:
	_unlock_nshm(nshm);
	return(rc);
}

static void _free_small_shmem(NShm *nshm,_shmem_hdr *hdr)
{
	void *_nshm_base = NULL;
	_nshm_base_t *base;
	int index;

	base = (_nshm_base_t*)nshm->_base;
	index = get_smallseg_index(hdr->seg_siz);
	SET_NSHMBASE(nshm);
	_lock_nshm(nshm);
	hdr->next_offset = base->small_msegs[index];
	base->small_msegs[index] = vos_assign(int64_t,hdr);
	_unlock_nshm(nshm);
}

static void _free_big_shmem(NShm *nshm,_shmem_hdr *hdr)
{
	void *_nshm_base = NULL;
	_nshm_base_t *base;
	int index;

	base = (_nshm_base_t*)nshm->_base;
	index = get_bigseg_index(hdr->seg_siz);
	SET_NSHMBASE(nshm);
	_lock_nshm(nshm);
	hdr->next_offset = base->big_msegs[index];
	base->big_msegs[index] = vos_assign(int64_t,hdr);
	_unlock_nshm(nshm);
}

static void* _shmalloc(NShm *nshm,int32_t size)
{
	void* _nshm_base = NULL;
	char *free_vaddr;
	int32_t alloc_siz;
	_nshm_base_t *base;

	alloc_siz = (size + _ALIGNMENT_BYTE - 1) & -_ALIGNMENT_BYTE;
	base = (_nshm_base_t*)nshm->_base;
	if( (size_t)(base->free_offset + alloc_siz) > nshm->_size ){
		return(NULL);
	}

	SET_NSHMBASE(nshm);
	free_vaddr = vos_ptr(char*,base->free_offset);
	base->free_offset += alloc_siz;
	return(free_vaddr);
}

static void _init_meta(NShm *nshm)
{
	nshm->_fd = -1;
	nshm->_size = 0;
	nshm->_base = MAP_FAILED;
	sigfillset(&nshm->_full_set);
	sigemptyset(&nshm->_backup_set);
}

static void _lock_nshm(NShm *nshm)
{
	_nshm_base_t *base = (_nshm_base_t*)nshm->_base;
	pthread_sigmask(SIG_BLOCK,&nshm->_full_set,&nshm->_backup_set);
	pthread_spin_lock(&base->lock_point);
}

static void _unlock_nshm(NShm *nshm)
{
	_nshm_base_t *base = (_nshm_base_t*)nshm->_base;
	pthread_spin_unlock(&base->lock_point);
	pthread_sigmask(SIG_SETMASK,&nshm->_backup_set,NULL);
}

static int _get_key_hash(const char *key,int klen)
{
	int rc = 0;
	int ix;

	for( ix = 0; ix < klen; ix++ ){
		rc += key[ix];
	}

	return(rc % _DATA_HASH_SIZ);
}

