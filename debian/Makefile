dummy:

install:
	mkdir -p ${DESTDIR}/etc/ld.so.conf.d
	install -m644 nshm.conf ${DESTDIR}/etc/ld.so.conf.d/nshm.conf
	mkdir -p ${DESTDIR}/home/y/lib
	install -m755 libnshm.so.1 ${DESTDIR}/home/y/lib/libnshm.so.1
	ln -s libnshm.so.1 ${DESTDIR}/home/y/lib/libnshm.so
	mkdir -p ${DESTDIR}/home/y/include/nshm
	install -m644 nshm.h ${DESTDIR}/home/y/include/nshm/nshm.h
