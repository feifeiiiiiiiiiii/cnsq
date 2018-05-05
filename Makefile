all:
	cd src/util; ${MAKE}
	cd src/diskqueue; ${MAKE}
	cd src/net; ${MAKE}
	cd src/nsqd; ${MAKE}

clean:
	cd src/util; ${MAKE} clean
	cd src/net; ${MAKE} clean
	cd src/nsqd; ${MAKE} clean
	cd src/diskqueue; ${MAKE} clean
